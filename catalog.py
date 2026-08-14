"""Plugin catalog: which backends the controller knows how to run, and
where they come from — Claude and Cursor are just the two built-in ones.

Unlike registry.json (which instances are actually running, local machine
state, gitignored), this is config: shareable across machines, meant to be
committed. `controller.py` lets you edit it from chat the same way it edits
the registry (locked read-modify-write), but the file itself is meant to be
readable/reviewable on its own.
"""
from __future__ import annotations

import fcntl
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

CATALOG_FILE = Path(os.environ.get("CATALOG_FILE", Path(__file__).parent / "catalog.json"))

# Where git-sourced plugins get cloned to. Fetched content, not authored
# here — gitignored, same reasoning as registry.json.
PLUGINS_DIR = Path(os.environ.get("PLUGINS_DIR", Path(__file__).parent / "_plugins"))

KINDS = {"git", "docker", "local"}

_GITEA = "ssh://git@REDACTED-GIT-HOST:7999/sijba"


def _default_data() -> dict:
    return {
        "claude": {
            "kind": "git", "url": f"{_GITEA}/sijba-claude-plugin.git", "ref": None,
            "description": "Claude Code CLI, persistent session",
        },
        "cursor": {
            "kind": "git", "url": f"{_GITEA}/sijba-cursor-plugin.git", "ref": None,
            "description": "Cursor Agent CLI, one-shot turns via --resume",
        },
        "remote-control": {
            "kind": "local", "command": ["claude", "--remote-control", "{name}"],
            "description": (
                "Interactive Claude Code session paired to the Claude mobile app via "
                "Remote Control. No sijba chat relay — talk to it from your phone, not "
                "from a sijba channel; the controller only keeps the session alive."
            ),
        },
    }


@contextmanager
def _locked() -> Iterator[dict]:
    CATALOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    is_new = not CATALOG_FILE.exists()
    CATALOG_FILE.touch(exist_ok=True)
    with open(CATALOG_FILE, "r+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            raw = f.read().strip()
            data = json.loads(raw) if raw else (_default_data() if is_new else {})
            yield data
            tmp = CATALOG_FILE.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
            os.replace(tmp, CATALOG_FILE)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def load() -> dict:
    with _locked() as data:
        return data


def get(name: str) -> Optional[dict]:
    return load().get(name)


def list_plugins() -> dict:
    return load()


def _add(name: str, entry: dict) -> dict:
    with _locked() as data:
        if name in data:
            raise ValueError(f"plugin {name!r} already in catalog")
        entry = {**entry, "added_at": time.time()}
        data[name] = entry
        return dict(entry)


def add_git(name: str, url: str, ref: Optional[str] = None, description: str = "") -> dict:
    return _add(name, {"kind": "git", "url": url, "ref": ref, "description": description})


def add_docker(name: str, image: str, description: str = "") -> dict:
    return _add(name, {"kind": "docker", "image": image, "description": description})


def add_local(name: str, command: list[str], description: str = "") -> dict:
    """A plugin that's just a local command — no fetch step. `command` may
    use `{name}`/`{work_dir}` placeholders, filled in at spawn time."""
    return _add(name, {"kind": "local", "command": command, "description": description})


def remove(name: str) -> bool:
    with _locked() as data:
        return data.pop(name, None) is not None
