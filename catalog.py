"""Plugin catalog: which backends the controller knows how to run, and
where they come from — cursor and remote-control are the built-in ones.

Unlike registry.json (which instances are actually running, local machine
state, gitignored), this is config: shareable across machines, meant to be
committed. `controller.py` lets you edit it from chat the same way it edits
the registry (locked read-modify-write), but the file itself is meant to be
readable/reviewable on its own.

A plugin entry may carry an optional `api` field naming an entry in
apis.py's registry (e.g. "openrouter") — the actual URL/key live there, not
here, since apis.json holds secrets and this file doesn't. `controller.py`
resolves `api` at spawn time and injects it into the plugin's environment.
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
        "cursor": {
            "kind": "git", "url": f"{_GITEA}/sijba-cursor-plugin.git", "ref": None,
            "description": "Cursor Agent CLI, one-shot turns via --resume",
        },
        "remote-control": {
            "kind": "local",
            # --spawn=same-dir: a brand-new/freshly-cloned project directory
            # makes the `claude remote-control` CLI prompt interactively
            # ("Spawn mode for this project: [1] same-dir [2] worktree ...")
            # the first time it's run there — found live (z-memex, a fresh
            # git clone), nothing answers that prompt inside the tmux/script
            # wrapper, so the session just sits there forever looking
            # "started" with no actual chat relay. Pin the default so it
            # never asks.
            "command": [
                "claude", "remote-control", "--name", "{name}",
                "--permission-mode", "auto", "--capacity", "2", "--spawn=same-dir",
            ],
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


def add_git(
    name: str, url: str, ref: Optional[str] = None, description: str = "",
    api: Optional[str] = None, env: Optional[dict] = None,
) -> dict:
    return _add(name, {
        "kind": "git", "url": url, "ref": ref, "description": description, "api": api, "env": env or {},
    })


def add_docker(
    name: str, image: str, description: str = "", api: Optional[str] = None, env: Optional[dict] = None,
) -> dict:
    return _add(name, {
        "kind": "docker", "image": image, "description": description, "api": api, "env": env or {},
    })


def add_local(
    name: str, command: list[str], description: str = "", api: Optional[str] = None,
    env: Optional[dict] = None,
) -> dict:
    """A plugin that's just a local command — no fetch step. `command` may
    use `{name}`/`{work_dir}` placeholders, filled in at spawn time."""
    return _add(name, {
        "kind": "local", "command": command, "description": description, "api": api, "env": env or {},
    })


def remove(name: str) -> bool:
    with _locked() as data:
        return data.pop(name, None) is not None
