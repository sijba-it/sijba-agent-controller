"""AI API registry: named chat-completions endpoints (URL + key) that
catalog plugins can reference by name instead of each carrying its own copy
of a secret.

Mirrors catalog.py's shape (locked read-modify-write JSON file), but unlike
catalog.json this holds secrets — local machine state, gitignored, same
reasoning as registry.json. A catalog plugin declares `"api": "<name>"`;
controller.py resolves that at spawn time and injects AI_API_URL/
AI_API_KEY/AI_API_NAME into the plugin's environment (see
controller.py's _spawn_command), so the plugin itself never touches this
file directly — same arrangement as SIJBA_URL/SIJBA_TOKEN.
"""
from __future__ import annotations

import fcntl
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

APIS_FILE = Path(os.environ.get("APIS_FILE", Path(__file__).parent / "apis.json"))


def _default_data() -> dict:
    # Seeded so `apis list` shows something useful out of the box — key
    # starts empty; set it with `apis set-key openrouter <key>` before any
    # plugin that declares `"api": "openrouter"` will actually work.
    return {
        "openrouter": {
            "url": "https://openrouter.ai/api/v1",
            "key": "",
            "description": "OpenRouter — model-agnostic chat completions API",
            "added_at": time.time(),
        }
    }


@contextmanager
def _locked() -> Iterator[dict]:
    APIS_FILE.parent.mkdir(parents=True, exist_ok=True)
    is_new = not APIS_FILE.exists()
    APIS_FILE.touch(exist_ok=True)
    with open(APIS_FILE, "r+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            raw = f.read().strip()
            data = json.loads(raw) if raw else (_default_data() if is_new else {})
            yield data
            tmp = APIS_FILE.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
            os.replace(tmp, APIS_FILE)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def load() -> dict:
    with _locked() as data:
        return data


def get(name: str) -> Optional[dict]:
    return load().get(name)


def list_apis() -> dict:
    return load()


def add(name: str, url: str, key: str = "", description: str = "") -> dict:
    with _locked() as data:
        if name in data:
            raise ValueError(f"api {name!r} already registered")
        entry = {"url": url, "key": key, "description": description, "added_at": time.time()}
        data[name] = entry
        return dict(entry)


def set_key(name: str, key: str) -> dict:
    with _locked() as data:
        if name not in data:
            raise KeyError(name)
        data[name]["key"] = key
        return dict(data[name])


def remove(name: str) -> bool:
    with _locked() as data:
        return data.pop(name, None) is not None
