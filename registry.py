"""Project registry: the single source of truth for which projects the
controller knows about, where they live, which catalog plugin runs them,
and whether they should currently be running.

A JSON file, not a database — matches the "config is JSON/YAML, readable
without a framework diagram" spirit of the rest of sijba. Both controller.py
(add/mkdir/archive/restore/remove/list/status) and the running plugin
processes (which flip their own entry to "archived"/"paused" right before
they exit) read and write this file directly, so all writes go through
`_locked()` to avoid two processes clobbering each other on a
read-modify-write.
"""
from __future__ import annotations

import fcntl
import json
import os
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional

import catalog

REGISTRY_FILE = Path(os.environ.get("REGISTRY_FILE", Path(__file__).parent / "registry.json"))

# Comma-separated list of workspace root directories `paths` searches (two
# levels deep — see search_paths) and `mkdir` creates under, e.g.
# "/home/rbartl/workspaces,/mnt/otherdrive/projects". Default is the single
# top-level ~/workspaces — the real layout is ~/workspaces/<org>/<project>/,
# with many orgs underneath (sijba, edvbartl, hocoas, ...), so searching two
# levels down from one root covers all of them without listing each org.
DEFAULT_ROOTS = [
    p.strip() for p in os.environ.get(
        "WORKSPACE_ROOTS", str(Path.home() / "workspaces")
    ).split(",") if p.strip()
]

STATUSES = {"active", "archived", "paused"}


def _default_data() -> dict:
    return {"projects": {}}


@contextmanager
def _locked() -> Iterator[dict]:
    """Read-modify-write the registry file under an exclusive flock.

    Yields the parsed dict; whatever the caller mutates in place is written
    back atomically (temp file + os.replace) on exit.
    """
    REGISTRY_FILE.parent.mkdir(parents=True, exist_ok=True)
    REGISTRY_FILE.touch(exist_ok=True)
    with open(REGISTRY_FILE, "r+") as f:
        fcntl.flock(f, fcntl.LOCK_EX)
        try:
            raw = f.read().strip()
            data = json.loads(raw) if raw else _default_data()
            yield data
            tmp = REGISTRY_FILE.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(data, indent=2, sort_keys=True))
            os.replace(tmp, REGISTRY_FILE)
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def load() -> dict:
    """Read-only snapshot (still flock'd — the file is small and this is
    infrequent, no need for a separate unlocked fast path)."""
    with _locked() as data:
        return data


def list_projects(status: Optional[str] = None) -> dict:
    data = load()
    projects = data["projects"]
    if status is None:
        return projects
    return {n: p for n, p in projects.items() if p.get("status") == status}


def get(name: str) -> Optional[dict]:
    return load()["projects"].get(name)


def add(name: str, path: str, plugin: str) -> dict:
    if catalog.get(plugin) is None:
        raise ValueError(f"unknown plugin {plugin!r} — see 'catalog list'")
    path = str(Path(path).expanduser())
    if not Path(path).is_absolute():
        # A relative path stored here is a footgun that only detonates
        # later, and differently in each consumer: cmd_add_project's own
        # is_dir() check resolves it against *this process's* cwd,
        # kind:local's `tmux -c` resolves it against the tmux *server*'s
        # cwd (frozen at that server's first-ever pane, e.g. $HOME) — so a
        # relative path can look fine at add time and still crash-loop
        # forever at spawn time in a directory nobody intended (found
        # live: 'edv-bartl/einhell-charger' silently landing inside this
        # repo's own tree, then spawning `claude` in $HOME and hitting its
        # untrusted-home-directory guard). Reject it here instead, once,
        # loudly — every caller should already be passing what
        # mkdir()/search_paths() returned, both always absolute.
        raise ValueError(
            f"path {path!r} is not absolute — use the path 'mkdir' or 'paths' "
            f"returned, not a bare name or org/name fragment"
        )
    with _locked() as data:
        existing = data["projects"].get(name)
        if existing is not None:
            raise ValueError(
                f"project {name!r} already registered (status: {existing['status']}) — "
                f"remove it first, or restore it if you want the original plugin/path back"
            )
        entry = {
            "path": path,
            "plugin": plugin,
            "status": "active",
            "created_at": time.time(),
        }
        data["projects"][name] = entry
        return dict(entry)


def set_can_inject(name: str, allowed: bool) -> None:
    """Local record only — the authoritative flag lives in sijba Core's
    own channel_permissions.yaml (core/channel_permissions.py), checked
    fresh on every inject. This copy is just so `list`/`status` can show
    it without a round-trip to Core, and so it survives being re-derived
    if Core's own file is ever rebuilt from scratch. The caller (cmd_
    allow_inject in controller.py) is responsible for actually pushing
    this to Core via its admin API — set_can_inject() alone does not."""
    with _locked() as data:
        if name not in data["projects"]:
            raise KeyError(name)
        data["projects"][name]["can_inject"] = bool(allowed)


def set_status(name: str, status: str) -> None:
    if status not in STATUSES:
        raise ValueError(f"unknown status {status!r}")
    with _locked() as data:
        if name not in data["projects"]:
            raise KeyError(name)
        data["projects"][name]["status"] = status
        data["projects"][name]["status_changed_at"] = time.time()


def remove(name: str) -> bool:
    with _locked() as data:
        return data["projects"].pop(name, None) is not None


def roots() -> list[str]:
    """Search/mkdir roots — always live from WORKSPACE_ROOTS, never stored
    state. Previously this was persisted into registry.json at first
    creation and never touched again, since nothing ever actually mutates
    it — which meant changing WORKSPACE_ROOTS had *no effect* on an
    existing registry.json: found live, search kept using a stale root
    from the very first time the file was created, no matter what the env
    var said on every restart since."""
    return DEFAULT_ROOTS


def search_paths(filter_str: str = "") -> list[str]:
    """Case-insensitive substring search, two levels deep under each
    configured root — matches the ~/workspaces/<org>/<project> layout (org
    at depth 1, project at depth 2), so one root covers every org without
    listing each one. Matches against the "<org>/<project>" relative path,
    so searching by org name or project name both work."""
    needle = filter_str.lower()
    hits: list[str] = []
    for root in roots():
        root_path = Path(root)
        if not root_path.is_dir():
            continue
        try:
            proc = subprocess.run(
                ["find", str(root_path), "-mindepth", "2", "-maxdepth", "2", "-type", "d"],
                capture_output=True, text=True, timeout=10, check=True,
            )
        except (subprocess.SubprocessError, OSError):
            continue
        for line in proc.stdout.splitlines():
            path = Path(line)
            rel = path.relative_to(root_path)
            if any(part.startswith(".") for part in rel.parts):
                continue
            if needle in str(rel).lower():
                hits.append(str(path))
    return sorted(hits)


def mkdir(name: str, root: Optional[str] = None) -> str:
    """Create <workspace-root>/<root>/<name>. `root`, when given, is an
    org-level subdirectory *under* the first configured workspace root
    (e.g. 'edvbartl') — never a standalone path. Previously `root` was used
    as-is (`Path(root)`), so a relative or misspelled org name (e.g.
    'edv-bartl' instead of the existing 'edvbartl') silently created the
    project nested inside whatever the caller's cwd happened to be — found
    live, it landed inside this controller's own repo tree, outside every
    configured workspace root, where nothing (not `paths`, not a human
    browsing ~/workspaces) would ever find it again. Anchoring to
    roots()[0] makes the result always absolute and always under a
    searchable root, regardless of what's passed in. Omit --root to fall
    back to the workspace root's top level. Errors if it already exists —
    this only ever creates brand-new project dirs."""
    base = Path(roots()[0])
    target = (base / root / name) if root else (base / name)
    if target.exists():
        raise FileExistsError(f"{target} already exists")
    target.mkdir(parents=True)
    return str(target)
