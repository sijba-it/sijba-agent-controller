#!/usr/bin/env python3
"""Agent controller: thin process supervisor for sijba project sessions.

Owns the registry (registry.py), the plugin catalog (catalog.py), and one
dedicated "controller" control channel on sijba Core. Its only real job is
*starting* things: for every `active` registry entry it keeps its plugin's
process running, always restarting it with crash-loop backoff on
unexpected exit — a controller reboot and a mid-run crash are handled by
the exact same ensure_running() loop, no special-casing.

Every plugin (including claude and cursor — see catalog.py) is a `kind:
git` or `kind: docker` catalog entry, fetched once at `add` time and run as
its own process. Those processes are self-registering (their own
WebSocket connection, own channel) and self-managing for /archive and
/pause — they write their own registry status and exit with a reserved
code (exit_codes.py) before the controller ever notices. Global commands
(`controller archive <name>`, ...) go through the same registry +
backoff-loop state via `_intentional_stop`, so a controller-initiated stop
is never mistaken for a crash.

Environment variables
----------------------
SIJBA_URL, SIJBA_TOKEN                same as any sijba plugin
REGISTRY_FILE, WORKSPACE_ROOTS         see registry.py
CATALOG_FILE, PLUGINS_DIR              see catalog.py
LLM_API_URL, LLM_API_KEY, LLM_MODEL, LLM_FORMAT   optional — same shape as
    plugins-builtin/router/main.py. If LLM_API_KEY is unset, free text that
    doesn't match an exact command just gets the command list back.
"""
from __future__ import annotations

import asyncio
import collections
import json
import os
import re
import shlex
import signal
import socket
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import websockets

import catalog
import registry
from exit_codes import EXIT_ARCHIVED, EXIT_PAUSED

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SIJBA_URL = os.environ.get("SIJBA_URL", "wss://sijba.edv-bartl.at")
SIJBA_TOKEN = os.environ.get("SIJBA_TOKEN", "")
# Running one controller per host means each needs its own
# plugin_id/channel_id/trigger, or the sijba server can't tell them apart
# (last one to register a given channel_id wins) — so the default already
# bakes in the short hostname rather than a bare "controller" that only
# works for a single-host setup. Override CONTROLLER_NAME directly for a
# friendlier trigger word (e.g. "controller-prod") if the hostname is ugly.
CONTROLLER_NAME = os.environ.get("CONTROLLER_NAME") or f"controller-{socket.gethostname().split('.')[0]}"
PLUGIN_ID = os.environ.get("PLUGIN_ID", CONTROLLER_NAME)
CHANNEL_ID = CONTROLLER_NAME
PYTHON = sys.executable


def _load_claude_plugin_api_key() -> str:
    """Loaded from a dedicated file, deliberately never the controller's own
    os.environ — _spawn_env below does env = dict(os.environ) for every
    plugin kind, so anything sitting in the controller's own environment
    reaches every spawned child, remote-control included. That plugin is a
    real interactive session tied to the Pro/Max subscription's OAuth login
    (`claude --remote-control`, driven from the phone app — see catalog.json);
    an ANTHROPIC_API_KEY leaking into it would silently switch it to
    per-token API billing instead. Reading from a file _spawn_env injects
    only for the 'claude' plugin keeps this key out of os.environ entirely,
    so there's nothing for the blanket inherit to pick up."""
    path = Path(os.environ.get(
        "CLAUDE_PLUGIN_API_KEY_FILE", str(Path.home() / ".config" / "sijba-claude-plugin-api-key"),
    ))
    try:
        return path.read_text().strip()
    except FileNotFoundError:
        return ""


CLAUDE_PLUGIN_API_KEY = _load_claude_plugin_api_key()

BACKOFF_BASE = 5.0
BACKOFF_MAX = 300.0
BACKOFF_RESET_AFTER = 60.0  # stayed up this long -> next failure starts back at BACKOFF_BASE

LLM_API_URL = os.environ.get("LLM_API_URL", "https://openrouter.ai/api/v1").rstrip("/")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
LLM_MODEL = os.environ.get("LLM_MODEL", "openai/gpt-4o-mini")
LLM_FORMAT = os.environ.get("LLM_FORMAT", "openai").lower()

if not SIJBA_TOKEN:
    print("ERROR: SIJBA_TOKEN environment variable required", file=sys.stderr)
    sys.exit(1)

_LOG_BUFFER: collections.deque[str] = collections.deque(maxlen=200)


def _log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    line = f"{ts} [controller] {msg}"
    _LOG_BUFFER.append(line)
    print(line, flush=True)


def _age(ts: float) -> str:
    """Human-readable age of a unix timestamp, e.g. "3h ago" — used to show
    when a project's status last changed, so a stale archived entry (which
    silently blocks re-adding under the same name) is visible instead of
    just sitting there invisibly."""
    delta = time.time() - ts
    if delta < 60:
        return "just now"
    if delta < 3600:
        return f"{int(delta // 60)}m ago"
    if delta < 86400:
        return f"{int(delta // 3600)}h ago"
    return f"{int(delta // 86400)}d ago"


# ---------------------------------------------------------------------------
# Process supervision
# ---------------------------------------------------------------------------

_procs: dict[str, asyncio.subprocess.Process] = {}
_supervisors: dict[str, asyncio.Task] = {}
_intentional_stop: dict[str, str] = {}  # name -> "archived" | "paused" | "removed", set right before killing

# kind:local wraps a full-screen terminal UI (tmux) whose raw ANSI screen
# redraws are noise in the shared journal (see `quiet` in _spawn) — but
# DEVNULL swallowed that unconditionally, including the one plain-text line
# that would've explained a crash. Captured to a file instead, per project,
# overwritten on every (re)spawn — ensure_running() tails it into the crash
# log line when a spawn dies fast enough that a screen was never drawn.
_LOCAL_LOG_DIR = Path(__file__).parent / "_logs"
_ANSI_RE = re.compile(rb"\x1b(?:\[[0-9;?]*[a-zA-Z]|\][^\x07]*\x07|[()][A-Za-z0-9]|[=>])")


def _local_log_path(name: str) -> Path:
    return _LOCAL_LOG_DIR / f"{name}.log"


def _tail_local_log(name: str, max_bytes: int = 4000) -> str:
    """Best-effort, ANSI-stripped tail of a kind:local child's captured pty
    output, for surfacing in a crash log line. A fast crash rarely gets far
    enough to draw a screen, so what's left is usually just the plain-text
    error that actually explains why."""
    try:
        data = _local_log_path(name).read_bytes()[-max_bytes:]
    except FileNotFoundError:
        return ""
    text = _ANSI_RE.sub(b"", data).decode("utf-8", "replace")
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    return " | ".join(lines[-8:])


def _spawn_env(name: str, entry: dict) -> dict:
    """Env vars common to every plugin kind."""
    env = dict(os.environ)
    env.update({
        "SIJBA_URL": SIJBA_URL,
        "SIJBA_TOKEN": SIJBA_TOKEN,
        "PROJECT_NAME": name,
        "WORK_DIR": entry["path"],
        "REGISTRY_FILE": str(registry.REGISTRY_FILE),
    })
    # kind:local wraps its command in tmux inside a `script`-provided pty
    # (see _spawn_command) — tmux's client needs $TERM to look up a
    # terminfo entry to even initialize, and a controller with no
    # controlling terminal of its own (e.g. run under systemd, unlike an
    # ad-hoc shell launch that happens to inherit one) has no TERM at all.
    # Without this, tmux fails silently (stdout/stderr are DEVNULL'd for
    # this kind) with "open terminal failed: terminal does not support
    # clear" and the whole thing crash-loops forever on exit=0.
    env.setdefault("TERM", "xterm-256color")
    return env


async def ensure_fetched(plugin_name: str, plugin: dict) -> None:
    """One-time fetch for a plugin's code. Runs at `add` time, not on
    every (re)spawn — a crash-loop shouldn't hammer git/docker on every
    retry, and it keeps fetch failures a distinct, immediately-reported
    error instead of getting lost in the backoff loop's log spam."""
    if plugin["kind"] == "git":
        dest = catalog.PLUGINS_DIR / plugin_name
        if dest.exists():
            cmd = ["git", "-C", str(dest), "pull"]
        else:
            dest.parent.mkdir(parents=True, exist_ok=True)
            cmd = ["git", "clone"]
            if plugin.get("ref"):
                cmd += ["--branch", plugin["ref"]]
            cmd += [plugin["url"], str(dest)]
        proc = await asyncio.create_subprocess_exec(*cmd)
        code = await proc.wait()
        if code != 0:
            raise RuntimeError(f"git fetch failed for {plugin_name!r} (exit {code})")
        if not (dest / "main.py").exists():
            raise RuntimeError(f"{plugin_name!r}: expected {dest / 'main.py'} (git plugin convention)")
    elif plugin["kind"] == "docker":
        proc = await asyncio.create_subprocess_exec("docker", "pull", plugin["image"])
        code = await proc.wait()
        if code != 0:
            raise RuntimeError(f"docker pull failed for {plugin_name!r} (exit {code})")


def _spawn_command(name: str, entry: dict, plugin: dict) -> tuple[list[str], dict, str]:
    """Build (argv, env, cwd) for this project, dispatched on its plugin's kind."""
    env = _spawn_env(name, entry)
    # Catalog-level, per-plugin overrides (e.g. cursor's CURSOR_CMD pointing
    # at an absolute path) — kept out of the shared EnvironmentFile, which
    # every plugin kind inherits via _spawn_env's dict(os.environ), so a
    # knob one plugin needs doesn't leak into all the others.
    env.update(plugin.get("env", {}))
    controller_dir = str(Path(__file__).parent)
    if plugin["kind"] == "git":
        entry_point = catalog.PLUGINS_DIR / entry["plugin"] / "main.py"
        return [PYTHON, str(entry_point)], env, controller_dir
    if plugin["kind"] == "docker":
        cmd = [
            "docker", "run", "--rm", "--name", f"sijba-{name}",
            "-v", f"{entry['path']}:/work",
            "-v", f"{registry.REGISTRY_FILE}:/registry.json",
            "-e", f"SIJBA_URL={SIJBA_URL}", "-e", f"SIJBA_TOKEN={SIJBA_TOKEN}",
            "-e", f"PROJECT_NAME={name}", "-e", "WORK_DIR=/work",
            "-e", "REGISTRY_FILE=/registry.json",
        ]
        for key, val in plugin.get("env", {}).items():
            cmd += ["-e", f"{key}={val}"]
        cmd.append(plugin["image"])
        # Env vars for a container go via -e above, not the docker CLI's own env.
        return cmd, dict(os.environ), controller_dir
    if plugin["kind"] == "local":
        # No fetch step — just a locally-installed command, run in the
        # project's own directory, inside a named tmux session rather than
        # a bare pty: some commands need a real terminal (e.g. `claude
        # --remote-control` explicitly wants an *interactive* session), and
        # wrapping in tmux means a human can also `tmux attach -t
        # sijba-<name>` later, not just watch via whatever the command
        # itself pairs with. `-A` creates-or-attaches, so a restart after a
        # crash reattaches to a stray session instead of erroring. tmux
        # still needs *something* that looks like a terminal to attach
        # from and we have none (headless service) — `script -qc` fakes
        # one. Either way script's own exit stays tied to the session's
        # actual lifetime, so crash-loop backoff still sees it right.
        argv = [part.format(name=name, work_dir=entry["path"]) for part in plugin["command"]]
        session_name = f"sijba-{name}"
        inner = " ".join(shlex.quote(a) for a in argv)
        tmux_cmd = (
            f"tmux new-session -A -s {shlex.quote(session_name)} "
            f"-c {shlex.quote(entry['path'])} {inner}"
        )
        return ["script", "-qc", tmux_cmd, "/dev/null"], env, entry["path"]
    raise RuntimeError(f"unknown plugin kind {plugin['kind']!r}")


async def _spawn(name: str, entry: dict) -> asyncio.subprocess.Process:
    plugin = catalog.get(entry["plugin"])
    if plugin is None:
        raise RuntimeError(f"catalog has no plugin {entry['plugin']!r}")
    argv, env, cwd = _spawn_command(name, entry, plugin)
    # kind:local wraps a full-screen terminal UI (tmux) — its raw ANSI
    # screen redraws are noise in the shared journal, not useful for
    # troubleshooting. The actual interaction happens via `tmux attach` or
    # whatever the command itself pairs with, not here. Captured to a file
    # (see _LOCAL_LOG_DIR) instead of DEVNULL so a crash still has something
    # to show — ensure_running() tails it on fast exits.
    quiet = plugin["kind"] == "local"
    log_file = None
    if quiet:
        _LOCAL_LOG_DIR.mkdir(parents=True, exist_ok=True)
        log_file = open(_local_log_path(name), "wb")
    try:
        proc = await asyncio.create_subprocess_exec(
            *argv,
            cwd=cwd,
            env=env,
            # git/docker plugins inherit stdout/stderr (None) — one combined
            # journal stream, each line already prefixed with the project
            # name by the plugin's own _log(). kind:local gets the capture
            # file instead, stderr merged into the same stream.
            stdout=log_file if quiet else None,
            stderr=asyncio.subprocess.STDOUT if quiet else None,
        )
    finally:
        if log_file is not None:
            log_file.close()
    _procs[name] = proc
    _log(f"{name}: started (pid={proc.pid}, plugin={entry['plugin']})")
    return proc


async def ensure_running(name: str) -> None:
    """Supervises one project: (re)spawn while registry status is 'active',
    with crash-loop backoff on unexpected exit. Same function for first
    start, controller-reboot start, and post-crash restart."""
    backoff = BACKOFF_BASE
    while True:
        entry = registry.get(name)
        if entry is None or entry.get("status") != "active":
            _supervisors.pop(name, None)
            return

        started_at = time.time()
        try:
            proc = await _spawn(name, entry)
            code = await proc.wait()
        except Exception as exc:
            # Anything unexpected here — not just ENOENT — must still fall
            # through to the crash-bucket backoff-and-retry below. This was
            # previously scoped to just FileNotFoundError, so any other
            # exception (a bad env var, a transient OSError, ...) fell
            # through the bottom of the try without setting `code`, killing
            # this supervisor asyncio.Task outright with no log line and no
            # respawn — found live: a project's supervisor task vanished
            # silently mid-session with nothing in the log to show why.
            _log(f"{name}: failed to spawn/run: {exc!r}")
            code = None
        finally:
            _procs.pop(name, None)

        uptime = time.time() - started_at

        stop_reason = _intentional_stop.pop(name, None)
        if stop_reason is not None:
            _log(f"{name}: stopped ({stop_reason}, exit={code})")
            _supervisors.pop(name, None)
            return
        if code == EXIT_ARCHIVED:
            _log(f"{name}: self-archived (exit={code})")
            _supervisors.pop(name, None)
            return
        if code == EXIT_PAUSED:
            _log(f"{name}: self-paused (exit={code})")
            _supervisors.pop(name, None)
            return

        # Unplanned exit -> crash-loop backoff. Re-check status each spin so
        # a concurrent archive/remove during the sleep still takes effect.
        backoff = BACKOFF_BASE if uptime >= BACKOFF_RESET_AFTER else min(backoff * 2, BACKOFF_MAX)
        detail = ""
        plugin = catalog.get(entry["plugin"])
        if plugin is not None and plugin["kind"] == "local":
            tail = _tail_local_log(name)
            if tail:
                detail = f" — {tail}"
        _log(f"{name}: crashed (exit={code}, uptime={uptime:.0f}s){detail} — restart in {backoff:.0f}s")
        await asyncio.sleep(backoff)


async def terminate_all_children() -> None:
    """Stop every process this controller currently supervises, called on
    clean shutdown. Deliberately does NOT touch tmux sessions kind:local
    processes run inside (only the `script`/`tmux attach` wrapper client
    dies) — those persist on the tmux server and the next controller's `-A`
    reattaches. Without this, a restarted controller has no record of the
    previous instance's children, so they're never terminated: found live,
    two `cursor`-kind subprocesses (each holding its own websocket
    connection, so each still handling messages) were left running as
    orphans for a full day after a routine restart, double-replying on
    their channels the whole time."""
    procs = [p for p in _procs.values() if p.returncode is None]
    for proc in procs:
        proc.terminate()
    for proc in procs:
        try:
            await asyncio.wait_for(proc.wait(), timeout=10)
        except asyncio.TimeoutError:
            proc.kill()


def start_supervising(name: str) -> None:
    if name in _supervisors:
        return
    _supervisors[name] = asyncio.create_task(ensure_running(name))


async def stop_process(name: str, reason: str) -> None:
    """Controller-initiated stop (as opposed to the session archiving/pausing
    itself) — mark intentional so the supervisor loop doesn't treat the exit
    as a crash, then terminate the child if one is running."""
    _intentional_stop[name] = reason
    proc = _procs.get(name)
    if proc and proc.returncode is None:
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=10)
        except asyncio.TimeoutError:
            proc.kill()
    # else: nothing in _procs (e.g. mid-backoff-sleep) — nothing to
    # terminate there, but the supervisor loop will see the flag on its
    # next spin.

    # kind:local wraps its command in a named tmux session (see
    # _spawn_command) so it survives detach and is attachable independently
    # — but that means killing the client above (the `script`-wrapped tmux
    # attach we actually hold a handle to) only detaches it. The session,
    # and whatever's still running inside it, persists on the tmux server
    # until explicitly killed. Harmless no-op for git/docker plugins, which
    # never created a session with this name.
    try:
        kill_proc = await asyncio.create_subprocess_exec(
            "tmux", "kill-session", "-t", f"sijba-{name}",
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL,
        )
        await kill_proc.wait()
    except FileNotFoundError:
        pass


def boot() -> None:
    for name, entry in registry.list_projects(status="active").items():
        start_supervising(name)


# ---------------------------------------------------------------------------
# Deterministic command handlers — the actual side effects. Both the exact
# "controller <cmd> ..." syntax and the optional LLM tool-calling layer
# below call into these same functions; the LLM never executes anything
# itself.
# ---------------------------------------------------------------------------

def cmd_list() -> str:
    projects = registry.list_projects()
    if not projects:
        return "No projects registered."
    lines = []
    for name, e in sorted(projects.items()):
        live = "running" if name in _procs else ("supervised" if name in _supervisors else "-")
        since = e.get("status_changed_at") or e.get("created_at")
        age = f", {_age(since)}" if since else ""
        lines.append(f"- {name}: {e['plugin']} @ {e['path']} [{e['status']}/{live}{age}]")
    return "\n".join(lines)


def cmd_paths(filter_str: str = "") -> str:
    hits = registry.search_paths(filter_str)
    if not hits:
        return f"No directories matching {filter_str!r} under {registry.roots()}"
    return "\n".join(hits)


def cmd_mkdir(name: str, root: Optional[str] = None) -> str:
    path = registry.mkdir(name, root)
    return f"Created {path}"


CLAUDE_CONFIG_FILE = Path.home() / ".claude.json"


def _trust_claude_dir(path: str) -> None:
    """Pre-accept Claude Code's interactive workspace-trust dialog for a
    project directory. Only matters for plugins that run an actual
    interactive `claude` session (kind:local, e.g. remote-control) — unlike
    -p/headless mode, that dialog isn't auto-skipped there, and our
    tmux-wrapped process has nothing attached to answer an interactive
    prompt with. Harmless no-op for projects that never invoke claude.
    Best-effort: touches ~/.claude.json, which the real claude CLI also
    writes to, so a lost race just means the dialog appears once — not
    worth adding locking for."""
    if not CLAUDE_CONFIG_FILE.exists():
        return
    try:
        data = json.loads(CLAUDE_CONFIG_FILE.read_text())
        entry = data.setdefault("projects", {}).setdefault(path, {})
        entry["hasTrustDialogAccepted"] = True
        tmp = CLAUDE_CONFIG_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, indent=2))
        os.replace(tmp, CLAUDE_CONFIG_FILE)
    except (json.JSONDecodeError, OSError) as exc:
        _log(f"_trust_claude_dir({path}): {exc}")


async def cmd_add(name: str, path: Optional[str] = None, plugin: str = "claude") -> str:
    plugin_entry = catalog.get(plugin)
    if plugin_entry is None:
        return f"Unknown plugin {plugin!r}. See 'catalog list'."
    if path is None:
        hits = registry.search_paths(name)
        if len(hits) == 1:
            path = hits[0]
        elif len(hits) > 1:
            return "Multiple matches, be specific with --path:\n" + "\n".join(hits)
        else:
            return f"No directory found for {name!r}. Use 'mkdir {name}' to create one first, or pass --path."
    elif not Path(path).expanduser().is_dir():
        # A bare name mistaken for an explicit path (e.g. the LLM tool-call
        # reading "in dem verzeichnis X" as "path=X" instead of "name=X" to
        # search for) would otherwise register a project whose cwd doesn't
        # exist — it can never spawn, so it crash-loops on FileNotFoundError
        # forever with nothing surfaced beyond the log. Found live: exactly
        # this, for 'aiaccountant-export'. Fall back to the same search
        # add(path=None) does, so a plain name still resolves; only error
        # out if that search also can't find it.
        hits = registry.search_paths(path)
        if len(hits) == 1:
            path = hits[0]
        elif len(hits) > 1:
            return f"{path!r} is not a directory. Multiple name matches, be specific with --path:\n" + "\n".join(hits)
        else:
            return (
                f"{path!r} is not a directory (checked as given, and searched by that "
                f"name under {registry.roots()}). Use 'mkdir {name}' to create one first, "
                f"or pass a valid --path."
            )
    try:
        await ensure_fetched(plugin, plugin_entry)
    except Exception as exc:
        return f"Fetch failed: {exc}"
    _trust_claude_dir(path)
    entry = registry.add(name, path, plugin)
    start_supervising(name)
    return f"Added {name}: {entry['plugin']} @ {entry['path']}, starting."


def cmd_status(name: str) -> str:
    entry = registry.get(name)
    if entry is None:
        return f"No such project: {name}"
    live = "running" if name in _procs else ("supervised (starting/backoff)" if name in _supervisors else "stopped")
    created = f" (created {_age(entry['created_at'])})" if entry.get("created_at") else ""
    changed = f" ({_age(entry['status_changed_at'])})" if entry.get("status_changed_at") else ""
    return (
        f"{name}\n  path: {entry['path']}\n  plugin: {entry['plugin']}\n"
        f"  status: {entry['status']}{changed}\n  live: {live}{created}"
    )


async def cmd_archive(name: str) -> str:
    if registry.get(name) is None:
        return f"No such project: {name}"
    registry.set_status(name, "archived")
    await stop_process(name, "archived")
    return f"{name} archived."


async def cmd_pause(name: str) -> str:
    if registry.get(name) is None:
        return f"No such project: {name}"
    registry.set_status(name, "paused")
    await stop_process(name, "paused")
    return f"{name} paused."


def cmd_restore(name: str) -> str:
    entry = registry.get(name)
    if entry is None:
        return f"No such project: {name}"
    registry.set_status(name, "active")
    start_supervising(name)
    return f"{name} restored, starting."


async def cmd_remove(name: str, wipe: bool = False) -> str:
    entry = registry.get(name)
    if entry is None:
        return f"No such project: {name}"
    await stop_process(name, "removed")
    registry.remove(name)
    msg = f"{name} removed from registry."
    if wipe:
        import shutil
        shutil.rmtree(entry["path"], ignore_errors=True)
        msg += f" Directory {entry['path']} wiped."
    return msg


def cmd_log(n: int = 10) -> str:
    lines = list(_LOG_BUFFER)[-n:]
    return "\n".join(lines) or "(no logs yet)"


def cmd_catalog_list() -> str:
    plugins = catalog.list_plugins()
    if not plugins:
        return "No plugins in catalog."
    lines = []
    for name, p in sorted(plugins.items()):
        if p["kind"] == "git":
            detail = p["url"] + (f"@{p['ref']}" if p.get("ref") else "")
        elif p["kind"] == "docker":
            detail = p["image"]
        elif p["kind"] == "local":
            detail = " ".join(p["command"])
        else:
            detail = "?"
        desc = f" — {p['description']}" if p.get("description") else ""
        lines.append(f"- {name} [{p['kind']}]: {detail}{desc}")
    return "\n".join(lines)


def cmd_catalog_add_git(name: str, url: str, ref: Optional[str] = None, description: str = "") -> str:
    entry = catalog.add_git(name, url, ref, description)
    return f"Catalog: {name} added ({entry['kind']}: {url})"


def cmd_catalog_add_docker(name: str, image: str, description: str = "") -> str:
    entry = catalog.add_docker(name, image, description)
    return f"Catalog: {name} added ({entry['kind']}: {image})"


def cmd_catalog_add_local(name: str, command: str, description: str = "") -> str:
    argv = shlex.split(command)
    entry = catalog.add_local(name, argv, description)
    return f"Catalog: {name} added ({entry['kind']}: {' '.join(argv)})"


def cmd_catalog_remove(name: str) -> str:
    if not catalog.remove(name):
        return f"No such plugin: {name}"
    return f"Catalog: {name} removed."


# ---------------------------------------------------------------------------
# "controller <cmd> ..." exact-syntax dispatcher
# ---------------------------------------------------------------------------

def _parse_args(rest: str) -> tuple[list[str], dict[str, str]]:
    tokens = shlex.split(rest)
    positional, flags = [], {}
    i = 0
    while i < len(tokens):
        t = tokens[i]
        if t.startswith("--"):
            key = t[2:]
            if i + 1 < len(tokens) and not tokens[i + 1].startswith("--"):
                flags[key] = tokens[i + 1]
                i += 2
            else:
                flags[key] = "true"
                i += 1
        else:
            positional.append(t)
            i += 1
    return positional, flags


HELP = (
    "Commands: list | add <name> [--path P] [--plugin claude|cursor|...] | "
    "mkdir <name> [--root R] | paths [filter] | status <name> | "
    "archive <name> | pause <name> | restore <name> | remove <name> [--wipe] | log [n] | "
    "catalog list | catalog add <name> --git URL [--ref R] [--desc D] | "
    "catalog add <name> --docker IMAGE [--desc D] | "
    "catalog add <name> --local \"cmd {name} {work_dir}\" [--desc D] | catalog remove <name>"
)


async def dispatch_command(text: str) -> Optional[str]:
    """Try to handle `text` as an exact 'controller <cmd> ...' command.
    Returns the reply, or None if it doesn't match any known command (so the
    caller can fall back to the LLM layer, or just say "unrecognized")."""
    cmd, _, rest = text.strip().partition(" ")
    cmd = cmd.lower()
    try:
        if cmd == "list":
            return cmd_list()
        if cmd == "paths":
            return cmd_paths(rest.strip())
        if cmd == "log":
            n = int(rest.strip()) if rest.strip().isdigit() else 10
            return cmd_log(n)
        if cmd == "mkdir":
            pos, flags = _parse_args(rest)
            if not pos:
                return "usage: mkdir <name> [--root R]"
            return cmd_mkdir(pos[0], flags.get("root"))
        if cmd == "add":
            pos, flags = _parse_args(rest)
            if not pos:
                return "usage: add <name> [--path P] [--plugin claude|cursor|...]"
            return await cmd_add(pos[0], flags.get("path"), flags.get("plugin", "claude"))
        if cmd == "catalog":
            sub, _, subrest = rest.strip().partition(" ")
            pos, flags = _parse_args(subrest)
            if sub == "list":
                return cmd_catalog_list()
            if sub == "add":
                if not pos:
                    return "usage: catalog add <name> --git URL [--ref R] | catalog add <name> --docker IMAGE"
                if "git" in flags:
                    return cmd_catalog_add_git(pos[0], flags["git"], flags.get("ref"), flags.get("desc", ""))
                if "docker" in flags:
                    return cmd_catalog_add_docker(pos[0], flags["docker"], flags.get("desc", ""))
                if "local" in flags:
                    return cmd_catalog_add_local(pos[0], flags["local"], flags.get("desc", ""))
                return "usage: catalog add <name> --git URL | --docker IMAGE | --local \"cmd\""
            if sub == "remove":
                if not pos:
                    return "usage: catalog remove <name>"
                return cmd_catalog_remove(pos[0])
            return "usage: catalog list | catalog add <name> --git URL|--docker IMAGE | catalog remove <name>"
        if cmd == "status":
            pos, _ = _parse_args(rest)
            if not pos:
                return "usage: status <name>"
            return cmd_status(pos[0])
        if cmd == "archive":
            pos, _ = _parse_args(rest)
            if not pos:
                return "usage: archive <name>"
            return await cmd_archive(pos[0])
        if cmd == "pause":
            pos, _ = _parse_args(rest)
            if not pos:
                return "usage: pause <name>"
            return await cmd_pause(pos[0])
        if cmd in ("restore", "resume"):
            pos, _ = _parse_args(rest)
            if not pos:
                return f"usage: {cmd} <name>"
            return cmd_restore(pos[0])
        if cmd == "remove":
            pos, flags = _parse_args(rest)
            if not pos:
                return "usage: remove <name> [--wipe]"
            return await cmd_remove(pos[0], flags.get("wipe") == "true")
        if cmd in ("help", ""):
            return HELP
    except Exception as exc:
        return f"Error: {exc}"
    return None


# ---------------------------------------------------------------------------
# Optional LLM tool-calling layer — only used for free text that doesn't
# match an exact command above. Same OpenRouter-style config as
# plugins-builtin/router/main.py. The LLM only ever selects/parameterizes
# one of the cmd_* functions above via tool_calls; it never has a side
# effect of its own.
# ---------------------------------------------------------------------------

TOOLS = [
    {"type": "function", "function": {
        "name": "list_projects", "description": "List all registered projects with status.",
        "parameters": {"type": "object", "properties": {}},
    }},
    {"type": "function", "function": {
        "name": "list_paths", "description": "Search directories under the configured workspace roots.",
        "parameters": {"type": "object", "properties": {
            "filter": {"type": "string", "description": "substring to match against directory names"}
        }},
    }},
    {"type": "function", "function": {
        "name": "mkdir", "description": "Create a brand-new project directory. Does not register it.",
        "parameters": {"type": "object", "properties": {
            "name": {"type": "string"}, "root": {"type": "string"}
        }, "required": ["name"]},
    }},
    {"type": "function", "function": {
        "name": "add_project",
        "description": "Register a project and start its session, using a plugin from the catalog "
                        "(see list_catalog). Omit path to auto-search by name.",
        "parameters": {"type": "object", "properties": {
            "name": {"type": "string"}, "path": {"type": "string"},
            "plugin": {"type": "string", "description": "catalog plugin name, e.g. 'claude', 'cursor', or a custom one"},
        }, "required": ["name"]},
    }},
    {"type": "function", "function": {
        "name": "status", "description": "Show details for one project.",
        "parameters": {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]},
    }},
    {"type": "function", "function": {
        "name": "archive_project", "description": "Stop a project's session, no auto-restart.",
        "parameters": {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]},
    }},
    {"type": "function", "function": {
        "name": "restore_project", "description": "Reactivate an archived/paused project.",
        "parameters": {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]},
    }},
    {"type": "function", "function": {
        "name": "remove_project", "description": "Unregister a project. wipe=true also deletes its directory.",
        "parameters": {"type": "object", "properties": {
            "name": {"type": "string"}, "wipe": {"type": "boolean"}
        }, "required": ["name"]},
    }},
    {"type": "function", "function": {
        "name": "list_catalog", "description": "List available plugins (claude, cursor, and any custom git/docker ones).",
        "parameters": {"type": "object", "properties": {}},
    }},
    {"type": "function", "function": {
        "name": "add_git_plugin",
        "description": "Register a new plugin sourced from a git repo. The repo must contain main.py "
                        "at its root implementing the plugin contract (see sijba-claude-plugin for an example).",
        "parameters": {"type": "object", "properties": {
            "name": {"type": "string"}, "url": {"type": "string"},
            "ref": {"type": "string"}, "description": {"type": "string"},
        }, "required": ["name", "url"]},
    }},
    {"type": "function", "function": {
        "name": "add_docker_plugin", "description": "Register a new plugin sourced from a docker image.",
        "parameters": {"type": "object", "properties": {
            "name": {"type": "string"}, "image": {"type": "string"}, "description": {"type": "string"},
        }, "required": ["name", "image"]},
    }},
    {"type": "function", "function": {
        "name": "add_local_plugin",
        "description": "Register a new plugin that's just a locally-installed command, no fetch step "
                        "(e.g. `claude --remote-control {name}`). Runs in the project's own directory. "
                        "{name} and {work_dir} placeholders are substituted at start time.",
        "parameters": {"type": "object", "properties": {
            "name": {"type": "string"}, "command": {"type": "string"}, "description": {"type": "string"},
        }, "required": ["name", "command"]},
    }},
    # No remove_plugin here on purpose — see cmd_catalog_remove. Deleting a
    # catalog entry is shared, high-blast-radius state (breaks every future
    # add_project for that plugin, for every project, not just one), unlike
    # remove_project which only ever touches one project's own registry
    # entry. Found live: asked to clean up a badly-named *project* called
    # "cursor", the model called remove_plugin("cursor") instead of
    # remove_project("cursor") — deleting the whole cursor catalog entry
    # and silently breaking every subsequent cursor-agent request until a
    # human noticed something was off and it got manually restored. Catalog
    # removal now requires the exact "controller catalog remove <name>"
    # command, typed by a human on purpose.
]

_TOOL_IMPL = {
    "list_projects": lambda **kw: cmd_list(),
    "list_paths": lambda **kw: cmd_paths(kw.get("filter", "")),
    "mkdir": lambda **kw: cmd_mkdir(kw["name"], kw.get("root")),
    "add_project": lambda **kw: cmd_add(kw["name"], kw.get("path"), kw.get("plugin", "claude")),
    "status": lambda **kw: cmd_status(kw["name"]),
    "archive_project": lambda **kw: cmd_archive(kw["name"]),
    "restore_project": lambda **kw: cmd_restore(kw["name"]),
    "remove_project": lambda **kw: cmd_remove(kw["name"], kw.get("wipe", False)),
    "list_catalog": lambda **kw: cmd_catalog_list(),
    "add_git_plugin": lambda **kw: cmd_catalog_add_git(kw["name"], kw["url"], kw.get("ref"), kw.get("description", "")),
    "add_docker_plugin": lambda **kw: cmd_catalog_add_docker(kw["name"], kw["image"], kw.get("description", "")),
    "add_local_plugin": lambda **kw: cmd_catalog_add_local(kw["name"], kw["command"], kw.get("description", "")),
}

SYSTEM_PROMPT = (
    "You are the sijba agent controller. You manage coding-agent sessions, one per "
    "project directory, each backed by a plugin from the catalog. ALWAYS call "
    "list_catalog first, before add_project — match the user's wording against the "
    "catalog entries' names/descriptions precisely. If they name a mode explicitly "
    "(e.g. 'remote control', 'pair with my phone') use the catalog plugin whose "
    "description matches that (e.g. 'remote-control'), not a generic one like "
    "'claude' just because that word also appears in their message. "
    "If the user already gave an explicit path (starts with / or ~, or otherwise "
    "clearly names a directory), use it AS-IS for add_project's path argument — do "
    "not call list_paths to double-check it first, and never discard a path you "
    "were explicitly given just because list_paths doesn't find it. list_paths only "
    "searches the configured workspace roots, which is a small subset of the "
    "filesystem — a miss there says nothing about whether an explicitly-given path "
    "is real. "
    "Only call list_paths when the path is genuinely unknown (the user gave just a "
    "name, not a path). A miss there does NOT mean the project doesn't exist either "
    "— it only means it's not under the configured workspace roots. Only call mkdir "
    "when the user is clearly asking to create something new (e.g. 'set up a new "
    "project for X'); if they're referring to an existing project by name and "
    "list_paths missed, say you couldn't find it under the configured roots and ask "
    "for the path instead of silently creating an empty directory. "
    "Keep the project name exactly as the user gave it — never split a multi-word "
    "name across the search filter and the registered project name. If they don't "
    "give a name at all, derive one from the target directory's own name (e.g. "
    "'/home/x/Obsidian' -> 'obsidian') — never default to the plugin's name (e.g. "
    "'cursor' or 'claude'), that collides with the plugin identifier itself and "
    "makes the project ambiguous to refer to later, especially once more than one "
    "project uses the same plugin. "
    "A project (registry) and a plugin (catalog) are different things, even when "
    "they share a name — e.g. project 'cursor' vs the catalog's 'cursor' plugin. "
    "*_project tools only ever affect that one project. If a project happens to be "
    "named the same as a plugin, that is never a reason to touch the catalog — you "
    "have no catalog-removal tool at all, on purpose. "
    "Keep replies short."
)


def _post(url: str, payload: dict, extra_headers: dict = None) -> dict:
    data = json.dumps(payload).encode()
    headers = {"Content-Type": "application/json"}
    if LLM_API_KEY:
        headers["Authorization"] = f"Bearer {LLM_API_KEY}"
    if extra_headers:
        headers.update(extra_headers)
    req = urllib.request.Request(url, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


# Conversation memory for the LLM layer, across separate incoming chat
# messages (not just within one turn's tool-calling rounds). Without this,
# a clarifying follow-up ("edvbartl/zahnrad" after being asked for a path)
# arrives with zero context connecting it to what was actually being asked
# — the model has no way to know it's an answer to its own question.
# Global, not per-channel: there's only ever one "controller" channel today.
# Holds plain user/assistant turns only (not the tool-call scaffolding a
# single turn generates internally) to stay compact.
_llm_history: list[dict] = []
LLM_HISTORY_TURNS = 20  # ~10 back-and-forths


async def run_llm_turn(user_text: str) -> str:
    if not LLM_API_KEY:
        return f"Unrecognized command. {HELP}"

    _llm_history.append({"role": "user", "content": user_text})
    messages = [{"role": "system", "content": SYSTEM_PROMPT}] + _llm_history[-LLM_HISTORY_TURNS:]

    final_reply = "Gave up after 5 tool-call rounds."
    loop = asyncio.get_event_loop()
    for _ in range(5):
        try:
            resp = await loop.run_in_executor(
                None,
                lambda: _post(f"{LLM_API_URL}/chat/completions",
                               {"model": LLM_MODEL, "messages": messages, "tools": TOOLS, "temperature": 0.2}),
            )
        except urllib.error.HTTPError as exc:
            final_reply = f"LLM error: HTTP {exc.code}"
            break
        except Exception as exc:
            final_reply = f"LLM error: {exc}"
            break

        msg = resp["choices"][0]["message"]
        tool_calls = msg.get("tool_calls") or []
        if not tool_calls:
            final_reply = msg.get("content") or "(no reply)"
            break

        messages.append(msg)
        for tc in tool_calls:
            fn_name = tc["function"]["name"]
            try:
                fn_args = json.loads(tc["function"].get("arguments") or "{}")
            except json.JSONDecodeError:
                fn_args = {}
            impl = _TOOL_IMPL.get(fn_name)
            if impl is None:
                result = f"unknown tool {fn_name}"
            else:
                try:
                    out = impl(**fn_args)
                    result = await out if asyncio.iscoroutine(out) else out
                except Exception as exc:
                    result = f"error: {exc}"
            _log(f"llm tool_call {fn_name}({fn_args}) -> {str(result)[:200]!r}")
            messages.append({"role": "tool", "tool_call_id": tc["id"], "content": str(result)})

    _llm_history.append({"role": "assistant", "content": final_reply})
    del _llm_history[:-LLM_HISTORY_TURNS]
    return final_reply


# ---------------------------------------------------------------------------
# sijba plugin protocol (control channel)
# ---------------------------------------------------------------------------

class ControllerClient:
    def __init__(self):
        self.ws = None
        self.shutdown_event = asyncio.Event()
        self._reconnect_backoff = 5

    async def connect(self):
        ws_url = SIJBA_URL.replace("https://", "wss://").replace("http://", "ws://")
        ws_url = f"{ws_url}/api/plugin/ws?token={SIJBA_TOKEN}"
        _log(f"Connecting to {ws_url} as {PLUGIN_ID}")
        self.ws = await websockets.connect(ws_url, ping_interval=20, ping_timeout=10, close_timeout=5)
        _log("Connected!")

        await self.ws.send(json.dumps({
            "type": "register",
            "plugin_id": PLUGIN_ID,
            "capabilities": ["conversation", "tools"],
            "channels": {
                CHANNEL_ID: {
                    "label": "Agent Controller" if CONTROLLER_NAME == "controller"
                             else f"Agent Controller ({CONTROLLER_NAME})",
                    "hint": (
                        f"Manage per-project coding-agent sessions. Trigger: '{CONTROLLER_NAME} ...'. " + HELP
                    ),
                    "triggers": [CONTROLLER_NAME, f"@{CONTROLLER_NAME}"],
                    "capabilities": ["admin"],
                    "metadata": {},
                }
            },
        }))
        self._reconnect_backoff = 5
        await self.message_loop()

    async def message_loop(self):
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                mtype = msg.get("type")
                if mtype == "registered":
                    _log(f"Plugin registered: {msg['plugin_id']}")
                elif mtype == "channel_registered":
                    _log(f"Channel registered: {msg['channel_id']} (ok={msg.get('ok')})")
                elif mtype == "message":
                    await self.handle_message(msg)
                elif mtype == "ping":
                    await self.ws.send(json.dumps({"type": "pong"}))
                elif mtype == "error":
                    _log(f"Server error: {msg.get('message')}")
        except websockets.ConnectionClosed:
            _log("Connection closed")
        except Exception as exc:
            _log(f"Message loop error: {exc}")

    async def handle_message(self, msg: dict):
        channel_id = msg["channel"]
        payload = msg["payload"]
        envelope_id = payload["envelope_id"]
        text = payload["text"]

        for trigger in (CONTROLLER_NAME, f"@{CONTROLLER_NAME}"):
            if text.lower().startswith(trigger):
                text = text[len(trigger):].strip()
                break
        if not text:
            text = "help"

        reply = await dispatch_command(text)
        if reply is None:
            reply = await run_llm_turn(text)

        try:
            await self.ws.send(json.dumps({
                "type": "reply", "channel": channel_id, "envelope_id": envelope_id,
                "text": reply, "continue": True,
            }))
        except Exception as exc:
            _log(f"Failed to send reply: {exc}")

    async def run_forever(self):
        loop = asyncio.get_event_loop()

        def _request_shutdown() -> None:
            # Just setting the event isn't enough — the loop below is
            # blocked inside message_loop()'s `async for raw in self.ws`,
            # which only unblocks on a connection event. Without closing
            # the socket here too, shutdown_event.is_set() never gets
            # re-checked until the connection drops on its own (a stale
            # sijba, a ping timeout, ...) — verified live: a plain
            # `systemctl restart` left this hung for the full 90s
            # DefaultTimeoutStopSec before systemd killed it by force,
            # which (with a delegated cgroup) took every supervised child
            # down with it instead of the controlled terminate_all_children()
            # cleanup below ever running.
            self.shutdown_event.set()
            if self.ws is not None:
                asyncio.ensure_future(self.ws.close())

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, _request_shutdown)
            except NotImplementedError:
                pass

        while not self.shutdown_event.is_set():
            try:
                await self.connect()
            except Exception as exc:
                _log(f"Connection failed: {exc}")
            if self.shutdown_event.is_set():
                break
            _log(f"Reconnecting in {self._reconnect_backoff}s...")
            try:
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=self._reconnect_backoff)
            except asyncio.TimeoutError:
                pass
            self._reconnect_backoff = min(self._reconnect_backoff * 2, 300)

        await terminate_all_children()
        _log("Shutdown complete")


async def main() -> None:
    boot()
    client = ControllerClient()
    await client.run_forever()


if __name__ == "__main__":
    asyncio.run(main())
