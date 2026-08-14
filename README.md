# sijba-agent-controller

Process supervisor for per-project coding-agent sessions (Claude Code /
Cursor Agent), controllable from sijba chat. Replaces the two previous
one-off scripts (`sijba_plugin_client.py`, `sijba-cursor-plugin/`), each of
which was wired to exactly one fixed directory.

## How it fits together

- **`controller.py`** — thin supervisor. Owns `registry.json` (which
  projects exist, where, which tool, active/archived/paused) and its own
  `controller` chat channel. Its only real job is *starting* things: every
  `active` registry entry gets a `session_client.py` subprocess kept alive
  with crash-loop backoff. A controller restart and a mid-run crash go
  through the exact same code path — nothing special to handle on reboot.
- **`session_client.py`** — one process per project. Self-registers its own
  sijba channel (own WebSocket connection, independent of the controller),
  runs the actual Claude/Cursor turns, and handles `/archive`, `/pause`,
  `/log [n]` locally. On `/archive` or `/pause` it writes its own registry
  status and exits with a reserved code (`exit_codes.py`) *before* stopping
  — the controller reads that exit code (or, for controller-initiated stops,
  its own `_intentional_stop` flag) to tell "stopped on purpose" apart from
  "crashed", so only the latter triggers a backoff-restart.
- **`registry.py`** — the shared JSON file both processes read/write
  (`registry.json`, gitignored — local machine state, not config).

## Chat commands (channel `controller`, trigger `controller ...`)

```
list                                   all projects + live status
add <name> [--path P] [--tool T]       register + start (default tool: claude)
mkdir <name> [--root R]                create a new project dir (does not register)
paths [filter]                         search directories under WORKSPACE_ROOTS
status <name>                          details for one project
archive <name>                         stop, no auto-restart
pause <name>                           stop, no auto-restart (until resumed)
restore <name> / resume <name>         reactivate
remove <name> [--wipe]                 unregister (--wipe also deletes the directory)
log [n]                                controller's own last n log lines (default 10)
```

Per-project channel (`<tool>:<name>`, trigger `<name> ...`): plain text goes
to the agent; `/archive`, `/pause`, `/log [n]` are handled locally the same
way, scoped to that one project.

Free text that doesn't match the exact syntax above falls back to an
optional LLM tool-calling turn (OpenRouter-style, same env vars as
`plugins-builtin/router/main.py`) if `LLM_API_KEY` is set — the model can
only call the same `cmd_*` functions the exact commands use, never execute
anything itself.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install websockets

export SIJBA_URL=https://sijba.edv-bartl.at
export SIJBA_TOKEN=your-token-here
export WORKSPACE_ROOTS=/home/robert/workspaces/sijba   # comma-separated for multiple orgs

python controller.py
```

## systemd

```bash
sudo cp sijba-agent-controller.service /etc/systemd/system/
# edit Environment= values
sudo systemctl enable --now sijba-agent-controller
```

Once projects are running through the controller, retire the two old
services it replaces:

```bash
sudo systemctl disable --now sijba-claude-plugin sijba-cursor-plugin
```
