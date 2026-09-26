# sijba-agent-controller

Process supervisor for per-project coding-agent sessions, controllable from
sijba chat. Replaces the two previous one-off scripts
(`sijba_plugin_client.py`, `sijba-cursor-plugin/`'s old
`cursor_plugin_client.py`), each of which was wired to exactly one fixed
directory and ran as its own standalone systemd service — that code now
lives in [sijba-claude-plugin](https://REDACTED-GIT-HOST/sijba/sijba-claude-plugin)
and [sijba-cursor-plugin](https://REDACTED-GIT-HOST/sijba/sijba-cursor-plugin)
as `main.py`, one project directory per registry entry, supervised by this
controller instead.

## How it fits together

- **`controller.py`** — thin supervisor. Owns `registry.json` (which
  projects exist, where, which plugin runs them, active/archived/paused)
  and its own `controller` chat channel. Its only real job is *starting*
  things: every `active` registry entry gets its plugin's process kept
  alive with crash-loop backoff. A controller restart and a mid-run crash
  go through the exact same code path — nothing special to handle on
  reboot.
- **`catalog.py`** — the plugin catalog: which backends the controller
  knows how to run, and where they come from. There's no special-cased
  "builtin" — Claude and Cursor are `kind: git` entries too, pointing at
  [sijba-claude-plugin](https://REDACTED-GIT-HOST/sijba/sijba-claude-plugin)
  and [sijba-cursor-plugin](https://REDACTED-GIT-HOST/sijba/sijba-cursor-plugin)
  respectively. You can add your own `kind: git` (repo URL, cloned on first
  use), `kind: docker` (image, pulled on first use), or `kind: local` (a
  locally-installed command, no fetch step) plugins the same way. Unlike
  `registry.json` this **is** committed — it's config (what's possible),
  not local runtime state (what's currently running).
- **`registry.py`** — the shared JSON file both the controller and running
  plugin processes read/write (`registry.json`, gitignored — local machine
  state, not config).
- **`apis.py`** — the AI API registry: named chat-completions endpoints
  (`{url, key, description}`), e.g. `openrouter`. A catalog entry can
  declare `"api": "<name>"` instead of embedding its own URL/key; the
  controller resolves it fresh on every spawn and injects
  `AI_API_URL`/`AI_API_KEY`/`AI_API_NAME` into that plugin's environment.
  Holds secrets, so `apis.json` is gitignored like `registry.json`, not
  committed like `catalog.json`.

### Plugin contract

Every catalog entry is a separate, independent repo/image — nothing here
imports the controller's own code. A `kind: git` entry must contain a
`main.py` at its repo root that:

- reads `SIJBA_URL`/`SIJBA_TOKEN`/`PROJECT_NAME`/`WORK_DIR`/`REGISTRY_FILE`
  from its environment
- self-registers its own sijba channel (own WebSocket connection,
  independent of the controller)
- handles `/archive` and `/pause` locally: writes its own status directly
  into `REGISTRY_FILE`'s JSON (see `_set_registry_status` in
  [sijba-claude-plugin/main.py](https://REDACTED-GIT-HOST/sijba/sijba-claude-plugin)
  for the reference implementation — no shared import, just the same JSON
  shape) and exits with a reserved code (`90` archived / `91` paused,
  `exit_codes.py`) *before* actually stopping
- handles `/log [n]` from its own in-memory log buffer

The controller reads that exit code (or, for controller-initiated stops,
its own `_intentional_stop` flag) to tell "stopped on purpose" apart from
"crashed", so only the latter triggers a backoff-restart. It's cloned into
`_plugins/<name>/` (gitignored — fetched content, not authored here) the
first time a project uses it, then re-`git pull`ed on subsequent adds.

A `kind: docker` entry just needs an image that honors the same contract;
the controller runs it with `WORK_DIR` mounted at `/work` and
`REGISTRY_FILE` mounted at `/registry.json` inside the container.

### `kind: local` — plugins that don't speak the sijba protocol at all

Not every useful process fits the "self-registers a sijba channel" contract
above. `kind: local` is the escape hatch: just a command (`{name}`/
`{work_dir}` placeholders filled in at spawn time), run in the project's
directory inside a named tmux session, `sijba-<project>` — some commands
need a real terminal (an *interactive* session), and wrapping in tmux means
you can also `tmux attach -t sijba-<project>` from an actual shell, not
just however the command itself pairs with you. Wrapped once more in
`script -qc "..." /dev/null` since tmux itself needs something that looks
like a terminal to attach *from*, and the controller runs headless
(systemd, no tty) — `script`'s own exit stays tied to the tmux session's
actual lifetime either way, so crash-loop backoff still sees the right
process lifetime. stdout/stderr are muted for this kind (tmux's raw ANSI
screen redraws are noise in the shared journal, not useful for
troubleshooting) — unlike git/docker plugins, which still inherit.

The seeded example is `remote-control`:

```
catalog list
# remote-control [local]: claude --remote-control {name} — ...

add my-project --plugin remote-control
```

This starts `claude --remote-control my-project` in `my-project`'s
directory, supervised exactly like any other project (crash-loop backoff,
`archive`/`pause`/`restore`/`remove` all work the normal way) — but there's
**no sijba channel and no chat relay**: you talk to it by pairing the
Claude mobile app to the named Remote Control session directly, same as
you would from an interactive terminal. `controller status <name>` tells
you it's running; the conversation itself happens entirely outside sijba.

## Chat commands (channel `controller`, trigger `controller ...`)

```
list                                     all projects + live status
add <name> [--path P] [--plugin T]       register + start (default plugin: claude)
mkdir <name> [--root R]                  create a new project dir (does not register)
paths [filter]                           search directories under WORKSPACE_ROOTS
status <name>                            details for one project
archive <name>                           stop, no auto-restart
pause <name>                             stop, no auto-restart (until resumed)
restore <name> / resume <name>           reactivate
remove <name> [--wipe]                   unregister (--wipe also deletes the directory)
log [n]                                  controller's own last n log lines (default 10)
catalog list                             available plugins, flags any that need an unconfigured API
catalog add <name> --git URL [--ref R] [--api A]     register a git-sourced plugin
catalog add <name> --docker IMAGE [--api A]          register a docker-sourced plugin
catalog add <name> --local "cmd" [--api A]           register a local-command plugin
catalog set-env <name> --env K=V,K2=V2   update env overrides on an existing plugin (e.g. pi's PI_MODEL)
catalog remove <name>                    remove a plugin from the catalog
apis list                                registered AI APIs (name, URL, key set y/n)
apis add <name> --url URL [--key K]      register a new AI API
apis set-key <name> <key>                set/rotate an API's key
apis remove <name>                       remove an API from the registry
```

`--api A` names an entry from `apis list` (e.g. `openrouter`) whose
URL/key the plugin needs at runtime, injected as `AI_API_URL`/`AI_API_KEY`/
`AI_API_NAME` — see `apis.py`. `apis add`/`set-key`/`remove` write secrets,
so — like `catalog remove` — they're exact-syntax-only, never exposed to
the LLM tool-calling layer; that layer only gets a read-only `list_apis`
(no key values) to decide whether a plugin it wants to add is actually
usable yet.

Per-project channel (`<plugin>:<name>`, trigger `<name> ...`): plain text
goes to the agent; `/archive`, `/pause`, `/log [n]` are handled locally the
same way, scoped to that one project. The `claude` and `cursor` plugins
additionally support `/reset` — restarts the underlying session with no
prior conversation context (not part of the general plugin contract, just
something those two happen to implement).

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
# export CONTROLLER_NAME=controller-prod   # only if the default (controller-<hostname>) is ugly

python controller.py
```

Defaults to `controller-<hostname>` for `plugin_id`/`channel_id`/chat trigger, so
running one controller per host (the common case) just works without every
instance fighting over the same channel name — the last one to register a
given `channel_id` wins server-side, so two hosts both registering plain
`controller` silently steal the channel from each other. Set `CONTROLLER_NAME`
to override the whole name (e.g. for a friendlier trigger word than the raw
hostname).

## systemd

```bash
sudo cp sijba-agent-controller.service /etc/systemd/system/
# edit Environment= values
sudo systemctl enable --now sijba-agent-controller
```

Once projects are running through the controller, retire the two old
standalone services it replaces (`sijba-claude-plugin.service`,
`sijba-cursor-plugin.service`, if you had them installed) — the controller
now spawns the same code from the catalog instead:

```bash
sudo systemctl disable --now sijba-claude-plugin sijba-cursor-plugin
```
