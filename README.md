# msgr

Minimal multi-environment channel **mailbox** CLI — Slack and Telegram —
built for LLM agents and shell scripts.

The core idea: an agent shouldn't manage timestamps or connections. `read` is
a mailbox — it prints only what's new since that consumer's last read and
advances a cursor. `send` posts. Everything is a one-shot command; no daemon
required.

```bash
msgr send "#ops" "deploy finished"
echo "long report..." | msgr send standup       # alias from config
msgr read "news@daily_updates"                  # only new posts since last read
msgr read "#alerts" --as pnl-loop               # independent cursor per agent
msgr read "#alerts" --last 50                   # plain history, no cursor
msgr read "#alerts" --peek                      # look without consuming
msgr read "#alerts" --json                      # JSONL for scripts
msgr send "work@alice" "lunch?"                 # direct message
msgr read "#alerts" "#ops" --as watcher --block --timeout 3600
                                                # long-poll: block until any
                                                # address has new messages,
                                                # print them (exit 3: timeout)
msgr channels                                   # what the bot can see
msgr listen --json                              # stream messages (Socket Mode)
msgr react "#ops" 1712345678.123 white_check_mark
msgr tg-login news                              # one-time Telegram session setup
```

## Addresses

```
env#channel     channel in a named environment
env@person      direct message in that environment
#channel        default environment
@person         default environment
standup         any alias from the config
```

The default environment is `$MSGR_ENV`, else `default_env` from the config,
else the only environment configured.

## Config

First found of: `$MSGR_CONFIG`, `~/.config/msgr/config.toml`,
`/etc/msgr/config.toml`.

```toml
default_env = "work"

[envs.work]
platform = "slack"
bot_token = "xoxb-..."
app_token = "xapp-..."      # only needed for `listen` (Socket Mode)
owner = "U0123456789"       # optional: the operator's user ID — their
                            # messages get an authenticated "(owner)" mark
                            # in read/listen output, so agents can tell the
                            # operator apart from anyone merely claiming to be

[envs.family]
platform = "slack"
bot_token = "xoxb-..."

[envs.news]
platform = "telegram"
api_id = 12345
api_hash = "0123456789abcdef"
phone = "+15551234567"
session = "~/.local/state/msgr/news.session"   # default: <env>.session

[aliases]
standup = "work#standup"
alerts = "work#C0123456789"    # private Slack channels: use the ID
daily = "news@daily_updates"
```

## Notes

- **Slack**: the bot must be a member of channels it reads or posts to.
  Public channel names resolve via API; private channels need an ID or alias
  unless the app has the `groups:read` scope. `@person` resolves by username,
  display name, or `U…` ID. `listen` uses Socket Mode (needs an app-level
  token with `connections:write`); minimum bot scopes for the rest:
  `chat:write`, `channels:read`, `channels:history`, `users:read`
  (+ `groups:history` for private channels, `im:write` for DMs,
  `reactions:write` for react).
- **Telegram**: uses a user-account MTProto session (Telethon). Run
  `msgr tg-login <env>` once interactively; after that reads/sends are
  one-shot. The session file is as sensitive as being logged in — guard it.
- **`read --block`** is the long-poll gate for agent loops: if nothing is
  new it blocks (cheap API polling, no LLM anywhere) until one of the
  addresses has messages, then prints them and advances cursors atomically —
  wake and data in one command. Exit 3 on `--timeout`. A fresh cursor starts
  "from now" (a blocking read never fires on old history). Add `--peek` to
  block without consuming (shell-gate pattern: a supervisor blocks, then
  spawns an agent that reads for itself). Slack thread replies are included
  by default; `--no-threads` to exclude.
- **Attachments**: `read` downloads files (≤20 MB) to
  `~/.local/state/msgr/files/` and appends `[attachment: /path]` to the
  message — point your agent's file-reading tool at the path to view images.
  `--no-files` skips downloads. Slack needs the `files:read` scope; without
  it you get a `files_note` naming the undownloadable attachments.
- **Cursors** live in `~/.local/state/msgr/cursors/`, one per
  `(consumer, environment, channel)`. First read of a channel returns only
  the last 20 messages rather than all history.
- Not yet: Telegram in `listen`/`channels`, threads beyond Slack `--thread`
  replies, media attachments.

## Install

```bash
pip install -e .                       # Slack send/read: stdlib only
pip install -e '.[telegram,listen]'    # + Telethon, + Socket Mode listen
```
