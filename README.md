# msgr

Minimal multi-platform channel **mailbox** CLI — Slack and Telegram — built
for LLM agents and shell scripts.

The core idea: an agent shouldn't manage timestamps or connections. `read` is
a mailbox — it prints only what's new since that consumer's last read and
advances a cursor. `send` posts. Everything is a one-shot command; no daemon.

```bash
msgr send slack:#ops "deploy finished"
echo "long report..." | msgr send minion          # alias from config
msgr read tg:@binance_announcements               # only new posts since last read
msgr read slack:#alerts --as pnl-loop             # independent cursor per agent
msgr read slack:#alerts --last 50                 # plain history, no cursor
msgr read slack:#alerts --peek                    # look without consuming
msgr read slack:#alerts --json                    # JSONL for scripts
msgr channels                                     # Slack channels the bot is in
msgr tg-login                                     # one-time Telegram session setup
```

Addresses: `slack:#name`, `slack:C0123456789`, `tg:@channelhandle`, bare
`#name`/`C…` (Slack assumed), bare `@handle` (Telegram assumed), or any alias
defined in the config.

## Config

First found of: `$MSGR_CONFIG`, `/etc/phraklaude/msgr.toml`,
`~/.config/msgr/config.toml`.

```toml
owner = "U0123456789"          # Slack user ID of the human operator

[slack]
bot_token = "xoxb-..."         # falls back to $SLACK_BOT_TOKEN

[telegram]
api_id = 12345                 # falls back to $TELEGRAM_API_ID
api_hash = "..."               #   .. $TELEGRAM_API_HASH
phone = "+361234567"
session = "~/.local/state/msgr/telegram.session"

[aliases]
minion = "slack:C0BFD8VJFSR"   # private Slack channels need IDs (or groups:read scope)
news = "tg:@binance_announcements"
```

Tokens may also live in `KEY=VALUE` env files listed under `env_files`
(default: `/etc/phraklaude/slack.env`, `/etc/phraklaude/telegram.env`).

## Notes

- Slack: bot must be a member of channels it reads or posts to. Public
  channel names resolve via API; private channels need an ID or alias unless
  the app has the `groups:read` scope.
- Telegram: uses a user-account MTProto session (Telethon). Run `msgr
  tg-login` once interactively; after that reads/sends are one-shot. The
  session file is as sensitive as being logged in — keep it on one machine.
- Cursors live in `~/.local/state/msgr/cursors/`, one per
  `(consumer, channel)`. First read of a channel shows only the last 20
  messages rather than all history.
- Not yet: streaming/`listen` mode (interactive conversations are served by a
  separate bridge; polling covers reporting/monitoring loops), threads beyond
  Slack `--thread` replies, media.

## Install

```bash
pip install -e .            # msgr on PATH; Slack needs stdlib only
pip install -e '.[telegram]'  # + Telethon
```
