# msgr

Minimal multi-account channel **mailbox** CLI — Slack and Telegram —
built for LLM agents and shell scripts.

The core idea: an agent shouldn't manage timestamps or connections. `read` is
a mailbox — it prints only what's new since that consumer's last read and
advances a cursor. `send` posts. Everything is a one-shot command; no daemon
required. `read`/`listen` emit **JSONL by default** (structured, reliable for
agents & scripts); add `--text` for human-readable output.

```bash
msgr send "#ops" "deploy finished"
echo "long report..." | msgr send standup       # alias from config
msgr read "news:@daily_updates"                  # only new posts since last read
msgr read "#alerts" --as pnl-loop               # independent cursor per agent
msgr read "#alerts" --last 50 --text            # human-readable (ISO time)
msgr read "#alerts" --peek                      # look without consuming
msgr read "#alerts" --json                      # JSONL for scripts
msgr send "work:@alice" "lunch?"                 # direct message
msgr read "#alerts" "#ops" --as watcher --block --timeout 3600
                                                # long-poll: block until any
                                                # address has new messages,
                                                # print them (exit 3: timeout)
msgr list                                   # what the bot can see
msgr listen --json                              # stream messages (Socket Mode)
msgr react "#ops" 1712345678.123 white_check_mark
msgr login news                              # one-time Telegram session setup
```

## Addresses

URI-like: the **account is the scheme**, the target is written in the
platform's own syntax (like `mailto:foo@bar.com`).

```
dl:#ops              Slack channel in account "dl"
dl:@alice            DM with a person
dl:@                 the operator's own DM
tg:@some_channel     Telegram channel/handle
tg:-100123456        Telegram chat by numeric ID
robot:foo@bar.com    email recipient (email accounts, when supported)
robot:INBOX          mail folder
#ops  @alice  @      no prefix = the default account
standup              any alias from the config
```

The default account is `$MSGR_ACCOUNT`, else `default_account` in the
config, else the only account configured.

## Config

First found of: `$MSGR_CONFIG`, `~/.config/msgr/config.toml`,
`/etc/msgr/config.toml`.

```toml
default_account = "work"

[accounts.work]
platform = "slack"
bot_token = "xoxb-..."
app_token = "xapp-..."      # only needed for `listen` (Socket Mode)
owner = "U0123456789"       # optional: the operator's user ID — their
                            # messages get an authenticated "(owner)" mark
                            # in read/listen output, so agents can tell the
                            # operator apart from anyone merely claiming to be

[accounts.family]
platform = "slack"
bot_token = "xoxb-..."

[accounts.news]
platform = "telegram"
api_id = 12345
api_hash = "0123456789abcdef"
phone = "+15551234567"
session = "~/.local/state/msgr/news.session"   # default: <env>.session

[aliases]
standup = "work:#standup"
alerts = "work:C0123456789"    # private Slack channels: use the ID
daily = "news:@daily_updates"
```

## Read scoping

By default an account reads anything its account can see. `allow_read`
restricts it — for accounts with more access than the agent's business:

```toml
[accounts.personal]
platform = "telegram"
allow_read = ["@exchange_news", "@status_updates"]   # nothing else readable
```

## Posting is opt-in

Accounts are **quiet by default**: `read`/`listen` always work, but
`send`, `react`, and uploads are refused until the account is armed:

```toml
[accounts.work]
allow_post = true                      # whole account writable
[accounts.news]
# no allow_post -> pure notetaker: can read, can never post
[accounts.other]
allow_post = ["#ops", "@alice"]        # only these addresses
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
  `msgr login <account>` once interactively; after that reads/sends are
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
- **Message fields**: each message carries `ts` (the platform message ID —
  pass it verbatim to `--thread`/`react`; Slack's is an epoch-like key,
  Telegram's is the integer id) and `time` (ISO8601 UTC, human/LLM readable).
  Reformat `time`, never `ts`.
- **Cursors** live in `~/.local/state/msgr/cursors/`, one per
  `(consumer, account, channel)`. First read of a channel returns only
  the last 20 messages rather than all history.
- Not yet: Telegram in `listen`/`channels`; email as a platform (the model
  maps cleanly: env = mailbox account, `#folder` channels with IMAP UID
  cursors, `@address` sends via SMTP, MIME attachments to the spool,
  `allow_read` scoping to folders).

## Install

```bash
pip install -e .                       # Slack send/read: stdlib only
pip install -e '.[telegram,listen]'    # + Telethon, + Socket Mode listen
```
