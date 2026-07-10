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
msgr read "#alerts" --thread 1712345678.123     # one whole thread (root + replies)
msgr read "#alerts" --peek                      # look without consuming
msgr read "#alerts" --json                      # JSONL for scripts
msgr send "work:@alice" "lunch?"                 # direct message
msgr read "#alerts" "#ops" --as watcher --block --timeout 3600
                                                # long-poll: block until any
                                                # address has new messages,
                                                # print them (exit 3: timeout)
msgr list                                   # what the bot can see
msgr listen                                     # ingest into the durable spool
msgr read "acct:*" --as reception --follow      # durable, resumable live stream
msgr context "acct:*" --since 1d                # readable digest of recent chat
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
dl:*                 EVERY channel of an account (spool-backed read only)
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

## Durable spool (reliable real-time reads)

`read` polls an account's history on demand — great with zero setup, but a
consumer reading the raw real-time socket (`listen`) loses any message that
arrives during a restart/reconnect gap. The **spool** closes that gap:

- Run `msgr listen <account>` as an always-on **ingester**. Instead of printing
  events, it appends every one to a durable, append-only per-account log (add
  `--print` to also echo to stdout for debugging). The sole-listener flock
  guarantees exactly one writer.
- `read <addr…> --as <name>` then **auto-detects** the ingester (via that same
  flock) and reads from the spool with its per-consumer cursor. A reader can
  restart and **backfill** from where it left off — nothing is silently lost.
  If no ingester is running, `read` falls back to the on-demand history poll
  (unchanged), so `msgr read --last 10` on a laptop still works with no setup.
- `msgr read "acct:*" --as reception` consumes **every channel** of the account
  (a receptionist); `acct:#chan` filters to one channel. `--block` tails the
  spool live (returns after the first new batch; a fresh consumer starts "from
  now"). `--follow` is a **continuous** stream: it emits the retained backlog,
  then keeps tailing and prints each new event, advancing the cursor per event,
  until `--timeout` (0 = forever) — a durable, resumable drop-in for `listen`.
- `--last N` and `--thread <ts>` are always direct one-shot API calls, never
  spooled.

The journal doubles as ~a week of **situational-awareness memory**: it is
**time-retained** (`SPOOL_RETENTION_DAYS`, default 7 — tunable). Rotation drops
events older than the window (and caps size); `_seq` stays strictly increasing
across the prune (a persisted high-water mark survives even a prune-to-empty),
so cursors are never silently overtaken.

## `context` — situational-awareness digest

`msgr context [address] [--since <spec>]` renders the journal as a compact,
human/agent-readable digest (not JSONL) — load "what's going on in the chats"
in one shot. It reads the spool (offline, complete incl. thread replies).

```bash
msgr context "acct:*"                 # all channels, everything retained (~week)
msgr context "acct:#ops" --since 1d   # one channel, last day
msgr context "acct:*" --since today
msgr context "acct:*" --thread 1712345678.123   # zoom into one thread
msgr context "acct:*" --json          # filtered raw events (JSONL) for scripts
```

- Address grammar mirrors `read`: `acct:*` / `acct:` = all channels;
  `acct:#chan` = one channel; omitted = default account, all channels.
- `--since` accepts `7d` / `1d` / `2h` / `today` / `YYYY-MM-DD`; default is
  everything retained.
- Rendering groups by channel (readable `#name` when cached, else the id),
  orders chronologically, **nests thread replies** under their parent, marks
  the `(owner)`, and shows attachments as `[attachment: <name>]` — referenced,
  never inlined. If no journal exists yet, it prints a one-line note (run
  `msgr listen <account>` to start one).

Layout (under the state dir — `$MSGR_STATE_DIR`, else
`~/.local/state/msgr`, created `0700`):

```
<state>/spool/<account>.jsonl        append-only log; each line is a normal
                                     read entry + an internal "_seq"
<state>/cursors/<account>/<name>.json  per-consumer last-consumed _seq
```

`_seq` is a strictly-increasing, never-reused integer. The spool auto-rotates
(keeping the most recent lines) once it grows past a cap; `_seq` continues
across rotation. If a consumer falls so far behind that its cursor was rotated
away, `read` prints a one-line warning to stderr and resumes from the oldest
retained event (never silently skips, never errors).

## Message schema

`read` and `listen` emit one JSON object per message (same shape on every
platform):

| field | meaning |
|-------|---------|
| `id` | the message ID — pass it verbatim to `--thread` / `react` / replies |
| `time` | ISO8601 UTC timestamp |
| `account` | msgr account name |
| `channel` | channel / chat / folder id within the account |
| `addr` | canonical reply address (`account:channel`) |
| `thread` | parent/root message id, or null |
| `from` | sender display name |
| `user` | sender's platform id (raw), or null |
| `owner` | true if the sender is the configured operator (authenticated) |
| `text` | message body |
| `trust`, `reactions`, `files`, `files_note` | present when applicable |

Reformat `time`, never `id` (the id is the platform's message key). Add
`--text` for a human-readable rendering instead of JSON.

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
- **`read --thread <id>`** returns one whole thread — the root message plus
  every reply, reactions included — in the same JSON schema as a timeline
  read. It's a one-shot read (no cursor, no blocking): `conversations.history`
  returns only top-level timeline messages, so historical in-thread replies
  are invisible to a plain `read`; pass the id of the root (or any message in
  the thread) to pull the full exchange. Slack-only for now.
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
