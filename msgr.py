#!/usr/bin/env python3
"""msgr — minimal multi-platform channel mailbox CLI (Slack, Telegram).

Built for LLM agents and shell scripts: read is a mailbox (returns only new
messages, advances a per-consumer cursor), send takes stdin or args, output is
plain chronological text (or --json).

    msgr send slack:#ops "deploy done"
    msgr read tg:@binance_announcements
    echo "report..." | msgr send minion
"""

import argparse
import json
import os
import pathlib
import re
import sys
import urllib.parse
import urllib.request

CONFIG_CANDIDATES = [
    os.environ.get("MSGR_CONFIG"),
    "/etc/phraklaude/msgr.toml",
    os.path.expanduser("~/.config/msgr/config.toml"),
]
STATE_DIR = pathlib.Path(
    os.environ.get("XDG_STATE_HOME", os.path.expanduser("~/.local/state"))
) / "msgr"


def die(msg, code=1):
    print(f"msgr: {msg}", file=sys.stderr)
    sys.exit(code)


def load_config():
    import tomllib

    cfg = {}
    for p in CONFIG_CANDIDATES:
        if p and os.path.isfile(p):
            with open(p, "rb") as f:
                cfg = tomllib.load(f)
            break
    # env-file fallbacks: tokens may live in plain KEY=VALUE env files
    for envfile in cfg.get("env_files", ["/etc/phraklaude/slack.env",
                                         "/etc/phraklaude/telegram.env"]):
        if os.path.isfile(envfile) and os.access(envfile, os.R_OK):
            for line in open(envfile):
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())
    return cfg


def resolve_addr(cfg, addr):
    """Return (platform, target). Accepts alias, slack:#x, slack:CID, tg:@x,
    bare #x / CID (assumed Slack), bare @x (assumed Telegram)."""
    aliases = cfg.get("aliases", {})
    if addr in aliases:
        addr = aliases[addr]
    if ":" in addr:
        plat, _, target = addr.partition(":")
        plat = {"telegram": "tg"}.get(plat, plat)
        if plat not in ("slack", "tg"):
            die(f"unknown platform in address: {addr}")
        return plat, target
    if addr.startswith("@"):
        return "tg", addr
    if addr.startswith("#") or re.fullmatch(r"[CGD][A-Z0-9]{8,}", addr):
        return "slack", addr
    die(f"cannot resolve address '{addr}' (no alias, no platform prefix)")


def cursor_path(consumer, plat, target):
    safe = re.sub(r"[^A-Za-z0-9@#._-]", "_", f"{plat}:{target}")
    return STATE_DIR / "cursors" / f"{consumer}~{safe}"


def cursor_get(consumer, plat, target):
    p = cursor_path(consumer, plat, target)
    return p.read_text().strip() if p.exists() else None


def cursor_set(consumer, plat, target, value):
    p = cursor_path(consumer, plat, target)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(str(value))


# ---------------------------------------------------------------- Slack

class Slack:
    def __init__(self, cfg):
        s = cfg.get("slack", {})
        self.token = s.get("bot_token") or os.environ.get("SLACK_BOT_TOKEN")
        if not self.token:
            die("no Slack bot token (config slack.bot_token or SLACK_BOT_TOKEN)")
        self._users = {}

    def api(self, method, **params):
        data = urllib.parse.urlencode(params).encode()
        req = urllib.request.Request(
            f"https://slack.com/api/{method}", data=data,
            headers={"Authorization": f"Bearer {self.token}"})
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.load(r)
        if not resp.get("ok"):
            die(f"slack {method}: {resp.get('error')}")
        return resp

    def resolve_channel(self, target):
        if re.fullmatch(r"[CGD][A-Z0-9]{8,}", target):
            return target
        name = target.lstrip("#")
        cursor = ""
        while True:
            resp = self.api("conversations.list", types="public_channel",
                            limit=999, cursor=cursor)
            for c in resp["channels"]:
                if c["name"] == name:
                    return c["id"]
            cursor = resp.get("response_metadata", {}).get("next_cursor", "")
            if not cursor:
                break
        die(f"channel {target} not found — private channels need an ID or an "
            f"alias in the config (bot lacks groups:read)")

    def username(self, uid):
        if not uid:
            return "?"
        if uid not in self._users:
            try:
                u = self.api("users.info", user=uid)["user"]
                self._users[uid] = u.get("profile", {}).get("display_name") \
                    or u.get("real_name") or uid
            except SystemExit:
                self._users[uid] = uid
        return self._users[uid]

    def send(self, target, text, thread=None):
        params = {"channel": target, "text": text}
        if thread:
            params["thread_ts"] = thread
        resp = self.api("chat.postMessage", **params)
        return {"channel": resp["channel"], "ts": resp["ts"]}

    def read(self, target, cursor=None, limit=100):
        """Return (messages_oldest_first, new_cursor). cursor = slack ts."""
        cid = self.resolve_channel(target)
        params = {"channel": cid, "limit": min(limit, 200)}
        if cursor:
            params["oldest"] = cursor  # exclusive by default
        resp = self.api("conversations.history", **params)
        msgs = list(reversed(resp["messages"]))
        out = [{
            "platform": "slack", "channel": cid, "ts": m["ts"],
            "thread": m.get("thread_ts"),
            "from": self.username(m.get("user") or m.get("bot_id")),
            "text": m.get("text", ""),
        } for m in msgs]
        new_cursor = msgs[-1]["ts"] if msgs else cursor
        return out, new_cursor


# ------------------------------------------------------------- Telegram

class Telegram:
    def __init__(self, cfg):
        t = cfg.get("telegram", {})
        self.api_id = int(t.get("api_id") or os.environ.get("TELEGRAM_API_ID") or 0)
        self.api_hash = t.get("api_hash") or os.environ.get("TELEGRAM_API_HASH")
        self.phone = t.get("phone") or os.environ.get("TELEGRAM_PHONE")
        self.session = os.path.expanduser(
            t.get("session", "~/.local/state/msgr/telegram.session"))
        if not self.api_id or not self.api_hash or self.api_hash == "PLACEHOLDER":
            die("telegram not configured (api_id/api_hash)")

    def client(self):
        try:
            from telethon.sync import TelegramClient
        except ImportError:
            die("telethon not installed (pip install telethon)")
        pathlib.Path(self.session).parent.mkdir(parents=True, exist_ok=True)
        c = TelegramClient(self.session, self.api_id, self.api_hash)
        c.connect()
        if not c.is_user_authorized():
            die("telegram session not authorized — run: msgr tg-login")
        return c

    def login(self):
        try:
            from telethon.sync import TelegramClient
        except ImportError:
            die("telethon not installed (pip install telethon)")
        pathlib.Path(self.session).parent.mkdir(parents=True, exist_ok=True)
        with TelegramClient(self.session, self.api_id, self.api_hash) as c:
            c.start(phone=self.phone)
            me = c.get_me()
            print(f"logged in as {me.first_name} (@{me.username})")

    def send(self, target, text, thread=None):
        with self.client() as c:
            m = c.send_message(target, text)
            return {"channel": target, "ts": str(m.id)}

    def read(self, target, cursor=None, limit=100):
        with self.client() as c:
            min_id = int(cursor) if cursor else 0
            kwargs = {"min_id": min_id} if min_id else {"limit": min(limit, 100)}
            msgs = [m for m in c.get_messages(target, limit=min(limit, 500), **kwargs)]
            msgs.reverse()  # oldest first
            out = []
            for m in msgs:
                sender = getattr(m.sender, "username", None) \
                    or getattr(m.sender, "title", None) \
                    or getattr(m.chat, "title", None) or "?"
                out.append({
                    "platform": "tg", "channel": target, "ts": str(m.id),
                    "thread": None, "from": sender, "text": m.text or "",
                })
            new_cursor = str(msgs[-1].id) if msgs else cursor
            return out, new_cursor


# ------------------------------------------------------------------ CLI

def platform_client(cfg, plat):
    return Slack(cfg) if plat == "slack" else Telegram(cfg)


def fmt(m):
    return f"[{m['ts']}] {m['from']}: {m['text']}"


def main():
    ap = argparse.ArgumentParser(prog="msgr", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("send", help="send a message (text args or stdin)")
    p.add_argument("addr")
    p.add_argument("text", nargs="*")
    p.add_argument("--thread", help="Slack thread ts to reply in")

    p = sub.add_parser("read", help="mailbox read: new messages since last read")
    p.add_argument("addr")
    p.add_argument("--as", dest="consumer", default="default",
                   help="cursor namespace (per loop/agent)")
    p.add_argument("--peek", action="store_true", help="don't advance cursor")
    p.add_argument("--last", type=int, metavar="N",
                   help="ignore cursor, show last N messages")
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--json", action="store_true", help="JSONL output")

    p = sub.add_parser("channels", help="list Slack channels the bot is in")

    p = sub.add_parser("tg-login", help="interactive one-time Telegram login")

    args = ap.parse_args()
    cfg = load_config()

    if args.cmd == "tg-login":
        Telegram(cfg).login()
        return

    if args.cmd == "channels":
        s = Slack(cfg)
        resp = s.api("users.conversations", types="public_channel,private_channel",
                     limit=200)
        for c in resp["channels"]:
            kind = "private" if c.get("is_private") else "public"
            print(f"{c['id']}\t#{c['name']}\t{kind}")
        return

    plat, target = resolve_addr(cfg, args.addr)
    client = platform_client(cfg, plat)

    if args.cmd == "send":
        text = " ".join(args.text) if args.text else sys.stdin.read().strip()
        if not text:
            die("empty message")
        r = client.send(target, text, thread=getattr(args, "thread", None))
        print(f"sent to {plat}:{r['channel']} ts={r['ts']}")
        return

    if args.cmd == "read":
        cursor = None if args.last else cursor_get(args.consumer, plat, target)
        limit = args.last or args.limit
        first_read = cursor is None and not args.last
        msgs, new_cursor = client.read(target, cursor=cursor, limit=limit)
        if first_read:
            msgs = msgs[-20:]  # first contact: don't dump entire history
        for m in msgs:
            print(json.dumps(m, ensure_ascii=False) if args.json else fmt(m))
        if not args.peek and not args.last and new_cursor:
            cursor_set(args.consumer, plat, target, new_cursor)
        return


if __name__ == "__main__":
    main()
