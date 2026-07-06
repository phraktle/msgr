#!/usr/bin/env python3
"""msgr — minimal multi-environment channel mailbox CLI (Slack, Telegram).

Built for LLM agents and shell scripts: `read` is a mailbox (returns only new
messages since that consumer's last read, then advances a cursor), `send`
takes args or stdin, output is plain chronological text (or --json).

Addresses:
    env#channel      channel in a named environment (Slack channel, Telegram chat)
    env@person       direct message to a person in that environment
    #channel  @person   the default environment ($MSGR_ENV, or config
                        default_env, or the only one configured)
    ops              any alias defined in the config

Examples:
    msgr send "#ops" "deploy finished"
    echo "long report..." | msgr send standup
    msgr read "news@weather_updates" --as morning-loop
    msgr read "#alerts" --last 50
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
    os.path.expanduser("~/.config/msgr/config.toml"),
    "/etc/msgr/config.toml",
]
STATE_DIR = pathlib.Path(
    os.environ.get("XDG_STATE_HOME", os.path.expanduser("~/.local/state"))
) / "msgr"

ADDR_RE = re.compile(r"^([A-Za-z0-9_-]*)([#@])(.+)$")


def die(msg, code=1):
    print(f"msgr: {msg}", file=sys.stderr)
    sys.exit(code)


def load_config():
    import tomllib

    for p in CONFIG_CANDIDATES:
        if p and os.path.isfile(p):
            with open(p, "rb") as f:
                return tomllib.load(f)
    return {}


def pick_env(cfg, name=None):
    envs = cfg.get("envs", {})
    if not envs:
        die("no environments configured (add [envs.<name>] to the config)")
    name = (name or os.environ.get("MSGR_ENV") or cfg.get("default_env")
            or (next(iter(envs)) if len(envs) == 1 else None))
    if not name:
        die("multiple environments configured — set default_env or $MSGR_ENV")
    if name not in envs:
        die(f"unknown environment '{name}'")
    return name, envs[name]


def resolve_addr(cfg, addr):
    """Return (env_name, env_cfg, kind, target); kind is '#' or '@'."""
    aliases, seen = cfg.get("aliases", {}), set()
    while addr in aliases and addr not in seen:
        seen.add(addr)
        addr = aliases[addr]
    m = ADDR_RE.match(addr)
    if not m:
        die(f"bad address '{addr}' — expected [env]#channel or [env]@person, "
            f"or an alias from the config")
    env_name, kind, target = m.groups()
    name, env = pick_env(cfg, env_name or None)
    return name, env, kind, target


def platform_client(env_name, env):
    plat = env.get("platform")
    if plat == "slack":
        return Slack(env_name, env)
    if plat == "telegram":
        return Telegram(env_name, env)
    die(f"environment '{env_name}': unknown platform '{plat}'")


def cursor_path(consumer, env_name, kind, target):
    safe = re.sub(r"[^A-Za-z0-9@#._-]", "_", f"{env_name}{kind}{target}")
    return STATE_DIR / "cursors" / f"{consumer}~{safe}"


def cursor_get(consumer, env_name, kind, target):
    p = cursor_path(consumer, env_name, kind, target)
    return p.read_text().strip() if p.exists() else None


def cursor_set(consumer, env_name, kind, target, value):
    p = cursor_path(consumer, env_name, kind, target)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(str(value))


def name_cache_path(env_name, name):
    safe = re.sub(r"[^A-Za-z0-9._-]", "_", name)
    return STATE_DIR / "names" / f"{env_name}#{safe}"


def name_cache_get(env_name, name):
    p = name_cache_path(env_name, name)
    return p.read_text().strip() if p.exists() else None


def name_cache_set(env_name, name, cid):
    p = name_cache_path(env_name, name)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(cid)


# ---------------------------------------------------------------- Slack

class Slack:
    def __init__(self, env_name, env):
        self.env_name = env_name
        self.token = env.get("bot_token")
        self.app_token = env.get("app_token")
        self.owner = env.get("owner")
        if not self.token:
            die(f"environment '{env_name}': no bot_token")
        self._users = {}

    def api(self, method, _quiet=False, **params):
        data = urllib.parse.urlencode(params).encode()
        req = urllib.request.Request(
            f"https://slack.com/api/{method}", data=data,
            headers={"Authorization": f"Bearer {self.token}"})
        with urllib.request.urlopen(req, timeout=30) as r:
            resp = json.load(r)
        if not resp.get("ok"):
            if _quiet:
                raise SystemExit(1)
            die(f"slack {method}: {resp.get('error')}")
        return resp

    def _find_user(self, name):
        name = name.lstrip("@").lower()
        cursor = ""
        while True:
            resp = self.api("users.list", limit=500, cursor=cursor)
            for u in resp["members"]:
                cands = {u.get("name", ""), u.get("real_name", ""),
                         u.get("profile", {}).get("display_name", "")}
                if name in {c.lower() for c in cands if c}:
                    return u["id"]
            cursor = resp.get("response_metadata", {}).get("next_cursor", "")
            if not cursor:
                die(f"user '@{name}' not found")

    def target_id(self, kind, target):
        if kind == "@":
            uid = target if re.fullmatch(r"[UW][A-Z0-9]{8,}", target) \
                else self._find_user(target)
            return self.api("conversations.open", users=uid)["channel"]["id"]
        if re.fullmatch(r"[CGD][A-Z0-9]{8,}", target):
            return target
        name = target.lstrip("#")
        cached = name_cache_get(self.env_name, name)
        if cached:
            return cached
        for types in ("public_channel,private_channel", "public_channel"):
            cursor, ok = "", True
            while True:
                try:
                    resp = self.api("conversations.list", _quiet=True,
                                    types=types, limit=999, cursor=cursor)
                except SystemExit:
                    ok = False
                    break
                for c in resp["channels"]:
                    if c["name"] == name:
                        return c["id"]
                cursor = resp.get("response_metadata", {}) \
                    .get("next_cursor", "")
                if not cursor:
                    break
            if ok:
                break
        die(f"channel #{name} not found by name — for private channels use "
            f"the channel ID or an alias, or grant the app the groups:read "
            f"scope (sending by #name works regardless if the bot is a "
            f"member)")

    def username(self, uid):
        if not uid:
            return "?"
        if uid not in self._users:
            try:
                u = self.api("users.info", _quiet=True, user=uid)["user"]
                self._users[uid] = u.get("profile", {}).get("display_name") \
                    or u.get("real_name") or uid
            except SystemExit:
                self._users[uid] = uid
        return self._users[uid]

    def react(self, kind, target, ts, emoji):
        cid = self.target_id(kind, target)
        self.api("reactions.add", channel=cid, timestamp=ts,
                 name=emoji.strip(":"))

    def send(self, kind, target, text, thread=None):
        # chat.postMessage resolves #names itself for channels the bot is in
        # (including private ones) — only resolve when it's a person.
        chan = self.target_id(kind, target) if kind == "@" else \
            (target if target.startswith("#")
             or re.fullmatch(r"[CGD][A-Z0-9]{8,}", target) else "#" + target)
        params = {"channel": chan, "text": text}
        if thread:
            params["thread_ts"] = thread
        resp = self.api("chat.postMessage", **params)
        if kind == "#" and not re.fullmatch(r"[CGD][A-Z0-9]{8,}", target):
            name_cache_set(self.env_name, target.lstrip("#"), resp["channel"])
        return {"channel": resp["channel"], "ts": resp["ts"]}

    def read(self, kind, target, cursor=None, limit=100):
        cid = self.target_id(kind, target)
        params = {"channel": cid, "limit": min(limit, 200)}
        if cursor:
            params["oldest"] = cursor  # exclusive
        resp = self.api("conversations.history", **params)
        msgs = list(reversed(resp["messages"]))
        out = [{
            "env": self.env_name, "channel": cid, "ts": m["ts"],
            "thread": m.get("thread_ts"),
            "from": self.username(m.get("user") or m.get("bot_id")),
            "owner": bool(self.owner) and m.get("user") == self.owner,
            "text": m.get("text", ""),
        } for m in msgs]
        return out, (msgs[-1]["ts"] if msgs else cursor)

    def channels(self):
        try:
            chans = self.api("users.conversations", _quiet=True,
                             types="public_channel,private_channel",
                             limit=200)["channels"]
            note = ""
        except SystemExit:
            chans = self.api("users.conversations", types="public_channel",
                             limit=200)["channels"]
            note = ("(private channels hidden: app lacks groups:read; "
                    "use IDs or aliases)")
        return [(c["id"], "#" + c["name"],
                 "private" if c.get("is_private") else "public")
                for c in chans], note

    def listen(self, json_out):
        if not self.app_token:
            die(f"environment '{self.env_name}': listen needs app_token "
                f"(Slack app-level token with connections:write)")
        try:
            import websocket
        except ImportError:
            die("websocket-client not installed (pip install 'msgr[listen]')")
        import time

        while True:
            try:
                req = urllib.request.Request(
                    "https://slack.com/api/apps.connections.open", data=b"",
                    headers={"Authorization": f"Bearer {self.app_token}"})
                resp = json.load(urllib.request.urlopen(req, timeout=30))
                if not resp.get("ok"):
                    die(f"apps.connections.open: {resp.get('error')}")
                ws = websocket.create_connection(resp["url"], timeout=120)
                while True:
                    try:
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        ws.ping()
                        continue
                    env = json.loads(raw)
                    if env.get("type") == "disconnect":
                        break
                    if env.get("envelope_id"):
                        ws.send(json.dumps({"envelope_id": env["envelope_id"]}))
                    if env.get("type") != "events_api":
                        continue
                    ev = env.get("payload", {}).get("event", {})
                    if ev.get("type") != "message" or ev.get("subtype"):
                        continue
                    m = {"env": self.env_name, "channel": ev.get("channel"),
                         "user": ev.get("user"), "bot_id": ev.get("bot_id"),
                         "owner": bool(self.owner)
                         and ev.get("user") == self.owner,
                         "ts": ev.get("ts"), "thread": ev.get("thread_ts"),
                         "text": ev.get("text", "")}
                    print(json.dumps(m, ensure_ascii=False) if json_out
                          else f"[{m['ts']}] {m['channel']} "
                               f"{m['user'] or m['bot_id']}: {m['text']}",
                          flush=True)
                ws.close()
            except Exception as e:  # noqa: BLE001
                print(f"msgr listen: reconnecting ({e})", file=sys.stderr)
                time.sleep(5)


# ------------------------------------------------------------- Telegram

class Telegram:
    def __init__(self, env_name, env):
        self.env_name = env_name
        self.api_id = int(env.get("api_id") or 0)
        self.api_hash = env.get("api_hash")
        self.phone = env.get("phone")
        self.session = os.path.expanduser(
            env.get("session", f"~/.local/state/msgr/{env_name}.session"))
        if not self.api_id or not self.api_hash:
            die(f"environment '{env_name}': telegram api_id/api_hash not set")

    @staticmethod
    def _entity(kind, target):
        if re.fullmatch(r"-?\d+", target):
            return int(target)
        return target if target.startswith("@") else "@" + target

    def _client_cls(self):
        try:
            from telethon.sync import TelegramClient
        except ImportError:
            die("telethon not installed (pip install 'msgr[telegram]')")
        return TelegramClient

    def client(self):
        pathlib.Path(self.session).parent.mkdir(parents=True, exist_ok=True)
        c = self._client_cls()(self.session, self.api_id, self.api_hash)
        c.connect()
        if not c.is_user_authorized():
            die(f"telegram session not authorized — run: "
                f"msgr tg-login {self.env_name}")
        return c

    def login(self):
        pathlib.Path(self.session).parent.mkdir(parents=True, exist_ok=True)
        with self._client_cls()(self.session, self.api_id, self.api_hash) as c:
            c.start(phone=self.phone)
            me = c.get_me()
            print(f"logged in as {me.first_name} (@{me.username})")

    def send(self, kind, target, text, thread=None):
        with self.client() as c:
            m = c.send_message(self._entity(kind, target), text)
            return {"channel": target, "ts": str(m.id)}

    def read(self, kind, target, cursor=None, limit=100):
        with self.client() as c:
            entity = self._entity(kind, target)
            min_id = int(cursor) if cursor else 0
            kwargs = {"min_id": min_id} if min_id else {}
            msgs = list(c.get_messages(entity, limit=min(limit, 500), **kwargs))
            msgs.reverse()
            out = []
            for m in msgs:
                sender = getattr(m.sender, "username", None) \
                    or getattr(m.sender, "title", None) \
                    or getattr(m.chat, "title", None) or "?"
                out.append({"env": self.env_name, "channel": target,
                            "ts": str(m.id), "thread": None,
                            "from": sender, "text": m.text or ""})
            return out, (str(msgs[-1].id) if msgs else cursor)


# ------------------------------------------------------------------ CLI

def fmt(m):
    who = m["from"] + (" (owner)" if m.get("owner") else "")
    return f"[{m['ts']}] {who}: {m['text']}"


def main():
    ap = argparse.ArgumentParser(
        prog="msgr", description=__doc__,
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

    p = sub.add_parser("react", help="add a reaction emoji to a Slack message")
    p.add_argument("addr")
    p.add_argument("ts")
    p.add_argument("emoji")

    p = sub.add_parser("channels", help="list channels the bot can see")
    p.add_argument("env", nargs="?")

    p = sub.add_parser("listen", help="stream messages as they arrive (Slack)")
    p.add_argument("env", nargs="?")
    p.add_argument("--json", action="store_true", help="JSONL output")

    p = sub.add_parser("tg-login", help="one-time interactive Telegram login")
    p.add_argument("env", nargs="?")

    args = ap.parse_args()
    cfg = load_config()

    if args.cmd == "tg-login":
        name, env = pick_env(cfg, args.env)
        if env.get("platform") != "telegram":
            die(f"environment '{name}' is not telegram")
        Telegram(name, env).login()
        return

    if args.cmd == "channels":
        name, env = pick_env(cfg, args.env)
        client = platform_client(name, env)
        if not isinstance(client, Slack):
            die("channels is Slack-only for now")
        chans, note = client.channels()
        for cid, cname, kind in chans:
            print(f"{cid}\t{cname}\t{kind}")
        for alias, addr in cfg.get("aliases", {}).items():
            print(f"{addr}\t{alias}\talias")
        if note:
            print(note, file=sys.stderr)
        return

    if args.cmd == "listen":
        name, env = pick_env(cfg, args.env)
        client = platform_client(name, env)
        if not isinstance(client, Slack):
            die("listen is Slack-only for now")
        client.listen(args.json)
        return

    env_name, env, kind, target = resolve_addr(cfg, args.addr)
    client = platform_client(env_name, env)

    if args.cmd == "react":
        if not isinstance(client, Slack):
            die("react is Slack-only")
        client.react(kind, target, args.ts, args.emoji)
        return

    if args.cmd == "send":
        text = " ".join(args.text) if args.text else sys.stdin.read().strip()
        if not text:
            die("empty message")
        r = client.send(kind, target, text, thread=args.thread)
        print(f"sent to {env_name}{kind}{r['channel']} ts={r['ts']}")
        return

    if args.cmd == "read":
        cursor = None if args.last \
            else cursor_get(args.consumer, env_name, kind, target)
        limit = args.last or args.limit
        first_read = cursor is None and not args.last
        msgs, new_cursor = client.read(kind, target, cursor=cursor, limit=limit)
        if first_read:
            msgs = msgs[-20:]  # first contact: don't dump entire history
        for m in msgs:
            print(json.dumps(m, ensure_ascii=False) if args.json else fmt(m))
        if not args.peek and not args.last and new_cursor:
            cursor_set(args.consumer, env_name, kind, target, new_cursor)
        return


if __name__ == "__main__":
    main()
