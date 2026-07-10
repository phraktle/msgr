#!/usr/bin/env python3
"""Tests for msgr. Network-free: the Slack API is stubbed and synthetic events
are written into a temp spool via MSGR_STATE_DIR, so these exercise message
normalization, the thread-read path, and the whole durable-spool machinery
(append/seq, cursored read, resume, rotation, falloff, backend selection)
without a live workspace."""

import contextlib
import fcntl
import io
import json
import os
import pathlib
import re
import shutil
import sys
import tempfile
import unittest
from unittest import mock

import msgr


class FakeSlack(msgr.Slack):
    """A Slack client with no real token/network: canned API responses and a
    trivial username/id resolver, so read/thread logic can be tested offline."""

    def __init__(self, responses):
        self.env_name = "acct"
        self.token = "x"
        self.app_token = None
        self.owner = "U0OWNER"
        self.armed = False
        self.trust = None
        self._users = {}
        self._responses = responses  # method -> list of canned responses
        self.calls = []

    def api(self, method, _quiet=False, **params):
        self.calls.append((method, params))
        queue = self._responses.get(method)
        return queue.pop(0) if queue else {"ok": True}

    def target_id(self, kind, target):
        return "C0CHAN"

    def username(self, uid):
        return {"U0OWNER": "owner-name", "U0BOB": "bob"}.get(uid, uid or "?")


def _msg(ts, user, text, thread_ts=None, reactions=None):
    m = {"ts": ts, "user": user, "text": text}
    if thread_ts:
        m["thread_ts"] = thread_ts
    if reactions:
        m["reactions"] = reactions
    return m


class ThreadReadTest(unittest.TestCase):
    def test_thread_returns_root_and_replies_with_reactions(self):
        root = _msg("100.1", "U0OWNER", "root question", thread_ts="100.1",
                    reactions=[{"name": "eyes", "count": 2}])
        reply = _msg("100.2", "U0BOB", "a reply", thread_ts="100.1",
                     reactions=[{"name": "white_check_mark", "count": 1}])
        cl = FakeSlack({"conversations.replies": [
            {"ok": True, "messages": [root, reply], "has_more": False},
        ]})

        out = cl.thread("#", "chan", "100.1")

        # root + reply, oldest first
        self.assertEqual([m["id"] for m in out], ["100.1", "100.2"])
        # reactions preserved and flattened to name -> count
        self.assertEqual(out[0]["reactions"], {"eyes": 2})
        self.assertEqual(out[1]["reactions"], {"white_check_mark": 1})
        # owner flag is authenticated from the configured owner id
        self.assertTrue(out[0]["owner"])
        self.assertFalse(out[1]["owner"])
        # the Slack call maps to conversations.replies on the resolved channel
        method, params = cl.calls[0]
        self.assertEqual(method, "conversations.replies")
        self.assertEqual(params["channel"], "C0CHAN")
        self.assertEqual(params["ts"], "100.1")

    def test_thread_message_shape_matches_timeline_message(self):
        """A thread message and a timeline message must be shaped identically
        (same keys) — both go through Slack._entry."""
        m = _msg("200.1", "U0BOB", "hello")
        history = FakeSlack({"conversations.history": [
            {"ok": True, "messages": [m]}]})
        replies = FakeSlack({"conversations.replies": [
            {"ok": True, "messages": [m], "has_more": False}]})

        timeline, _ = history.read("#", "chan", cursor=None)
        thread = replies.thread("#", "chan", "200.1")

        self.assertEqual(set(timeline[0]), set(thread[0]))
        self.assertEqual(timeline[0], thread[0])

    def test_thread_paginates(self):
        page1 = {"ok": True, "messages": [_msg("1.1", "U0BOB", "a")],
                 "has_more": True,
                 "response_metadata": {"next_cursor": "CURS"}}
        page2 = {"ok": True, "messages": [_msg("1.2", "U0BOB", "b")],
                 "has_more": False}
        cl = FakeSlack({"conversations.replies": [page1, page2]})

        out = cl.thread("#", "chan", "1.1")

        self.assertEqual([m["id"] for m in out], ["1.1", "1.2"])
        self.assertEqual(cl.calls[1][1]["cursor"], "CURS")


# ------------------------------------------------------------- Spool tests

class SpoolBase(unittest.TestCase):
    """Point msgr's state dir at a fresh temp dir for each test."""

    def setUp(self):
        self.tmp = pathlib.Path(tempfile.mkdtemp(prefix="msgr-test-"))
        self._saved_state = msgr.STATE_DIR
        msgr.STATE_DIR = self.tmp

    def tearDown(self):
        msgr.STATE_DIR = self._saved_state
        shutil.rmtree(self.tmp, ignore_errors=True)

    def event(self, channel, text, ts="1.0", user="U0BOB", **extra):
        e = {"account": "acct", "channel": channel,
             "addr": f"acct:{channel}", "id": ts, "time": msgr.iso(ts),
             "thread": None, "from": "bob", "user": user,
             "owner": False, "text": text}
        e.update(extra)
        return e

    def append(self, channel, text, **kw):
        """Append one synthetic event through a fresh Spool (which re-seeds
        `_seq` from disk each time — as a restarted ingester would)."""
        return msgr.Spool("acct").append(self.event(channel, text, **kw))

    def raw_lines(self):
        p = msgr.spool_path("acct")
        return [json.loads(ln) for ln in p.read_text().splitlines() if ln]


class SpoolAppendTest(SpoolBase):
    def test_seq_strictly_increases_and_never_resets(self):
        s1 = self.append("C1", "one")
        s2 = self.append("C1", "two")
        s3 = self.append("C2", "three")
        self.assertEqual([s1, s2, s3], [1, 2, 3])
        seqs = [e["_seq"] for e in self.raw_lines()]
        self.assertEqual(seqs, [1, 2, 3])
        # a brand-new Spool object continues from the persisted max
        self.assertEqual(msgr.Spool("acct")._seq, 3)
        self.assertEqual(self.append("C1", "four"), 4)

    def test_spool_line_equals_read_entry_plus_seq(self):
        """A spooled line is exactly a normal read entry plus `_seq`, and the
        listen (socket) normalizer produces that same read entry."""
        cl = FakeSlack({})
        cl.env_name = "acct"
        # the entry a plain history read would emit
        read_entry = cl._entry(_msg("300.1", "U0BOB", "hi"), "C0CHAN")
        # the entry the ingester derives from a socket event
        ev = {"channel": "C0CHAN", "ts": "300.1", "user": "U0BOB",
              "text": "hi"}
        socket_entry = cl._socket_entry(ev)
        self.assertEqual(socket_entry, read_entry)  # identical schema+values

        seq = msgr.Spool("acct").append(socket_entry)
        line = self.raw_lines()[0]
        self.assertEqual(line, {**read_entry, "_seq": seq})
        # stripping _seq recovers the read entry byte-for-byte
        self.assertEqual({k: v for k, v in line.items() if k != "_seq"},
                         read_entry)


class SpoolDrainTest(SpoolBase):
    def test_emits_only_after_cursor_and_advances(self):
        self.append("C1", "a")
        self.append("C1", "b")
        msgs, nc, warn = msgr.spool_drain("acct", "watcher", None)
        self.assertEqual([m["text"] for m in msgs], ["a", "b"])
        self.assertEqual(nc, 2)
        self.assertFalse(warn)
        self.assertNotIn("_seq", msgs[0])  # _seq stripped from output
        msgr.spool_cursor_set("acct", "watcher", nc)

        # second read by the same consumer sees nothing new
        again, nc2, _ = msgr.spool_drain("acct", "watcher", None)
        self.assertEqual(again, [])
        self.assertEqual(nc2, 2)

        # a fresh consumer sees the whole backlog
        fresh, ncf, _ = msgr.spool_drain("acct", "other", None)
        self.assertEqual([m["text"] for m in fresh], ["a", "b"])
        self.assertEqual(ncf, 2)

    def test_resume_after_restart_no_loss_no_dup(self):
        self.append("C1", "a")
        self.append("C1", "b")
        msgs, nc, _ = msgr.spool_drain("acct", "loop", None)
        msgr.spool_cursor_set("acct", "loop", nc)
        self.assertEqual([m["text"] for m in msgs], ["a", "b"])

        # "restart": cursor is reloaded from disk; more events arrive
        self.assertEqual(msgr.spool_cursor_get("acct", "loop"), 2)
        self.append("C1", "c")
        self.append("C2", "d")
        msgs2, nc2, _ = msgr.spool_drain("acct", "loop", None)
        self.assertEqual([m["text"] for m in msgs2], ["c", "d"])  # exactly new
        self.assertEqual(nc2, 4)

    def test_channel_filter_vs_account_level(self):
        self.append("C1", "one")
        self.append("C2", "two")
        self.append("C1", "three")
        # account-level (channels=None) sees everything
        allmsgs, _, _ = msgr.spool_drain("acct", "recept", None)
        self.assertEqual([m["text"] for m in allmsgs], ["one", "two", "three"])
        # channel-filtered sees only C1, and the cursor advances only to the
        # max *emitted* seq (the C1 line at seq 3)
        c1, nc, _ = msgr.spool_drain("acct", "c1only", {"C1"})
        self.assertEqual([m["text"] for m in c1], ["one", "three"])
        self.assertEqual(nc, 3)


class SpoolRotationTest(SpoolBase):
    def test_rotation_drops_old_keeps_seq_and_in_window_cursor_works(self):
        sp = msgr.Spool("acct", max_lines=10, keep_lines=4)
        for i in range(9):
            sp.append(self.event("C1", f"m{i}"))       # seq 1..9, no rotation
        # a consumer read up to seq 7 — contiguous with the retained min (8)
        # after rotation, so it resumes cleanly with no gap/falloff
        msgr.spool_cursor_set("acct", "win", 7)
        sp.append(self.event("C1", "m9"))              # seq 10
        sp.append(self.event("C1", "m10"))             # seq 11 -> rotation

        lines = self.raw_lines()
        self.assertEqual(len(lines), 4)                # kept KEEP_LINES
        seqs = [e["_seq"] for e in lines]
        self.assertEqual(seqs, [8, 9, 10, 11])         # seq preserved & rising
        self.assertEqual(sp._seq, 11)

        # the in-window cursor (6) still resumes correctly, no warning
        msgs, nc, warn = msgr.spool_drain("acct", "win", None)
        self.assertFalse(warn)
        self.assertEqual([m["text"] for m in msgs],
                         ["m7", "m8", "m9", "m10"])    # seq 8..11
        self.assertEqual(nc, 11)


class SpoolFalloffTest(SpoolBase):
    def test_cursor_below_retained_min_warns_and_emits_from_oldest(self):
        sp = msgr.Spool("acct", max_lines=10, keep_lines=4)
        for i in range(11):
            sp.append(self.event("C1", f"m{i}"))       # rotates; keeps seq 8..11
        lines = self.raw_lines()
        self.assertEqual([e["_seq"] for e in lines], [8, 9, 10, 11])

        # a consumer stuck at seq 3 (long since rotated away)
        msgr.spool_cursor_set("acct", "slow", 3)
        msgs, nc, warn = msgr.spool_drain("acct", "slow", None)
        self.assertTrue(warn)                          # falloff detected
        self.assertEqual([m["text"] for m in msgs],
                         ["m7", "m8", "m9", "m10"])    # from oldest retained
        self.assertEqual(nc, 11)

    def test_fresh_consumer_backfills_without_warning(self):
        sp = msgr.Spool("acct", max_lines=10, keep_lines=4)
        for i in range(11):
            sp.append(self.event("C1", f"m{i}"))
        # fresh consumer (no cursor) gets the retained backlog, NOT a warning
        msgs, nc, warn = msgr.spool_drain("acct", "brandnew", None)
        self.assertFalse(warn)
        self.assertEqual(len(msgs), 4)
        self.assertEqual(nc, 11)


class BackendDecisionTest(SpoolBase):
    def test_ingester_active_reflects_flock(self):
        acct = "acct"
        lockpath = f"/tmp/.msgr-listen-{acct}.lock"
        # no holder -> not active
        self.assertFalse(msgr.ingester_active(acct))
        # simulate a live ingester by holding the exclusive flock
        holder = open(lockpath, "a+")
        fcntl.flock(holder, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            self.assertTrue(msgr.ingester_active(acct))
        finally:
            fcntl.flock(holder, fcntl.LOCK_UN)
            holder.close()
        # released -> not active again
        self.assertFalse(msgr.ingester_active(acct))
        try:
            os.unlink(lockpath)
        except OSError:
            pass


# ---------------------------------------------- read CLI, spool backend

class FakeSlackRead(msgr.Slack):
    """Networkless Slack client for driving the `read` command in spool mode.
    target_id maps `#name`/`@name` to a stable channel id so channel filters
    resolve without any API call."""

    def __init__(self, env_name, env):
        self.env_name = env_name
        self.token = "x"
        self.app_token = None
        self.owner = None
        self.armed = False
        self.trust = None
        self._users = {}

    def target_id(self, kind, target):
        return "C" + re.sub(r"[^A-Za-z0-9]", "", target)

    def read(self, *a, **k):  # should never be hit in spool mode
        raise AssertionError("poll read must not run when ingester is active")


class ReadSpoolCliTest(SpoolBase):
    CFG = {"accounts": {"acct": {"platform": "slack",
                                 "bot_token": "x", "app_token": "y"}}}

    def run_read(self, argv, ingester=True, cfg=None):
        cfg = cfg or self.CFG
        with mock.patch.object(msgr, "load_config", return_value=cfg), \
             mock.patch.object(msgr, "platform_client",
                               side_effect=lambda en, env:
                               FakeSlackRead(en, env)), \
             mock.patch.object(msgr, "ingester_active", return_value=ingester),\
             mock.patch.object(sys, "argv", ["msgr", "read"] + argv):
            buf, err = io.StringIO(), io.StringIO()
            code = 0
            try:
                with contextlib.redirect_stdout(buf), \
                        contextlib.redirect_stderr(err):
                    msgr.main()
            except SystemExit as e:
                code = e.code or 0
            return buf.getvalue(), err.getvalue(), code

    def jsonl(self, out):
        return [json.loads(ln) for ln in out.splitlines() if ln.strip()]

    def test_account_level_read_backfills_then_resumes(self):
        self.append("Cops", "hello", ts="1.0")
        self.append("Calerts", "beep", ts="2.0")

        out, _, _ = self.run_read(["acct:*", "--as", "recept"])
        got = self.jsonl(out)
        self.assertEqual([m["text"] for m in got], ["hello", "beep"])
        self.assertNotIn("_seq", got[0])          # schema unchanged

        # same consumer again: nothing new
        out2, _, _ = self.run_read(["acct:*", "--as", "recept"])
        self.assertEqual(self.jsonl(out2), [])

        # more arrives; resume gets exactly the new one
        self.append("Cops", "again", ts="3.0")
        out3, _, _ = self.run_read(["acct:*", "--as", "recept"])
        self.assertEqual([m["text"] for m in self.jsonl(out3)], ["again"])

    def test_channel_filtered_read(self):
        self.append("Cops", "for-ops", ts="1.0")
        self.append("Calerts", "for-alerts", ts="2.0")
        out, _, _ = self.run_read(["acct:#ops", "--as", "opsloop"])
        self.assertEqual([m["text"] for m in self.jsonl(out)], ["for-ops"])

    def test_fresh_consumer_sees_full_backlog(self):
        for i in range(30):
            self.append("Cops", f"m{i}", ts=f"{i}.0")
        out, _, _ = self.run_read(["acct:*", "--as", "late"])
        # spool backfill returns the WHOLE backlog (not capped at 20)
        self.assertEqual(len(self.jsonl(out)), 30)

    def test_poll_backend_when_no_ingester(self):
        """With no ingester, spool is bypassed and the poll path runs (here it
        would raise from FakeSlackRead.read, proving poll — not spool — ran)."""
        self.append("Cops", "spooled", ts="1.0")
        with self.assertRaises(AssertionError):
            self.run_read(["acct:#ops", "--as", "x"], ingester=False)

    def test_falloff_warns_on_stderr(self):
        sp = msgr.Spool("acct", max_lines=6, keep_lines=3)
        for i in range(7):
            sp.append(self.event("Cops", f"m{i}", ts=f"{i}.0"))
        msgr.spool_cursor_set("acct", "slow", 1)   # below retained min
        out, err, _ = self.run_read(["acct:*", "--as", "slow"])
        self.assertIn("fell behind", err)
        self.assertTrue(self.jsonl(out))            # still emits, no crash


if __name__ == "__main__":
    unittest.main()
