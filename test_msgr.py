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
import subprocess
import sys
import tempfile
import time
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


class ReactSwapTest(unittest.TestCase):
    def _armed(self):
        cl = FakeSlack({})
        cl.armed = True
        return cl

    def test_swap_adds_new_then_removes_old_in_order(self):
        cl = self._armed()
        cl.react("#", "chan", "1.5", "clock2", swap="clock1")
        self.assertEqual(
            [(m, p["name"]) for m, p in cl.calls],
            [("reactions.add", "clock2"), ("reactions.remove", "clock1")])

    def test_swap_survives_benign_add_failure(self):
        # already_reacted on the add must not abort the remove
        cl = self._armed()
        real_api = cl.api
        def flaky(method, _quiet=False, **params):
            real_api(method, **params)
            if method == "reactions.add":
                raise SystemExit(1)
            return {"ok": True}
        cl.api = flaky
        cl.react("#", "chan", "1.5", "clock2", swap="clock1")
        self.assertEqual([m for m, _ in cl.calls],
                         ["reactions.add", "reactions.remove"])

    def test_plain_react_unchanged(self):
        cl = self._armed()
        cl.react("#", "chan", "1.5", "eyes")
        cl.react("#", "chan", "1.5", "eyes", remove=True)
        self.assertEqual([m for m, _ in cl.calls],
                         ["reactions.add", "reactions.remove"])

    def test_swap_strips_colons(self):
        cl = self._armed()
        cl.react("#", "chan", "1.5", ":clock2:", swap=":clock1:")
        self.assertEqual([p["name"] for _, p in cl.calls],
                         ["clock2", "clock1"])


class RenderBlocksTest(unittest.TestCase):
    """Slack's `text` fallback omits in-message tables entirely (only the
    surrounding prose survives) — render_blocks recovers them at ingest and
    stubs other uncovered content-bearing block types."""

    TABLE = {"type": "table", "rows": [
        [{"type": "raw_text", "text": "tier"},
         {"type": "raw_text", "text": "cap"}],
        [{"type": "rich_text",
          "elements": [{"type": "rich_text_section",
                        "elements": [{"type": "text", "text": "huge "},
                                     {"type": "emoji", "name": "fire"}]}]},
         {"type": "raw_number", "text": "43"}],
        [{"type": "rich_text",
          "elements": [{"type": "rich_text_section",
                        "elements": [{"type": "user", "user_id": "U0BOB"},
                                     {"type": "link",
                                      "url": "https://x.io"}]}]},
         {"type": "raw_text", "text": "a|b"}],
    ]}

    def test_table_renders_markdown_pipe_table(self):
        out = msgr.render_blocks([self.TABLE])
        self.assertEqual(out.splitlines(), [
            "| tier | cap |",
            "| --- | --- |",
            "| huge :fire: | 43 |",
            "| <@U0BOB>https://x.io | a\\|b |",
        ])

    def test_text_covered_blocks_render_nothing(self):
        # the text fallback already carries these — no stub, no duplication
        blocks = [{"type": "rich_text", "elements": []},
                  {"type": "section", "text": {"type": "mrkdwn", "text": "x"}},
                  {"type": "divider"}, {"type": "header"},
                  {"type": "context"}, {"type": "actions"}]
        self.assertIsNone(msgr.render_blocks(blocks))

    def test_unknown_block_gets_visible_stub(self):
        out = msgr.render_blocks([{"type": "video", "title": "demo"}])
        self.assertEqual(out, "[video block: unrendered]")

    def test_malformed_never_raises(self):
        self.assertIsNone(msgr.render_blocks(None))
        self.assertIsNone(msgr.render_blocks("nope"))
        self.assertIsNone(msgr.render_blocks([None, "x", {}]))
        # garbage rows/cells degrade to '?', not a crash
        out = msgr.render_blocks([{"type": "table", "rows": [None, [{}], 7]}])
        self.assertIn("?", out)

    def test_entry_and_socket_entry_append_rendered_blocks(self):
        cl = FakeSlack({})
        m = {**_msg("400.1", "U0BOB", "tiers below"), "blocks": [self.TABLE]}
        e = cl._entry(m, "C0CHAN")
        self.assertTrue(e["text"].startswith("tiers below\n| tier | cap |"))
        # ingester path renders identically
        ev = {"channel": "C0CHAN", "ts": "400.1", "user": "U0BOB",
              "text": "tiers below", "blocks": [self.TABLE]}
        self.assertEqual(cl._socket_entry(ev)["text"], e["text"])


class FileNoteTest(unittest.TestCase):
    def test_partial_download_failure_names_the_failed_file(self):
        cl = FakeSlack({})
        cl._fetch_file = \
            lambda f: "/spool/ok.pdf" if f["id"] == "F1" else None
        m = {**_msg("500.1", "U0BOB", ""),
             "files": [{"id": "F1", "name": "ok.pdf"},
                       {"id": "F2", "name": "gone.pdf"}]}
        e = cl._entry(m, "C0CHAN")
        self.assertEqual(e["files"], ["/spool/ok.pdf"])   # success kept
        self.assertIn("gone.pdf", e["files_note"])        # failure named
        self.assertNotIn("ok.pdf", e["files_note"])

    def test_canvas_and_list_noted_as_not_exportable(self):
        cl = FakeSlack({})
        cl._fetch_file = \
            lambda f: self.fail("no download attempt for canvas/list")
        m = {**_msg("500.2", "U0BOB", ""),
             "files": [{"id": "F3", "name": "plan", "filetype": "quip",
                        "mode": "canvas"},
                       {"id": "F4", "title": "tasks", "filetype": "list",
                        "mode": "list"}]}
        e = cl._entry(m, "C0CHAN")
        self.assertIsNone(e["files"])
        self.assertIn("canvas 'plan': not exportable", e["files_note"])
        self.assertIn("list 'tasks': not exportable", e["files_note"])
        self.assertNotIn("files:read", e["files_note"])   # no scope guess


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

    def event(self, channel, text, ts=None, user="U0BOB", **extra):
        # default to "now" so retention (7-day window) doesn't drop test events
        if ts is None:
            ts = f"{time.time():.4f}"
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

    def test_socket_entry_downloads_files(self):
        """The ingester downloads attachments (spool consumers can't — no
        token cross-user) and journals PATHS; names + a note only when the
        download fails. Parity with _entry's files handling."""
        cl = FakeSlack({})
        cl.env_name = "acct"
        ev = {"channel": "C0CHAN", "ts": "300.2", "user": "U0BOB",
              "text": "", "files": [{"id": "F1", "name": "a.pdf"},
                                    {"id": "F2", "name": "b.pdf"}]}
        cl._fetch_file = lambda f: f"/spool/{f['id']}-{f['name']}"
        m = cl._socket_entry(ev)
        self.assertEqual(m["files"], ["/spool/F1-a.pdf", "/spool/F2-b.pdf"])
        self.assertNotIn("files_note", m)
        # read-path parity: same event shape through _entry
        e = cl._entry({**_msg("300.2", "U0BOB", ""),
                       "files": ev["files"]}, "C0CHAN")
        self.assertEqual(e["files"], m["files"])

        cl._fetch_file = lambda f: None  # download failure -> names + note
        m2 = cl._socket_entry(ev)
        self.assertIsNone(m2["files"])
        self.assertIn("a.pdf", m2["files_note"])


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


class SpoolRetentionTest(SpoolBase):
    def test_old_events_dropped_on_rotation_seq_and_cursor(self):
        sp = msgr.Spool("acct", max_lines=8, keep_lines=100, retention_days=7)
        now = time.time()
        old, recent = now - 10 * 86400, now - 1 * 86400
        for i in range(5):
            sp.append(self.event("C1", f"old{i}", ts=f"{old + i:.4f}"))
        for i in range(4):                              # 9th append -> rotation
            sp.append(self.event("C1", f"new{i}", ts=f"{recent + i:.4f}"))

        lines = self.raw_lines()
        self.assertEqual([e["text"] for e in lines],
                         ["new0", "new1", "new2", "new3"])  # old dropped by age
        self.assertEqual([e["_seq"] for e in lines], [6, 7, 8, 9])  # seq rises
        self.assertEqual(sp._seq, 9)

        # a within-window cursor (read up to new0 = seq 6) resumes with no gap
        msgr.spool_cursor_set("acct", "w", 6)
        msgs, nc, warn = msgr.spool_drain("acct", "w", None)
        self.assertFalse(warn)
        self.assertEqual([m["text"] for m in msgs], ["new1", "new2", "new3"])
        self.assertEqual(nc, 9)

    def test_periodic_prune_during_append_without_size_rotation(self):
        """The cheap periodic check prunes stale events even when the size cap
        is nowhere near — the common low-volume case."""
        sp = msgr.Spool("acct", max_lines=100000, keep_lines=100000,
                        retention_days=7)
        sp.PRUNE_EVERY = 3
        old = time.time() - 10 * 86400
        sp.append(self.event("C1", "old0", ts=f"{old:.4f}"))
        sp.append(self.event("C1", "old1", ts=f"{old + 1:.4f}"))
        sp.append(self.event("C1", "new", ts=f"{time.time():.4f}"))  # triggers
        self.assertEqual([e["text"] for e in self.raw_lines()], ["new"])
        self.assertEqual(sp._seq, 3)                    # seq unaffected by prune

    def test_seq_survives_prune_to_empty_across_restart(self):
        """If retention prunes the whole spool (no traffic for > the window),
        a restarted ingester must NOT reset `_seq` — a persisted high-water
        mark keeps it monotonic so no consumer cursor is silently overtaken."""
        sp = msgr.Spool("acct", max_lines=3, keep_lines=100, retention_days=7)
        old = time.time() - 10 * 86400
        for i in range(4):                    # >3 -> rotation; all old -> empty
            sp.append(self.event("C1", f"o{i}", ts=f"{old + i:.4f}"))
        self.assertEqual(self.raw_lines(), [])          # journal pruned to empty
        self.assertEqual(sp._seq, 4)
        self.assertEqual(msgr.spool_hwm_get("acct"), 4)
        # "restart": a fresh Spool reseeds from the high-water mark, not 0
        self.assertEqual(msgr.Spool("acct")._seq, 4)
        self.assertEqual(msgr.Spool("acct").append(self.event("C1", "fresh")),
                         5)


class SpoolArchiveTest(SpoolBase):
    def archived(self):
        d = msgr.spool_archive_dir("acct")
        out = {}
        for f in sorted(d.glob("*.jsonl")) if d.exists() else []:
            out[f.name] = [json.loads(ln)
                           for ln in f.read_text().splitlines() if ln]
        return out

    def test_size_rotation_archives_overflow_nothing_lost(self):
        sp = msgr.Spool("acct", max_lines=10, keep_lines=4)
        for i in range(11):
            sp.append(self.event("C1", f"m{i}"))   # 11th append -> rotation
        kept = [e["text"] for e in self.raw_lines()]
        self.assertEqual(kept, ["m7", "m8", "m9", "m10"])
        arch = [e["text"] for seg in self.archived().values() for e in seg]
        self.assertEqual(arch, [f"m{i}" for i in range(7)])  # overflow saved
        # union hot+archive = everything, no dup, _seq preserved
        seqs = sorted(e["_seq"] for seg in self.archived().values()
                      for e in seg) + [e["_seq"] for e in self.raw_lines()]
        self.assertEqual(seqs, list(range(1, 12)))

    def test_age_rotation_archives_to_weekly_segments(self):
        sp = msgr.Spool("acct", retention_days=7)
        old1 = time.time() - 30 * 86400
        old2 = time.time() - 20 * 86400
        sp.append(self.event("C1", "ancient", ts=f"{old1:.4f}"))
        sp.append(self.event("C1", "old", ts=f"{old2:.4f}"))
        sp.append(self.event("C1", "fresh"))
        sp._rotate()
        self.assertEqual([e["text"] for e in self.raw_lines()], ["fresh"])
        segs = self.archived()
        self.assertEqual(len(segs), 2)             # two distinct weeks
        self.assertEqual(list(segs), sorted(segs))  # names sort chronological
        texts = [e["text"] for seg in segs.values() for e in seg]
        self.assertEqual(sorted(texts), ["ancient", "old"])
        for name in segs:
            self.assertRegex(name, r"^\d{4}-W\d{2}\.jsonl$")

    def test_archive_iter_is_chronological_and_tolerant(self):
        sp = msgr.Spool("acct", retention_days=7)
        for days_ago, text in ((30, "a"), (20, "b"), (15, "c")):
            ts = time.time() - days_ago * 86400
            sp.append(self.event("C1", text, ts=f"{ts:.4f}"))
        sp._rotate()
        d = msgr.spool_archive_dir("acct")
        (d / "0000-W00.jsonl").write_text("not json\n\n")  # tolerated
        got = [e["text"] for e in msgr.spool_archive_iter("acct")]
        self.assertEqual(got, ["a", "b", "c"])


class ParseSinceUnitsTest(unittest.TestCase):
    def test_week_month_year_units(self):
        now = time.time()
        for spec, secs in (("2w", 14 * 86400), ("6mo", 180 * 86400),
                           ("1y", 365 * 86400), ("5m", 300)):
            got = msgr.parse_since(spec)
            self.assertAlmostEqual(now - got, secs, delta=5, msg=spec)


class SearchCliTest(SpoolBase):
    CFG = {"accounts": {"acct": {"platform": "slack",
                                 "bot_token": "x", "app_token": "y"}}}

    def run_search(self, argv):
        with mock.patch.object(msgr, "load_config", return_value=self.CFG), \
             mock.patch.object(msgr, "platform_client",
                               side_effect=lambda en, env:
                               FakeSlackRead(en, env)), \
             mock.patch.object(sys, "argv", ["msgr", "search"] + argv):
            buf, err = io.StringIO(), io.StringIO()
            code = 0
            try:
                with contextlib.redirect_stdout(buf), \
                        contextlib.redirect_stderr(err):
                    msgr.main()
            except SystemExit as e:
                code = e.code or 0
            return buf.getvalue(), err.getvalue(), code

    def seed(self):
        sp = msgr.Spool("acct", retention_days=7)
        ts = time.time() - 30 * 86400
        sp.append(self.event("Cops", "deploy failed on venue X",
                             ts=f"{ts:.4f}"))
        sp.append(self.event("Cops", "deploy went fine"))
        sp.append(self.event("Calerts", "unrelated beep"))
        sp._rotate()  # first event moves to the archive

    def test_search_spans_archive_and_hot_spool(self):
        self.seed()
        out, _, _ = self.run_search(["deploy", "--json"])
        texts = [json.loads(ln)["text"] for ln in out.splitlines() if ln]
        self.assertEqual(texts, ["deploy failed on venue X",
                                 "deploy went fine"])
        self.assertNotIn("_seq", out)

    def test_and_semantics_and_case_insensitive(self):
        self.seed()
        out, _, _ = self.run_search(["DEPLOY", "failed", "--json"])
        texts = [json.loads(ln)["text"] for ln in out.splitlines() if ln]
        self.assertEqual(texts, ["deploy failed on venue X"])

    def test_since_and_channel_scope(self):
        self.seed()
        out, _, _ = self.run_search(["deploy", "--since", "1w", "--json"])
        self.assertEqual(len([ln for ln in out.splitlines() if ln]), 1)
        out, _, _ = self.run_search(["beep", "--addr", "acct:#alerts",
                                     "--json"])
        texts = [json.loads(ln)["text"] for ln in out.splitlines() if ln]
        self.assertEqual(texts, ["unrelated beep"])

    def test_limit_keeps_newest(self):
        self.seed()
        out, _, _ = self.run_search(["deploy", "--limit", "1", "--json"])
        texts = [json.loads(ln)["text"] for ln in out.splitlines() if ln]
        self.assertEqual(texts, ["deploy went fine"])

    def test_rendered_output_mentions_hits(self):
        self.seed()
        out, _, _ = self.run_search(["deploy"])
        self.assertIn("search: deploy", out)
        self.assertIn("2 hit(s)", out)

    def test_bad_regex_dies_cleanly(self):
        self.seed()
        _, err, code = self.run_search(["deploy", "["])
        self.assertNotEqual(code, 0)
        self.assertIn("bad pattern", err)


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
            sp.append(self.event("Cops", f"m{i}"))  # recent ts (size rotation)
        msgr.spool_cursor_set("acct", "slow", 1)   # below retained min
        out, err, _ = self.run_read(["acct:*", "--as", "slow"])
        self.assertIn("fell behind", err)
        self.assertTrue(self.jsonl(out))            # still emits, no crash


class SendFileCaptionTest(unittest.TestCase):
    """CLI send with --file: an explicit "-" reads stdin into the upload's
    caption; the bare no-text-arg form stays caption-less without touching
    stdin (so it can't hang waiting on it)."""

    CFG = {"accounts": {"acct": {"platform": "slack", "bot_token": "x",
                                 "allow_post": True}}}

    class CaptureClient:
        def __init__(self):
            self.calls = []

        def send_file(self, kind, target, path, text=None, thread=None):
            self.calls.append({"path": path, "text": text, "thread": thread})
            return {"channel": "C1", "ts": "(file)"}

        def send(self, kind, target, text, thread=None):
            self.calls.append({"text": text, "thread": thread})
            return {"channel": "C1", "ts": "1.0"}

    def run_send(self, argv, stdin):
        client = self.CaptureClient()
        with mock.patch.object(msgr, "load_config", return_value=self.CFG), \
             mock.patch.object(msgr, "platform_client", return_value=client), \
             mock.patch.object(sys, "stdin", stdin), \
             mock.patch.object(sys, "argv", ["msgr", "send"] + argv):
            buf, err = io.StringIO(), io.StringIO()
            code = 0
            try:
                with contextlib.redirect_stdout(buf), \
                        contextlib.redirect_stderr(err):
                    msgr.main()
            except SystemExit as e:
                code = e.code or 0
        return client.calls, buf.getvalue(), err.getvalue(), code

    def setUp(self):
        fd, self.png = tempfile.mkstemp(suffix=".png")
        os.close(fd)
        self.addCleanup(os.unlink, self.png)

    def test_explicit_dash_pipes_stdin_caption_with_file(self):
        calls, out, err, code = self.run_send(
            ["acct:#ops", "--file", self.png, "-"],
            io.StringIO("*Facts*\nline two\n"))
        self.assertEqual(code, 0, err)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["text"], "*Facts*\nline two")

    def test_bare_file_send_never_reads_stdin(self):
        stdin = mock.Mock()
        stdin.read.side_effect = AssertionError("stdin must not be read")
        calls, out, err, code = self.run_send(
            ["acct:#ops", "--file", self.png], stdin)
        self.assertEqual(code, 0, err)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0]["text"], "")

    def test_inline_text_caption_with_file(self):
        calls, out, err, code = self.run_send(
            ["acct:#ops", "one line", "--file", self.png],
            io.StringIO(""))
        self.assertEqual(code, 0, err)
        self.assertEqual(calls[0]["text"], "one line")


class EditIngestTest(unittest.TestCase):
    """message_changed / message_deleted -> typed spool entries. The filters
    keep bot housekeeping, our own bot's amendments (a coordinator's status
    line would otherwise loop back into the spool), and Slack's
    unchanged-text unfurl housekeeping out of the journal."""

    def client(self):
        cl = FakeSlack({})
        cl._self = "U0SELF"                    # cached auth.test identity
        return cl

    @staticmethod
    def changed(new_text="new", prev_text="old", user="U0BOB", **inner_extra):
        inner = {"ts": "100.1", "user": user, "text": new_text,
                 "thread_ts": "100.0"}
        inner.update(inner_extra)
        return {"type": "message", "subtype": "message_changed",
                "channel": "C0CHAN", "ts": "200.5", "message": inner,
                "previous_message": {"ts": "100.1", "user": user,
                                     "text": prev_text}}

    def test_edit_entry_shape(self):
        e = self.client()._edit_entry(self.changed())
        self.assertEqual(e["id"], "100.1")     # ORIGINAL ts stays the id
        self.assertEqual(e["edit_of"], "100.1")
        self.assertEqual(e["prev_text"], "old")
        self.assertEqual(e["text"], "new")
        self.assertEqual(e["time"], msgr.iso("200.5"))   # edit time, not post
        self.assertEqual(e["thread"], "100.0")
        self.assertEqual(e["user"], "U0BOB")
        self.assertFalse(e["owner"])

    def test_owner_flag_authenticated_on_edit(self):
        e = self.client()._edit_entry(self.changed(user="U0OWNER"))
        self.assertTrue(e["owner"])

    def test_unchanged_text_skipped(self):
        # Slack re-fires message_changed for its own link-unfurl housekeeping
        self.assertIsNone(self.client()._edit_entry(
            self.changed(new_text="same", prev_text="same")))

    def test_bot_and_self_edits_skipped(self):
        cl = self.client()
        self.assertIsNone(cl._edit_entry(
            self.changed(**{"subtype": "bot_message"})))
        self.assertIsNone(cl._edit_entry(self.changed(bot_id="B123")))
        self.assertIsNone(cl._edit_entry(self.changed(user="U0SELF")))

    def test_delete_entry_shape(self):
        ev = {"type": "message", "subtype": "message_deleted",
              "channel": "C0CHAN", "ts": "200.6", "deleted_ts": "100.1",
              "previous_message": {"ts": "100.1", "user": "U0BOB",
                                   "text": "the ask", "thread_ts": "100.0"}}
        e = self.client()._delete_entry(ev)
        self.assertEqual(e["id"], "100.1")
        self.assertEqual(e["delete_of"], "100.1")
        self.assertEqual(e["text"], "")
        self.assertEqual(e["prev_text"], "the ask")
        self.assertEqual(e["thread"], "100.0")

    def test_delete_without_previous_message_still_ingests(self):
        ev = {"type": "message", "subtype": "message_deleted",
              "channel": "C0CHAN", "ts": "200.6", "deleted_ts": "100.1"}
        e = self.client()._delete_entry(ev)
        self.assertEqual(e["delete_of"], "100.1")
        self.assertIsNone(e["user"])           # author unknown, tombstone kept

    def test_self_and_bot_deletes_skipped(self):
        cl = self.client()
        base = {"type": "message", "subtype": "message_deleted",
                "channel": "C0CHAN", "ts": "200.6", "deleted_ts": "100.1"}
        self.assertIsNone(cl._delete_entry(
            {**base, "previous_message": {"user": "U0SELF", "text": "x"}}))
        self.assertIsNone(cl._delete_entry(
            {**base, "previous_message": {"bot_id": "B123", "text": "x"}}))

    def test_human_rendering_markers(self):
        edit = self.client()._edit_entry(self.changed())
        self.assertIn("(edited) new", msgr.fmt(edit))
        self.assertIn("(edited) new", "\n".join(msgr._ctx_lines(edit, "")))
        dele = {"from": "bob", "text": "", "delete_of": "1.0", "id": "1.0"}
        self.assertIn("(deleted)", msgr.fmt(dele))
        # normal entries render untouched
        self.assertNotIn("(edited)", msgr.fmt({"from": "bob", "text": "hi"}))


class EditDeleteVerbTest(unittest.TestCase):
    """CLI edit/delete: send's addressing + arming rules, Slack-only."""

    CFG = {"accounts": {"acct": {"platform": "slack", "bot_token": "x",
                                 "allow_post": True}}}

    def run_cmd(self, argv, cfg=None, client=None, arm=True, stdin=""):
        if client is None:
            client = FakeSlack({})
        client.armed = arm
        with mock.patch.object(msgr, "load_config",
                               return_value=cfg or self.CFG), \
             mock.patch.object(msgr, "platform_client", return_value=client), \
             mock.patch.object(sys, "stdin", io.StringIO(stdin)), \
             mock.patch.object(sys, "argv", ["msgr"] + argv):
            buf, err = io.StringIO(), io.StringIO()
            code = 0
            try:
                with contextlib.redirect_stdout(buf), \
                        contextlib.redirect_stderr(err):
                    msgr.main()
            except SystemExit as e:
                code = e.code or 0
        return client, buf.getvalue(), err.getvalue(), code

    def test_edit_inline_text(self):
        cl, out, err, code = self.run_cmd(
            ["edit", "acct:#ops", "100.1", "fixed", "wording"])
        self.assertEqual(code, 0, err)
        self.assertEqual(cl.calls, [("chat.update",
                                     {"channel": "C0CHAN", "ts": "100.1",
                                      "text": "fixed wording"})])
        self.assertEqual(out.strip(), "edited acct#C0CHAN ts=100.1")

    def test_edit_dash_reads_stdin(self):
        cl, out, err, code = self.run_cmd(
            ["edit", "acct:#ops", "100.1", "-"], stdin="multi\nline\n")
        self.assertEqual(code, 0, err)
        self.assertEqual(cl.calls[0][1]["text"], "multi\nline")

    def test_edit_empty_refused(self):
        cl, _, err, code = self.run_cmd(["edit", "acct:#ops", "100.1"],
                                        stdin="")
        self.assertNotEqual(code, 0)
        self.assertIn("empty message", err)
        self.assertEqual(cl.calls, [])

    def test_delete(self):
        cl, out, err, code = self.run_cmd(["delete", "acct:#ops", "100.1"])
        self.assertEqual(code, 0, err)
        self.assertEqual(cl.calls, [("chat.delete",
                                     {"channel": "C0CHAN", "ts": "100.1"})])
        self.assertEqual(out.strip(), "deleted acct#C0CHAN ts=100.1")

    def test_unarmed_account_refused(self):
        cl, _, err, code = self.run_cmd(["edit", "acct:#ops", "1.0", "x"],
                                        arm=False)
        self.assertNotEqual(code, 0)
        self.assertIn("not armed", err)
        self.assertEqual(cl.calls, [])
        cl, _, err, code = self.run_cmd(["delete", "acct:#ops", "1.0"],
                                        arm=False)
        self.assertNotEqual(code, 0)
        self.assertEqual(cl.calls, [])

    def test_non_slack_account_dies_clearly(self):
        import types
        dummy = types.SimpleNamespace()
        cfg = {"accounts": {"tg": {"platform": "telegram", "api_id": 1,
                                   "api_hash": "x", "allow_post": True}}}
        for argv in (["edit", "tg:@chan", "5", "x"],
                     ["delete", "tg:@chan", "5"]):
            _, _, err, code = self.run_cmd(argv, cfg=cfg, client=dummy)
            self.assertNotEqual(code, 0)
            self.assertIn("Slack-only", err)


class ParseSinceTest(unittest.TestCase):
    def test_relative_today_and_iso(self):
        now = time.time()
        self.assertAlmostEqual(msgr.parse_since("1d"), now - 86400, delta=5)
        self.assertAlmostEqual(msgr.parse_since("2h"), now - 7200, delta=5)
        self.assertIsNone(msgr.parse_since(None))
        # today = midnight UTC, within the last 24h
        t = msgr.parse_since("today")
        self.assertTrue(now - 86400 <= t <= now)
        # ISO date
        self.assertEqual(msgr.parse_since("2026-01-02"),
                         __import__("datetime").datetime(
                             2026, 1, 2,
                             tzinfo=__import__("datetime").timezone.utc)
                         .timestamp())


class ContextCliTest(SpoolBase):
    CFG = {"accounts": {"acct": {"platform": "slack", "bot_token": "x"}}}

    def run_ctx(self, argv, cfg=None):
        cfg = cfg or self.CFG
        with mock.patch.object(msgr, "load_config", return_value=cfg), \
             mock.patch.object(msgr, "platform_client",
                               side_effect=lambda en, env:
                               FakeSlackRead(en, env)), \
             mock.patch.object(sys, "argv", ["msgr", "context"] + argv):
            buf, err = io.StringIO(), io.StringIO()
            code = 0
            try:
                with contextlib.redirect_stdout(buf), \
                        contextlib.redirect_stderr(err):
                    msgr.main()
            except SystemExit as e:
                code = e.code or 0
            return buf.getvalue(), err.getvalue(), code

    def _seed(self):
        # a small journal: a thread in #ops, an owner message, an attachment
        self.append("Cops", "deploy?", ts="100.1")
        self.append("Cops", "done", ts="100.2", thread="100.1")
        self.append("Cops", "ship it", ts="100.3", user="U0OWNER",
                    owner=True, **{"from": "chief"})
        self.append("Calerts", "disk full", ts="200.1", files=["report.pdf"])

    def test_grouped_by_channel_threads_nested_owner_attachment(self):
        self._seed()
        msgr.name_cache_set("acct", "ops", "Cops")   # resolvable label
        out, _, _ = self.run_ctx(["acct:*"])
        self.assertIn("## #ops", out)                 # readable label from cache
        self.assertIn("(Cops)", out)
        self.assertIn("## Calerts", out)              # id label (no cache entry)
        self.assertIn("↳", out)                       # reply nested under root
        # reply comes after its root and is indented under it
        self.assertLess(out.index("deploy?"), out.index("done"))
        done_line = [ln for ln in out.splitlines() if "done" in ln][0]
        self.assertTrue(done_line.startswith("  ↳ "))  # nested reply marker
        self.assertIn("chief (owner):", out)          # owner marked
        self.assertIn("[attachment: report.pdf]", out)  # referenced, not inlined
        self.assertNotIn('"files"', out)              # not raw JSON

    def test_since_filters(self):
        self.append("Cops", "stale", ts=f"{time.time() - 2 * 86400:.4f}")
        self.append("Cops", "recent", ts=f"{time.time():.4f}")
        out, _, _ = self.run_ctx(["acct:*", "--since", "1d"])
        self.assertIn("recent", out)
        self.assertNotIn("stale", out)

    def test_channel_filter(self):
        self._seed()
        out, _, _ = self.run_ctx(["acct:#ops"])       # -> target_id "Cops"
        self.assertIn("deploy?", out)
        self.assertNotIn("disk full", out)            # #alerts excluded

    def test_thread_filter(self):
        self._seed()
        out, _, _ = self.run_ctx(["acct:*", "--thread", "100.1"])
        self.assertIn("deploy?", out)                 # root
        self.assertIn("done", out)                    # its reply
        self.assertNotIn("ship it", out)              # other message excluded
        self.assertNotIn("disk full", out)

    def test_json_emits_raw_filtered_events(self):
        self._seed()
        out, _, _ = self.run_ctx(["acct:#ops", "--json"])
        recs = [json.loads(ln) for ln in out.splitlines() if ln.strip()]
        self.assertEqual([r["text"] for r in recs],
                         ["deploy?", "done", "ship it"])
        self.assertTrue(all("_seq" not in r for r in recs))  # _seq stripped

    def test_no_local_record_note(self):
        out, err, code = self.run_ctx(["acct:*"])     # no spool written
        self.assertEqual(out, "")
        self.assertIn("no local record", err)
        self.assertEqual(code, 0)


class FollowSubprocessTest(SpoolBase):
    """Drive `read --follow` as a real subprocess (posing as the ingester by
    holding the sole-listener flock), appending a live event mid-stream."""

    def _run_follow(self, consumer, flags, late, pre_sleep=1.3, timeout=3):
        cfg = self.tmp / "config.toml"
        cfg.write_text('[accounts.acct]\nplatform="slack"\n'
                       'bot_token="xoxb-fake"\n')
        lock = open("/tmp/.msgr-listen-acct.lock", "a+")
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            env = dict(os.environ, MSGR_STATE_DIR=str(self.tmp),
                       MSGR_CONFIG=str(cfg))
            proc = subprocess.Popen(
                [sys.executable, msgr.__file__, "read", "acct:*",
                 "--as", consumer, "--follow", *flags,
                 "--timeout", str(timeout)],
                env=env, stdout=subprocess.PIPE, text=True)
            time.sleep(pre_sleep)
            for t in late:
                self.append("C", t)                   # arrives mid-stream
            out, _ = proc.communicate(timeout=20)
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)
            lock.close()
            try:
                os.unlink("/tmp/.msgr-listen-acct.lock")
            except OSError:
                pass
        texts = [json.loads(ln)["text"]
                 for ln in out.splitlines() if ln.strip()]
        return texts, proc.returncode

    def test_fresh_cursor_starts_from_now_no_backlog_replay(self):
        # a week of retained backlog must NOT be replayed into a new follower
        self.append("C", "old1")
        self.append("C", "old2")
        texts, rc = self._run_follow("fresh", [], ["live"])
        self.assertEqual(texts, ["live"])             # only post-start events
        self.assertEqual(rc, 0)

    def test_existing_cursor_backfills_gap_then_tails(self):
        self.append("C", "g1")                        # seq 1
        self.append("C", "g2")                        # seq 2
        msgr.spool_cursor_set("acct", "resume", 1)    # consumed through g1
        texts, _ = self._run_follow("resume", [], ["g3"])
        self.assertEqual(texts, ["g2", "g3"])         # gap backfilled, then live

    def test_from_start_replays_backlog_then_tails(self):
        self.append("C", "b1")
        self.append("C", "b2")
        texts, _ = self._run_follow("scratch", ["--from-start"], ["b3"])
        self.assertEqual(texts, ["b1", "b2", "b3"])   # backlog + live
        # cursor advanced through every event: a fresh drain sees nothing
        msgs, _, _ = msgr.spool_drain("acct", "scratch", None)
        self.assertEqual(msgs, [])


if __name__ == "__main__":
    unittest.main()
