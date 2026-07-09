#!/usr/bin/env python3
"""Tests for msgr. Network-free: the Slack API is stubbed, so these exercise
message normalization and the thread-read path without a live workspace."""

import unittest

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


if __name__ == "__main__":
    unittest.main()
