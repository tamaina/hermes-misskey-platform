"""Mock-based unit tests for the Misskey Hermes platform adapter (stdlib unittest).

Run with Hermes's venv Python (so `gateway.*` and aiohttp are importable):
  <hermes-venv>/bin/python test_adapter.py
Set HERMES_AGENT_ROOT if the Hermes source tree is not at ~/.hermes/hermes-agent.
"""

from __future__ import annotations

import asyncio
import os
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock, patch

HERMES_ROOT = Path(os.environ.get("HERMES_AGENT_ROOT") or (Path.home() / ".hermes" / "hermes-agent"))
PLUGIN_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(HERMES_ROOT))

import importlib.util

_spec = importlib.util.spec_from_file_location("misskey_adapter", PLUGIN_DIR / "adapter.py")
adapter = importlib.util.module_from_spec(_spec)
assert _spec.loader is not None
sys.modules["misskey_adapter"] = adapter
_spec.loader.exec_module(adapter)


def _config(extra: Dict[str, Any] | None = None):
    return SimpleNamespace(extra=extra or {
        "token": "test-token-not-real",
        "instance_url": "https://example.social",
        "allowed_users": ["alice@example.social"],
        "listen_mentions": False,
    })


def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


class MisskeyAdapterTests(unittest.TestCase):
    def setUp(self):
        self._aio_patch = patch.object(adapter, "AIOHTTP_AVAILABLE", True)
        self._aio_patch.start()
        self.mk = adapter.MisskeyAdapter(_config())
        self.mk._session = MagicMock()
        self.mk._me_id = "botid001"
        self.mk._me_acct = "bot@example.social"
        self.mk._instance_host = "example.social"
        self.mk._chat_available = True
        self.mk._running = True
        self.mk.handle_message = AsyncMock()
        self.mk.build_source = MagicMock(
            side_effect=lambda **kw: SimpleNamespace(**kw, platform="misskey")
        )

    def tearDown(self):
        self._aio_patch.stop()

    def test_processing_reactions_chat(self):
        from gateway.platforms.event import ProcessingOutcome
        mk = self.mk
        mk._api = AsyncMock(return_value=None)
        _run(mk._handle_chat_message({
            "id": "c1", "fromUserId": "userid002",
            "fromUser": {"id": "userid002", "username": "alice", "host": None}, "text": "hi",
        }))
        ev = SimpleNamespace(message_id="c1")
        _run(mk.on_processing_start(ev))
        _run(mk.on_processing_complete(ev, ProcessingOutcome.SUCCESS))
        calls = [(c.args[0], c.args[1]["reaction"]) for c in mk._api.await_args_list]
        self.assertEqual(calls, [
            ("chat/messages/react", "\U0001f440"),
            ("chat/messages/unreact", "\U0001f440"),
            ("chat/messages/react", "\u2705"),
        ])

    def test_reactions_can_be_disabled(self):
        mk = self.mk
        mk._reactions_enabled = False
        mk._api = AsyncMock()
        _run(mk.on_processing_start(SimpleNamespace(message_id="x")))
        mk._api.assert_not_called()

    def test_split_text_respects_limit(self):
        parts = adapter._split_text("a" * 50 + "\n\n" + "b" * 50, 60)
        self.assertTrue(all(len(p) <= 60 for p in parts))

    def test_acct_of_local_and_remote(self):
        self.assertEqual(
            adapter._acct_of({"username": "alice", "host": None}, "example.social"),
            "alice@example.social",
        )
        self.assertEqual(
            adapter._acct_of({"username": "bob", "host": "misskey.io"}, "example.social"),
            "bob@misskey.io",
        )

    def test_not_configured_without_token(self):
        cfg = SimpleNamespace(extra={})
        with patch.object(adapter, "_get_scoped_secret", return_value=""):
            with patch.object(adapter, "_extra_or_secret", return_value=""):
                self.assertFalse(adapter.validate_config(cfg))
                self.assertFalse(adapter.is_connected(cfg))

    def test_ignore_own_note(self):
        note = {
            "id": "n1",
            "userId": "botid001",
            "user": {"id": "botid001", "username": "bot", "host": None},
            "visibility": "specified",
            "text": "hello",
            "visibleUserIds": ["botid001", "userid002"],
        }
        _run(self.mk._handle_note_event(note, event="mention"))
        self.mk.handle_message.assert_not_called()

    def test_ignore_public_mention_by_default(self):
        note = {
            "id": "n2",
            "userId": "userid002",
            "user": {"id": "userid002", "username": "alice", "host": None},
            "visibility": "public",
            "text": "@bot hi",
            "visibleUserIds": [],
        }
        _run(self.mk._handle_note_event(note, event="mention"))
        self.mk.handle_message.assert_not_called()

    def test_receive_specified_dm_and_reply(self):
        note = {
            "id": "n3",
            "userId": "userid002",
            "user": {"id": "userid002", "username": "alice", "host": None, "name": "Alice"},
            "visibility": "specified",
            "text": "ping the agent",
            "visibleUserIds": ["botid001", "userid002"],
        }
        _run(self.mk._handle_note_event(note, event="mention"))
        self.mk.handle_message.assert_awaited_once()
        ev = self.mk.handle_message.await_args.args[0]
        self.assertEqual(ev.text, "ping the agent")
        self.assertEqual(ev.source.user_id, "alice@example.social")
        self.assertEqual(ev.source.chat_id, "user:userid002")
        self.assertEqual(self.mk._reply_ctx["user:userid002"]["visibility"], "specified")

        calls: List[Dict[str, Any]] = []

        async def fake_api(endpoint, body=None):
            calls.append({"endpoint": endpoint, "body": dict(body or {})})
            return {"createdNote": {"id": "reply1"}}

        self.mk._api = fake_api
        result = _run(self.mk.send("user:userid002", "pong from agent"))
        self.assertTrue(result.success)
        self.assertEqual(calls[0]["endpoint"], "notes/create")
        body = calls[0]["body"]
        self.assertEqual(body["visibility"], "specified")
        self.assertEqual(body["replyId"], "n3")
        self.assertIn("userid002", body["visibleUserIds"])
        self.assertEqual(body["text"], "pong from agent")

    def test_receive_chat_and_reply(self):
        msg = {
            "id": "c1",
            "fromUserId": "userid002",
            "fromUser": {"id": "userid002", "username": "alice", "host": None},
            "toUserId": "botid001",
            "toRoomId": None,
            "text": "chat hi",
        }
        _run(self.mk._handle_chat_message(msg))
        self.mk.handle_message.assert_awaited_once()
        ev = self.mk.handle_message.await_args.args[0]
        self.assertEqual(ev.source.chat_id, "chat:userid002")

        calls = []

        async def fake_api(endpoint, body=None):
            calls.append({"endpoint": endpoint, "body": dict(body or {})})
            return {"id": "c2"}

        self.mk._api = fake_api
        result = _run(self.mk.send("chat:userid002", "chat reply"))
        self.assertTrue(result.success)
        self.assertEqual(calls[0]["endpoint"], "chat/messages/create-to-user")
        self.assertEqual(calls[0]["body"]["toUserId"], "userid002")
        self.assertEqual(calls[0]["body"]["text"], "chat reply")

    def test_long_note_is_split(self):
        self.mk._reply_ctx["user:userid002"] = {
            "kind": "note",
            "reply_id": "n9",
            "visibility": "specified",
            "visible_user_ids": ["userid002", "botid001"],
            "user_id": "userid002",
        }
        self.mk._note_limit = 100
        texts = []

        async def fake_api(endpoint, body=None):
            texts.append(body.get("text"))
            return {"createdNote": {"id": f"r{len(texts)}"}}

        self.mk._api = fake_api
        long = ("hello world " * 20).strip()
        result = _run(self.mk.send("user:userid002", long))
        self.assertTrue(result.success)
        self.assertGreaterEqual(len(texts), 2)
        self.assertTrue(all(len(t) <= 100 for t in texts))

    def test_listen_mentions_flag(self):
        self.mk._listen_mentions = True
        note = {
            "id": "n4",
            "userId": "userid002",
            "user": {"id": "userid002", "username": "alice", "host": None},
            "visibility": "home",
            "text": "@bot public-ish mention",
            "visibleUserIds": [],
        }
        _run(self.mk._handle_note_event(note, event="mention"))
        self.mk.handle_message.assert_awaited_once()

    def test_env_enablement_without_token(self):
        with patch.object(adapter, "_get_scoped_secret", return_value=""):
            self.assertIsNone(adapter._env_enablement())

    def test_env_enablement_with_token(self):
        def fake_secret(name, default=""):
            return {
                "MISSKEY_TOKEN": "tok",
                "MISSKEY_INSTANCE_URL": "https://example.social",
                "MISSKEY_ALLOWED_USERS": "alice@example.social",
            }.get(name, default)

        with patch.object(adapter, "_get_scoped_secret", side_effect=fake_secret):
            with patch.object(
                adapter,
                "_seed_extra_from_env",
                return_value={
                    "instance_url": "https://example.social",
                    "allowed_users": ["alice@example.social"],
                },
            ):
                seed = adapter._env_enablement()
                self.assertIsNotNone(seed)
                self.assertEqual(seed["token"], "tok")

    # ── Fail-closed allowlist / required config ──────────────────────────

    def _dm(self, user_id="userid002", username="alice", host=None):
        return {
            "id": f"n-{user_id}",
            "userId": user_id,
            "user": {"id": user_id, "username": username, "host": host},
            "visibility": "specified",
            "text": "hello",
            "visibleUserIds": ["botid001", user_id],
        }

    def _fresh(self, extra):
        with patch.object(adapter, "_get_scoped_secret", return_value=""):
            mk = adapter.MisskeyAdapter(_config(extra))
        mk._me_id = "botid001"
        mk._chat_available = True
        mk.handle_message = AsyncMock()
        mk.build_source = MagicMock(side_effect=lambda **kw: SimpleNamespace(**kw, platform="misskey"))
        return mk

    def test_empty_allowlist_accepts_nobody(self):
        mk = self._fresh({"token": "t", "instance_url": "https://example.social", "allowed_users": []})
        _run(mk._handle_note_event(self._dm(), event="mention"))
        _run(mk._handle_chat_message({
            "id": "c9", "fromUserId": "userid002",
            "fromUser": {"id": "userid002", "username": "alice", "host": None}, "text": "hi",
        }))
        mk.handle_message.assert_not_called()

    def test_non_allowlisted_user_rejected(self):
        _run(self.mk._handle_note_event(self._dm("userid003", "mallory"), event="mention"))
        self.mk.handle_message.assert_not_called()

    def test_remote_allowlisted_user_accepted(self):
        mk = self._fresh({"token": "t", "instance_url": "https://example.social",
                          "allowed_users": "bob@remote.example"})
        _run(mk._handle_note_event(self._dm("userid004", "Bob", "remote.example"), event="mention"))
        mk.handle_message.assert_awaited_once()

    def test_empty_allowlist_warns_on_connect(self):
        mk = self._fresh({"token": "t", "instance_url": "https://example.social", "allowed_users": []})
        mk._acquire_platform_lock = MagicMock(return_value=False)
        with self.assertLogs(adapter.logger, level="WARNING") as cm:
            _run(mk.connect())
        self.assertTrue(any("MISSKEY_ALLOWED_USERS is empty" in m for m in cm.output))

    def test_missing_instance_url_fails(self):
        mk = self._fresh({"token": "t", "allowed_users": ["alice@example.social"]})
        self.assertEqual(mk._instance_url, "")
        mk._fail = MagicMock(return_value=False)
        with self.assertLogs(adapter.logger, level="WARNING") as cm:
            self.assertFalse(_run(mk.connect()))
        self.assertTrue(any("MISSKEY_INSTANCE_URL is required" in m for m in cm.output))
        mk._fail.assert_called_once()
        self.assertEqual(mk._fail.call_args.args[0], "not_configured")
        with patch.object(adapter, "_get_scoped_secret", return_value=""):
            self.assertFalse(adapter.validate_config(SimpleNamespace(extra={"token": "t"})))
            self.assertFalse(adapter.is_connected(SimpleNamespace(extra={"token": "t"})))

    def test_env_enablement_requires_instance_url(self):
        def fake_secret(name, default=""):
            return {"MISSKEY_TOKEN": "tok"}.get(name, default)
        with patch.object(adapter, "_get_scoped_secret", side_effect=fake_secret):
            self.assertIsNone(adapter._env_enablement())

    def test_no_personal_defaults(self):
        self.assertFalse(hasattr(adapter, "DEFAULT_ALLOWED_USER"))
        self.assertFalse(hasattr(adapter, "DEFAULT_INSTANCE_URL"))
        self.assertEqual(adapter._normalize_base_url(""), "")


if __name__ == "__main__":
    # Python 3.14: asyncio.get_event_loop() may need an explicit loop policy.
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())
    unittest.main(verbosity=2)
