"""Misskey platform adapter for Hermes Agent.

Env vars (read via Hermes scoped secrets / the Hermes .env file):
  MISSKEY_TOKEN            — required access token (never logged)
  MISSKEY_INSTANCE_URL     — required instance base URL (e.g. https://example.social)
  MISSKEY_ALLOWED_USERS    — required comma-separated acct forms (e.g. alice@example.social);
                             if empty, nobody is accepted (fail closed)
  MISSKEY_ALLOW_ALL_USERS  — truthy to disable allowlist (dev only)
  MISSKEY_LISTEN_MENTIONS  — truthy to also handle public/home @mentions (default: off)
  MISSKEY_HOME_CHANNEL     — chat_id for cron delivery (chat:<userId> or user:<userId>)
  MISSKEY_HOME_CHANNEL_NAME
  MISSKEY_MAX_NOTE_LENGTH  — override note char limit (else /api/meta maxNoteTextLength or 3000)

Token permissions needed: read:account, write:notes, read:notifications, read:chat, write:chat.
"""

from __future__ import annotations

import asyncio
import hashlib
import contextlib
import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlencode

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    aiohttp = None  # type: ignore[assignment]
    AIOHTTP_AVAILABLE = False

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType, ProcessingOutcome
from gateway.platforms.helpers import MessageDeduplicator, cancel_task
from gateway.platforms._shared import (
    extra_or_secret as _extra_or_secret,
    get_scoped_secret as _get_scoped_secret,
    seed_extra_from_env as _seed_extra_from_env,
    send_error,
)

logger = logging.getLogger(__name__)

DEFAULT_NOTE_LIMIT = 3000
DEFAULT_CHAT_LIMIT = 2000
RECONNECT_BACKOFF = [1, 2, 5, 10, 30, 60]
DEDUP_WINDOW_SECONDS = 300
DEDUP_MAX_SIZE = 2000
_TRUTHY = {"1", "true", "yes", "on"}
_MAIN_CHANNEL_ID = "main"


def _truthy(raw: Any) -> bool:
    return str(raw or "").strip().lower() in _TRUTHY


def _instance_host(url: str) -> str:
    parsed = urlparse(url if "://" in url else f"https://{url}")
    return (parsed.hostname or "").lower()


def _normalize_base_url(url: Optional[str]) -> str:
    """Normalize an instance URL; returns "" when unset (no default instance)."""
    url = str(url or "").strip().rstrip("/")
    if not url:
        return ""
    if "://" not in url:
        url = "https://" + url
    return url.rstrip("/")


def _acct_of(user: Optional[Dict[str, Any]], instance_host: str) -> str:
    """Build username@host for local (host=null) and remote users."""
    if not user:
        return ""
    username = str(user.get("username") or "").strip()
    if not username:
        return ""
    host = user.get("host")
    if host:
        return f"{username}@{str(host).lower()}"
    return f"{username}@{instance_host}" if instance_host else username


def _parse_list(raw: Any) -> List[str]:
    if not raw:
        return []
    if isinstance(raw, str):
        return [p.strip() for p in raw.split(",") if p.strip()]
    return [str(p).strip() for p in raw if str(p).strip()]


def _split_text(text: str, limit: int) -> List[str]:
    """Split *text* into chunks of at most *limit* characters, preferring paragraph/space breaks."""
    text = text or ""
    if limit <= 0 or len(text) <= limit:
        return [text] if text else []
    chunks: List[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        window = remaining[:limit]
        split_at = window.rfind("\n\n")
        if split_at < limit // 3:
            split_at = window.rfind("\n")
        if split_at < limit // 3:
            split_at = window.rfind(" ")
        if split_at < limit // 3:
            split_at = limit
        chunk = remaining[:split_at].rstrip()
        remaining = remaining[split_at:].lstrip()
        if chunk:
            chunks.append(chunk)
        elif remaining:
            # hard-split pathological no-whitespace case
            chunks.append(remaining[:limit])
            remaining = remaining[limit:]
    return chunks


def check_requirements() -> bool:
    """Dependencies available (token presence is checked at connect / is_connected)."""
    return AIOHTTP_AVAILABLE


def validate_config(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    token = _extra_or_secret(extra, "token", "MISSKEY_TOKEN")
    instance = _extra_or_secret(extra, "instance_url", "MISSKEY_INSTANCE_URL")
    return bool(str(token or "").strip()) and bool(_normalize_base_url(instance))


def is_connected(config) -> bool:
    """Configured when MISSKEY_TOKEN and MISSKEY_INSTANCE_URL are both set."""
    extra = getattr(config, "extra", {}) or {}
    token = _get_scoped_secret("MISSKEY_TOKEN", "").strip() or extra.get("token")
    instance = _get_scoped_secret("MISSKEY_INSTANCE_URL", "").strip() or extra.get("instance_url")
    return bool(token) and bool(_normalize_base_url(instance))


class MisskeyAdapter(BasePlatformAdapter):
    """Misskey adapter: REST out, streaming WebSocket in (main channel)."""

    MAX_MESSAGE_LENGTH = DEFAULT_NOTE_LIMIT

    def __init__(self, config: PlatformConfig):
        # User plugins are not in the bundled Platform scan; Platform("misskey") works
        # after register() has added the registry entry. For early/test construction,
        # fall back to a pseudo member.
        try:
            platform = Platform("misskey")
        except ValueError:
            platform = Platform._add_pseudo_member("misskey")
        super().__init__(config=config, platform=platform)
        extra = config.extra or {}
        self._instance_url = _normalize_base_url(
            _extra_or_secret(extra, "instance_url", "MISSKEY_INSTANCE_URL")
        )
        self._token = str(_extra_or_secret(extra, "token", "MISSKEY_TOKEN") or "").strip()
        self._listen_mentions = _truthy(
            extra.get("listen_mentions", _get_scoped_secret("MISSKEY_LISTEN_MENTIONS", ""))
        )
        raw_max = extra.get("max_note_length") or _get_scoped_secret("MISSKEY_MAX_NOTE_LENGTH", "")
        try:
            self._note_limit = int(raw_max) if raw_max else DEFAULT_NOTE_LIMIT
        except (TypeError, ValueError):
            self._note_limit = DEFAULT_NOTE_LIMIT
        self.MAX_MESSAGE_LENGTH = self._note_limit
        self._chat_limit = DEFAULT_CHAT_LIMIT
        self._instance_host = _instance_host(self._instance_url)
        # Allowlist is REQUIRED (Hermes also reads MISSKEY_ALLOWED_USERS via allowed_users_env).
        # There is no default: an empty allowlist means nobody is accepted (fail closed),
        # unless MISSKEY_ALLOW_ALL_USERS is explicitly truthy.
        allowed = _parse_list(extra.get("allowed_users"))
        if not allowed:
            allowed = _parse_list(_get_scoped_secret("MISSKEY_ALLOWED_USERS", ""))
        self._allow_all = _truthy(
            extra.get("allow_all_users", _get_scoped_secret("MISSKEY_ALLOW_ALL_USERS", ""))
        )
        self.config.extra = dict(extra)
        self.config.extra["allowed_users"] = list(allowed)
        self._allowed_users: set[str] = set()
        for entry in allowed:
            self._allowed_users.add(entry.strip().lower())
            norm = self.normalize_user_id(entry)
            if norm:
                self._allowed_users.add(norm.lower())

        self._session: Optional["aiohttp.ClientSession"] = None
        self._ws: Optional[Any] = None  # aiohttp WebSocket response
        self._ws_task: Optional[asyncio.Task] = None
        self._me: Optional[Dict[str, Any]] = None
        self._me_id: Optional[str] = None
        self._me_acct: Optional[str] = None
        self._chat_available = True  # updated from meta policies
        self._dedup = MessageDeduplicator(max_size=DEDUP_MAX_SIZE, ttl_seconds=DEDUP_WINDOW_SECONDS)
        # chat_id -> reply context for notes/create
        self._reply_ctx: Dict[str, Dict[str, Any]] = {}
        self._resolved_allow_ids: set[str] = set()
        # message_id -> "chat" | "note" (to pick the right reaction endpoint)
        self._msg_kind: Dict[str, str] = {}
        self._reactions_enabled = _truthy(
            extra.get("reactions", _get_scoped_secret("MISSKEY_REACTIONS", "true"))
        )
        self._note_reactions_ok = True  # flips off if the token lacks write:reactions

    @property
    def name(self) -> str:
        return "Misskey"

    def normalize_user_id(self, value: Optional[str]) -> Optional[str]:
        """Normalize allowlist / inbound ids to lowercase acct form when possible."""
        if value is None:
            return None
        v = str(value).strip()
        if not v:
            return None
        if v.startswith(("user:", "chat:")):
            v = v.split(":", 1)[1]
        if "@" in v:
            user, _, host = v.partition("@")
            return f"{user.lower()}@{(host or self._instance_host).lower()}"
        # bare local username
        if re.fullmatch(r"[A-Za-z0-9_.-]+", v) and not re.fullmatch(r"[a-zA-Z0-9]{8,}", v):
            # Heuristic: misskey ids are alphanumeric snowflakes; usernames can overlap.
            # Prefer treating short/name-like tokens as local usernames.
            if self._instance_host and not v.isdigit():
                return f"{v.lower()}@{self._instance_host}"
        return v

    def _is_allowed(self, acct: str, user_id: str) -> bool:
        """Adapter-level allowlist gate. Empty allowlist => nobody (fail closed)."""
        if self._allow_all:
            return True
        if not self._allowed_users:
            return False
        candidates = {str(acct or "").lower(), str(user_id or "").lower()}
        norm = self.normalize_user_id(acct) if acct else None
        if norm:
            candidates.add(norm.lower())
        candidates.discard("")
        return bool(candidates & self._allowed_users)

    def resolved_allowlist_user_ids(self) -> set[str]:
        return set(self._resolved_allow_ids)

    # ── Connection lifecycle ──────────────────────────────────────────────

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not AIOHTTP_AVAILABLE:
            logger.warning("[%s] aiohttp not installed in Hermes venv", self.name)
            return self._fail("deps_missing", "aiohttp is required for the Misskey adapter", retryable=False)
        if not self._token:
            logger.warning("[%s] not configured — set MISSKEY_TOKEN in the Hermes .env", self.name)
            return self._fail("not_configured", "MISSKEY_TOKEN not set", retryable=False)
        if not self._instance_url:
            logger.warning(
                "[%s] not configured — MISSKEY_INSTANCE_URL is required (e.g. https://example.social)",
                self.name,
            )
            return self._fail("not_configured", "MISSKEY_INSTANCE_URL not set", retryable=False)
        if not self._allowed_users and not self._allow_all:
            logger.warning(
                "[%s] MISSKEY_ALLOWED_USERS is empty — the adapter will accept NO users (fail closed). "
                "Set MISSKEY_ALLOWED_USERS=alice@example.social to allow someone.",
                self.name,
            )

        # Hash the token so lock files never store credential material.
        token_fp = hashlib.sha256(self._token.encode()).hexdigest()[:12]
        lock_key = f"{self._instance_host}:{token_fp}"
        if not self._acquire_platform_lock("misskey", lock_key, f"Misskey {self._instance_host}"):
            return False

        try:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=60, sock_connect=20, sock_read=120)
            )
            me = await self._api("i", {})
            if not isinstance(me, dict) or not me.get("id"):
                await self._cleanup_session()
                return self._fail("auth_failed", "MISSKEY_TOKEN rejected by /api/i", retryable=False)
            self._me = me
            self._me_id = str(me["id"])
            self._me_acct = _acct_of(me, self._instance_host)
            self._resolved_allow_ids = {self._me_id}
            if self._me_acct:
                self._resolved_allow_ids.add(self._me_acct)

            with contextlib.suppress(Exception):
                meta = await self._api("meta", {"detail": True})
                if isinstance(meta, dict):
                    limit = meta.get("maxNoteTextLength")
                    if isinstance(limit, int) and limit > 0 and not _get_scoped_secret("MISSKEY_MAX_NOTE_LENGTH", ""):
                        self._note_limit = limit
                        self.MAX_MESSAGE_LENGTH = limit
                    policies = meta.get("policies") or {}
                    chat_avail = str(policies.get("chatAvailability") or "").lower()
                    self._chat_available = chat_avail in {"", "available", "everyone"} or policies.get("chatAvailability") is True
                    if chat_avail in {"unavailable", "disabled", "false"}:
                        self._chat_available = False

            self._ws_task = asyncio.create_task(self._run_ws())
            self._mark_connected()
            logger.info(
                "[%s] Connected to %s as %s (chat=%s, listen_mentions=%s)",
                self.name, self._instance_url, self._me_acct or self._me_id,
                self._chat_available, self._listen_mentions,
            )
            self._wire_plugin_handlers(None)
            return True
        except Exception as e:
            logger.error("[%s] connect failed: %s", self.name, e)
            await self._cleanup_session()
            with contextlib.suppress(Exception):
                self._release_platform_lock()
            return self._fail("connect_failed", str(e), retryable=True)

    def _fail(self, code: str, message: str, *, retryable: bool) -> bool:
        self._set_fatal_error(code, message, retryable=retryable)
        return False

    async def disconnect(self) -> None:
        with contextlib.suppress(Exception):
            self._release_platform_lock()
        self._mark_disconnected()
        self._running = False
        await cancel_task(self._ws_task)
        self._ws_task = None
        if self._ws is not None and not self._ws.closed:
            with contextlib.suppress(Exception):
                await self._ws.close()
        self._ws = None
        await self._cleanup_session()
        self._dedup.clear()
        logger.info("[%s] Disconnected", self.name)

    async def _cleanup_session(self) -> None:
        if self._session and not self._session.closed:
            with contextlib.suppress(Exception):
                await self._session.close()
        self._session = None

    # ── WebSocket streaming ───────────────────────────────────────────────

    def _streaming_url(self) -> str:
        parsed = urlparse(self._instance_url)
        scheme = "wss" if parsed.scheme == "https" else "ws"
        # Token only in query string for the WS handshake; never log this URL.
        return f"{scheme}://{parsed.netloc}/streaming?{urlencode({'i': self._token})}"

    async def _run_ws(self) -> None:
        backoff_idx = 0
        while self._running:
            started = time.monotonic()
            try:
                await self._ws_session()
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._running:
                    return
                logger.warning("[%s] WebSocket error: %s", self.name, e)
            if not self._running:
                return
            if time.monotonic() - started >= 60.0:
                backoff_idx = 0
            delay = RECONNECT_BACKOFF[min(backoff_idx, len(RECONNECT_BACKOFF) - 1)]
            logger.info("[%s] Reconnecting streaming in %ds...", self.name, delay)
            await asyncio.sleep(delay)
            backoff_idx += 1

    async def _ws_session(self) -> None:
        assert self._session is not None
        url = self._streaming_url()
        async with self._session.ws_connect(url, heartbeat=20, autoping=True, ssl=True) as ws:
            self._ws = ws
            await ws.send_json({
                "type": "connect",
                "body": {"channel": "main", "id": _MAIN_CHANNEL_ID},
            })
            logger.debug("[%s] Subscribed to main streaming channel", self.name)
            async for msg in ws:
                if not self._running:
                    return
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await self._on_ws_text(msg.data)
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                    return

    async def _on_ws_text(self, raw: str) -> None:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return
        if payload.get("type") != "channel":
            return
        body = payload.get("body") or {}
        event = body.get("type")
        data = body.get("body")
        if not event:
            return
        if event in {"mention", "reply"}:
            if isinstance(data, dict):
                await self._handle_note_event(data, event=event)
        elif event == "notification":
            if isinstance(data, dict):
                await self._handle_notification(data)
        elif event in {"newChatMessage", "messagingMessage"}:
            if isinstance(data, dict):
                await self._handle_chat_message(data)

    # ── Inbound handlers ──────────────────────────────────────────────────

    async def _handle_notification(self, note_or_notif: Dict[str, Any]) -> None:
        ntype = note_or_notif.get("type")
        note = note_or_notif.get("note")
        if ntype in {"mention", "reply", "specified"} and isinstance(note, dict):
            await self._handle_note_event(note, event=str(ntype))

    async def _handle_note_event(self, note: Dict[str, Any], *, event: str) -> None:
        note_id = str(note.get("id") or "")
        if note_id and self._dedup.is_duplicate(f"note:{note_id}"):
            return
        user = note.get("user") or {}
        user_id = str(note.get("userId") or user.get("id") or "")
        if self._me_id and user_id == self._me_id:
            return  # ignore own notes (loop prevention)
        visibility = str(note.get("visibility") or "public")
        is_dm = visibility == "specified"
        if not is_dm and not self._listen_mentions:
            logger.debug("[%s] Ignoring %s note %s (listen_mentions=off)", self.name, visibility, note_id)
            return
        if not is_dm and visibility not in {"public", "home", "followers"}:
            return
        text = (note.get("text") or "").strip()
        if not text:
            return

        acct = _acct_of(user, self._instance_host) or user_id
        if not self._is_allowed(acct, user_id):
            logger.debug("[%s] Ignoring note %s from non-allowlisted user", self.name, note_id)
            return
        chat_id = f"user:{user_id}" if user_id else f"note:{note_id}"
        visible_ids = list(note.get("visibleUserIds") or [])
        # Ensure the original author stays visible on our reply.
        if user_id and user_id not in visible_ids:
            visible_ids.append(user_id)
        if self._me_id and self._me_id not in visible_ids and is_dm:
            visible_ids.append(self._me_id)
        if note_id:
            self._remember_kind(note_id, "note")
        self._reply_ctx[chat_id] = {
            "kind": "note",
            "reply_id": note_id,
            # Spec: reply with the same visibility and visibleUserIds.
            "visibility": visibility,
            "visible_user_ids": visible_ids,
            "user_id": user_id,
        }

        source = self.build_source(
            chat_id=chat_id,
            chat_name=acct,
            chat_type="dm" if is_dm else "channel",
            user_id=acct or user_id,
            user_name=str(user.get("name") or user.get("username") or acct),
            user_id_alt=user_id or None,
            message_id=note_id or None,
        )
        event_obj = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=note_id or uuid.uuid4().hex,
            raw_message=note,
            timestamp=datetime.now(tz=timezone.utc),
            reply_to_message_id=str(note.get("replyId") or "") or None,
        )
        await self.handle_message(event_obj)

    async def _handle_chat_message(self, msg: Dict[str, Any]) -> None:
        if not self._chat_available:
            return
        msg_id = str(msg.get("id") or "")
        if msg_id and self._dedup.is_duplicate(f"chat:{msg_id}"):
            return
        # Room chats are out of scope (personal chat only).
        if msg.get("toRoomId") or msg.get("toRoom"):
            return
        from_user = msg.get("fromUser") or {}
        from_id = str(msg.get("fromUserId") or from_user.get("id") or "")
        if self._me_id and from_id == self._me_id:
            return
        text = (msg.get("text") or "").strip()
        if not text and not msg.get("fileId"):
            return
        if not text:
            text = "[attachment]"
        acct = _acct_of(from_user, self._instance_host) or from_id
        if not self._is_allowed(acct, from_id):
            logger.debug("[%s] Ignoring chat %s from non-allowlisted user", self.name, msg_id)
            return
        chat_id = f"chat:{from_id}"
        self._reply_ctx[chat_id] = {"kind": "chat", "user_id": from_id}
        if msg_id:
            self._remember_kind(msg_id, "chat")
        source = self.build_source(
            chat_id=chat_id,
            chat_name=acct,
            chat_type="dm",
            user_id=acct or from_id,
            user_name=str(from_user.get("name") or from_user.get("username") or acct),
            user_id_alt=from_id or None,
            message_id=msg_id or None,
        )
        event_obj = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=msg_id or uuid.uuid4().hex,
            raw_message=msg,
            timestamp=datetime.now(tz=timezone.utc),
        )
        await self.handle_message(event_obj)

    # ── Outbound ──────────────────────────────────────────────────────────

    async def send(
        self, chat_id: str, content: str, reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        if not self._session:
            return SendResult(success=False, error="Not connected")
        ctx = dict(self._reply_ctx.get(chat_id) or {})
        meta = metadata or {}
        kind = meta.get("kind") or ctx.get("kind")
        if not kind:
            if chat_id.startswith("chat:"):
                kind = "chat"
            elif chat_id.startswith("user:") or chat_id.startswith("note:"):
                kind = "note"
            else:
                kind = "note"
        try:
            if kind == "chat":
                return await self._send_chat(chat_id, content, ctx, meta)
            return await self._send_note(chat_id, content, reply_to, ctx, meta)
        except Exception as e:
            logger.error("[%s] send error: %s", self.name, e)
            return SendResult(success=False, error=str(e))

    async def _send_chat(
        self, chat_id: str, content: str, ctx: Dict[str, Any], meta: Dict[str, Any],
    ) -> SendResult:
        to_user = str(meta.get("user_id") or ctx.get("user_id") or chat_id.split(":", 1)[-1])
        if not to_user:
            return SendResult(success=False, error="Missing chat recipient user id")
        last_id = None
        for chunk in _split_text(content, self._chat_limit):
            created = await self._api("chat/messages/create-to-user", {"toUserId": to_user, "text": chunk})
            if isinstance(created, dict):
                last_id = created.get("id") or last_id
            await asyncio.sleep(0.2)
        return SendResult(success=True, message_id=str(last_id or uuid.uuid4().hex[:12]))

    async def _send_note(
        self, chat_id: str, content: str, reply_to: Optional[str],
        ctx: Dict[str, Any], meta: Dict[str, Any],
    ) -> SendResult:
        visibility = str(meta.get("visibility") or ctx.get("visibility") or "specified")
        visible_ids = list(meta.get("visible_user_ids") or ctx.get("visible_user_ids") or [])
        user_id = str(meta.get("user_id") or ctx.get("user_id") or "")
        if chat_id.startswith("user:") and not user_id:
            user_id = chat_id.split(":", 1)[-1]
        if user_id and user_id not in visible_ids:
            visible_ids.append(user_id)
        reply_id = reply_to or ctx.get("reply_id")
        last_id = None
        for i, chunk in enumerate(_split_text(content, self._note_limit)):
            body: Dict[str, Any] = {
                "text": chunk,
                "visibility": visibility,
            }
            if visibility == "specified":
                body["visibleUserIds"] = visible_ids
            # Only the first chunk is a threaded reply; subsequent chunks stay in-visibility.
            if i == 0 and reply_id:
                body["replyId"] = reply_id
            created = await self._api("notes/create", body)
            created_note = (created or {}).get("createdNote") if isinstance(created, dict) else None
            if isinstance(created_note, dict) and created_note.get("id"):
                last_id = created_note["id"]
                # Chain subsequent chunks as replies to the previous chunk.
                reply_id = last_id
                if visibility == "specified":
                    # keep visible set
                    pass
            await asyncio.sleep(0.25)
        return SendResult(success=True, message_id=str(last_id or uuid.uuid4().hex[:12]))

    # ── Processing reactions (👀 while working, ✅/❌ when done) ───────────

    def _remember_kind(self, message_id: str, kind: str) -> None:
        self._msg_kind[message_id] = kind
        if len(self._msg_kind) > 500:
            for k in list(self._msg_kind)[:250]:
                self._msg_kind.pop(k, None)

    async def _react(self, message_id: str, emoji: str, *, add: bool) -> None:
        kind = self._msg_kind.get(message_id)
        try:
            if kind == "chat":
                await self._api(
                    "chat/messages/react" if add else "chat/messages/unreact",
                    {"messageId": message_id, "reaction": emoji},
                )
            elif kind == "note" and self._note_reactions_ok:
                if add:
                    await self._api("notes/reactions/create", {"noteId": message_id, "reaction": emoji})
                else:
                    await self._api("notes/reactions/delete", {"noteId": message_id})
        except Exception as exc:  # reactions are best-effort
            msg = str(exc)
            if kind == "note" and ("HTTP 403" in msg or "PERMISSION" in msg.upper()):
                self._note_reactions_ok = False
                logger.warning("[%s] Note reactions disabled (token needs write:reactions)", self.name)
            else:
                logger.debug("[%s] reaction failed: %s", self.name, msg[:200])

    async def on_processing_start(self, event: MessageEvent) -> None:
        if self._reactions_enabled and event.message_id:
            await self._react(str(event.message_id), "\U0001f440", add=True)

    async def on_processing_complete(self, event: MessageEvent, outcome: ProcessingOutcome) -> None:
        if not self._reactions_enabled or not event.message_id:
            return
        mid = str(event.message_id)
        await self._react(mid, "\U0001f440", add=False)
        if outcome == ProcessingOutcome.CANCELLED:
            return
        await self._react(mid, "\u2705" if outcome == ProcessingOutcome.SUCCESS else "\u274c", add=True)

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        """Misskey has no general typing indicator for notes; chat typing is unsupported here."""

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": chat_id, "type": "dm"}

    # ── REST helper ───────────────────────────────────────────────────────

    async def _api(self, endpoint: str, body: Optional[Dict[str, Any]] = None) -> Any:
        if not self._session:
            raise RuntimeError("HTTP session not initialized")
        payload = dict(body or {})
        payload["i"] = self._token
        url = f"{self._instance_url}/api/{endpoint.lstrip('/')}"
        async with self._session.post(url, json=payload) as resp:
            text = await resp.text()
            if resp.status >= 400:
                # Never include request body (has token) in logs.
                raise RuntimeError(f"Misskey API {endpoint} HTTP {resp.status}: {text[:300]}")
            if not text:
                return None
            return json.loads(text)


# ── Plugin registration helpers ─────────────────────────────────────────────


def _env_enablement() -> dict | None:
    """Seed PlatformConfig.extra from env before adapter construction.

    Returns None if MISSKEY_TOKEN or MISSKEY_INSTANCE_URL is missing.
    """
    token = _get_scoped_secret("MISSKEY_TOKEN", "").strip()
    if not token:
        return None
    if not _normalize_base_url(_get_scoped_secret("MISSKEY_INSTANCE_URL", "")):
        logger.warning("[Misskey] MISSKEY_INSTANCE_URL is required (e.g. https://example.social); not enabling")
        return None
    seed = _seed_extra_from_env((
        ("MISSKEY_INSTANCE_URL", "instance_url", lambda v: _normalize_base_url(v)),
        ("MISSKEY_LISTEN_MENTIONS", "listen_mentions", lambda v: v.lower() in _TRUTHY),
        ("MISSKEY_MAX_NOTE_LENGTH", "max_note_length", int),
        ("MISSKEY_ALLOWED_USERS", "allowed_users", lambda v: [p.strip() for p in v.split(",") if p.strip()]),
    ), home_env="MISSKEY_HOME_CHANNEL")
    seed["token"] = token
    seed.setdefault("allowed_users", [])
    return seed


async def _standalone_send(
    pconfig, chat_id: str, message: str, *,
    thread_id: Optional[str] = None, media_files: Optional[List[str]] = None, force_document: bool = False,
) -> Dict[str, Any]:
    """Out-of-process send for cron / send_message_tool."""
    if not AIOHTTP_AVAILABLE:
        return send_error("Misskey standalone send: aiohttp not installed")
    extra = getattr(pconfig, "extra", {}) or {}
    token = str(_extra_or_secret(extra, "token", "MISSKEY_TOKEN") or "").strip()
    if not token:
        return send_error("Misskey standalone send: MISSKEY_TOKEN not configured")
    instance = _normalize_base_url(
        _extra_or_secret(extra, "instance_url", "MISSKEY_INSTANCE_URL")
    )
    if not instance:
        return send_error("Misskey standalone send: MISSKEY_INSTANCE_URL not configured")
    note_limit = DEFAULT_NOTE_LIMIT
    with contextlib.suppress(Exception):
        note_limit = int(_get_scoped_secret("MISSKEY_MAX_NOTE_LENGTH", "") or note_limit)

    kind = "chat" if str(chat_id).startswith("chat:") else "note"
    target = str(chat_id).split(":", 1)[-1] if ":" in str(chat_id) else str(chat_id)

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        async def api(endpoint: str, body: dict) -> Any:
            body = {**body, "i": token}
            async with session.post(f"{instance}/api/{endpoint}", json=body) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    raise RuntimeError(f"HTTP {resp.status}: {text[:200]}")
                return json.loads(text) if text else None

        try:
            last_id = None
            if kind == "chat":
                for chunk in _split_text(message, DEFAULT_CHAT_LIMIT):
                    created = await api("chat/messages/create-to-user", {"toUserId": target, "text": chunk})
                    if isinstance(created, dict):
                        last_id = created.get("id") or last_id
            else:
                reply_id = thread_id
                for i, chunk in enumerate(_split_text(message, note_limit)):
                    body: Dict[str, Any] = {
                        "text": chunk,
                        "visibility": "specified",
                        "visibleUserIds": [target],
                    }
                    if i == 0 and reply_id:
                        body["replyId"] = reply_id
                    created = await api("notes/create", body)
                    note = (created or {}).get("createdNote") if isinstance(created, dict) else None
                    if isinstance(note, dict) and note.get("id"):
                        last_id = note["id"]
                        reply_id = last_id
            return {
                "success": True,
                "platform": "misskey",
                "chat_id": chat_id,
                "message_id": str(last_id or uuid.uuid4().hex[:12]),
            }
        except Exception as e:
            return send_error(f"Misskey standalone send failed: {e}")


def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="misskey",
        label="Misskey",
        adapter_factory=lambda cfg: MisskeyAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["MISSKEY_TOKEN", "MISSKEY_INSTANCE_URL"],
        install_hint="aiohttp (already in Hermes venv)",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="MISSKEY_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="MISSKEY_ALLOWED_USERS",
        allow_all_env="MISSKEY_ALLOW_ALL_USERS",
        max_message_length=DEFAULT_NOTE_LIMIT,
        emoji="🟣",
        pii_safe=True,
        allow_update_command=True,
        platform_hint=(
            "You are chatting via Misskey, a federated (ActivityPub) microblogging "
            "network. Posts are called notes. Users are written as acct handles: "
            "@username for local users, @username@host for remote users (e.g. "
            "@alice@example.social). Users can react to notes and chat messages "
            "with emoji, including custom emoji written as :name:. Misskey renders "
            "light Markdown plus MFM, so plain text or simple formatting works best; "
            "avoid heavy tables. Keep replies short: long replies are split to the "
            "instance's note/chat character limit. A 👀 reaction is added to the "
            "user's message while you work and ✅ when you finish; do not mention it. "
            "You are talking in a direct message or personal chat. Reply in the "
            "user's language."
        ),
    )
