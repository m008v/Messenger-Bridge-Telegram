from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
from typing import Any, Callable, Optional

from fbchat_v2 import dataGetHome, listeningE2EEEvent
from fbchat_v2._features._facebook._get_user_info import func as get_user_info

from config import AppConfig
from models import IncomingMessengerMessage, QuoteData, TopicEntry


MessengerEventCallback = Callable[[dict[str, Any]], None]
logger = logging.getLogger(__name__)


class MessengerClient:
    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.data_fb: Optional[dict] = None
        self.self_id = ""
        self.listener: Optional[listeningE2EEEvent] = None
        self._listener_task: Optional[asyncio.Task[None]] = None
        self._stopping = False
        self._last_error: Optional[BaseException] = None
        self._event_callback: Optional[MessengerEventCallback] = None
        self._user_name_cache: dict[str, str] = {}
        self._thread_name_cache: dict[str, str] = {}

    @property
    def last_error(self) -> Optional[BaseException]:
        return self._last_error

    @property
    def is_running(self) -> bool:
        return self._listener_task is not None and not self._listener_task.done()

    async def login(self) -> None:
        logger.info("Logging in to Messenger with fbchat-v2")
        data_fb = await dataGetHome(self.config.facebook_cookie)

        if not data_fb or not isinstance(data_fb, dict):
            raise RuntimeError("Could not retrieve Facebook session data. Check FACEBOOK_COOKIE or network connection.")

        facebook_id = str(data_fb.get("FacebookID") or "")
        if not facebook_id or "Unable to retrieve" in facebook_id:
            raise RuntimeError("Could not read FacebookID from cookie. Refresh FACEBOOK_COOKIE.")

        self.data_fb = data_fb
        self.self_id = facebook_id
        logger.info("Messenger login complete: facebook_id=%s", self.self_id)

    async def start(self, event_callback: MessengerEventCallback) -> None:
        if self.is_running:
            raise RuntimeError("Messenger listener is already running")
        if self.data_fb is None:
            await self.login()

        self._event_callback = event_callback
        self._last_error = None
        self._stopping = False
        self.listener = listeningE2EEEvent(
            self.data_fb,
            log_level=self.config.fbchat_e2ee_log_level,
            device_path=self.config.fbchat_e2ee_device_path,
            e2ee_memory_only=self.config.fbchat_e2ee_memory_only,
            enable_e2ee=self.config.fbchat_enable_e2ee,
            binary_path=self.config.fbchat_e2ee_bin,
        )
        self.listener.on_message(event_callback)
        logger.info(
            "Messenger listener configured: e2ee_enabled=%s memory_only=%s binary=%s",
            self.config.fbchat_enable_e2ee,
            self.config.fbchat_e2ee_memory_only,
            self.config.fbchat_e2ee_bin or "auto/default",
        )

        listener_task = asyncio.create_task(
            self.listener.connect_mqtt(),
            name="fbchat-v2-e2ee-listener",
        )
        self._listener_task = listener_task
        try:
            logger.info("Messenger listener connecting MQTT")
            ready = await asyncio.to_thread(
                self.listener.wait_until_connected,
                self.config.fbchat_e2ee_ready_timeout,
                require_e2ee=self.config.fbchat_enable_e2ee,
            )
            if not ready:
                raise TimeoutError(
                    "Messenger listener did not become ready within "
                    f"{self.config.fbchat_e2ee_ready_timeout:g}s"
                )
        except BaseException:
            await self.stop()
            raise

        listener_task.add_done_callback(self._on_listener_done)
        logger.info("Messenger listener is ready")

    def _on_listener_done(self, task: asyncio.Task[None]) -> None:
        if self._stopping or self.listener is None or task.cancelled():
            return
        error = task.exception()
        if error is None:
            error = RuntimeError("Messenger listener stopped unexpectedly")
        self._last_error = error
        logger.error(
            "Messenger listener stopped with error",
            exc_info=(type(error), error, error.__traceback__),
        )
        if self._event_callback:
            self._event_callback({
                "type": "error",
                "data": {"message": str(error), "source": "listener"},
            })

    async def stop(self) -> None:
        if self.listener is None and self._listener_task is None:
            return
        logger.info("Stopping Messenger listener")
        self._stopping = True
        try:
            await self._stop_listener_task()
        finally:
            self.listener = None
            self._listener_task = None
            self._event_callback = None
            self._stopping = False

    async def _stop_listener_task(self) -> None:
        listener = self.listener
        listener_task = self._listener_task
        if listener is not None:
            await asyncio.to_thread(listener.stop)
        if listener_task is None or listener_task is asyncio.current_task():
            return

        try:
            await asyncio.wait_for(asyncio.shield(listener_task), timeout=5.0)
        except asyncio.TimeoutError:
            listener_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await listener_task
        except asyncio.CancelledError:
            if not listener_task.cancelled():
                raise
        except Exception:  # Listener errors are reported by start/done callbacks.
            pass

    async def resolve_user_name(self, user_id: str) -> Optional[str]:
        clean_id = str(user_id or "").strip()
        if not clean_id or self.data_fb is None:
            return None
        if clean_id in self._user_name_cache:
            return self._user_name_cache[clean_id]

        try:
            info = await get_user_info(self.data_fb, clean_id)
        except Exception as exc:  # noqa: BLE001 - profile lookup is best effort
            logger.debug("Could not resolve Messenger user name for %s: %s", clean_id, exc)
            return None
        if not isinstance(info, dict) or info.get("error"):
            logger.debug("Could not resolve Messenger user name for %s", clean_id)
            return None

        name = str(info.get("nameUser") or info.get("firstName") or "").strip()
        if name:
            self._user_name_cache[clean_id] = name
            return name
        return None

    def resolve_thread_name(self, thread_id: str) -> Optional[str]:
        clean_id = str(thread_id or "").strip()
        if not clean_id or self.data_fb is None:
            return None
        if clean_id in self._thread_name_cache:
            return self._thread_name_cache[clean_id]

        try:
            fbt = getattr(self.listener, "fbt", None)
            if not isinstance(fbt, dict) or not fbt:
                return None
            all_threads = (fbt.get("dataAllThread") or {}) if isinstance(fbt, dict) else {}
            ids = [str(item) for item in (all_threads.get("threadIDList") or [])]
            names = [str(item).strip() for item in (all_threads.get("threadNameList") or [])]
            for index, candidate_id in enumerate(ids):
                if candidate_id == clean_id and index < len(names) and names[index]:
                    self._thread_name_cache[clean_id] = names[index]
                    return names[index]

            data_get = fbt.get("dataGet") if isinstance(fbt, dict) else None
            if data_get:
                snapshot_name = self._resolve_name_from_thread_snapshot(data_get, clean_id)
                if snapshot_name:
                    self._thread_name_cache[clean_id] = snapshot_name
                    return snapshot_name
        except Exception as exc:  # noqa: BLE001
            logger.debug("Could not resolve Messenger thread name for %s: %s", clean_id, exc)
        return None

    def _resolve_name_from_thread_snapshot(self, data_get: str, lookup_id: str) -> Optional[str]:
        try:
            nodes = json.loads(data_get)["o0"]["data"]["viewer"]["message_threads"]["nodes"]
        except (KeyError, TypeError, json.JSONDecodeError):
            return None

        for node in nodes:
            if not isinstance(node, dict):
                continue
            thread_key = node.get("thread_key") or {}
            key_values = {
                str(thread_key.get("thread_fbid") or ""),
                str(thread_key.get("other_user_id") or ""),
                str(thread_key.get("other_user_fbid") or ""),
                str(node.get("id") or ""),
            }
            if lookup_id not in key_values:
                continue

            thread_name = str(node.get("name") or "").strip()
            if thread_name:
                return thread_name

            participant_name = self._participant_name_from_thread_node(node)
            if participant_name:
                return participant_name
        return None

    def _participant_name_from_thread_node(self, node: dict) -> Optional[str]:
        edges = (((node.get("all_participants") or {}).get("edges")) or [])
        fallback_name = None
        for edge in edges:
            actor = ((edge or {}).get("node") or {}).get("messaging_actor") or {}
            actor_id = str(actor.get("id") or "").strip()
            actor_name = str(actor.get("name") or "").strip()
            if not actor_name:
                continue
            if not fallback_name:
                fallback_name = actor_name
            if actor_id and actor_id != self.self_id:
                return actor_name
        return fallback_name

    async def resolve_topic_display_name(self, message: IncomingMessengerMessage) -> Optional[str]:
        event_thread_name = str(message.thread_name or "").strip()
        if event_thread_name:
            self._thread_name_cache[message.thread_id] = event_thread_name
            return event_thread_name

        thread_name = self.resolve_thread_name(message.thread_id)
        if thread_name:
            return thread_name

        if self._is_regular_group_message(message):
            return self._group_fallback_name(message.thread_id or message.messenger_id)

        for candidate_id in self._candidate_profile_ids(message):
            user_name = await self.resolve_user_name(candidate_id)
            if user_name:
                return user_name
        return None

    @staticmethod
    def _is_regular_group_message(message: IncomingMessengerMessage) -> bool:
        if message.transport != "regular":
            return False
        thread_id = str(message.thread_id or message.messenger_id or "").strip()
        sender_id = str(message.sender_id or "").strip()
        if not thread_id:
            return False
        if sender_id and thread_id != sender_id:
            return True
        return message.thread_type not in {0, 1}

    @staticmethod
    def _group_fallback_name(thread_id: str) -> str:
        clean_id = str(thread_id or "").strip()
        return f"Messenger group {clean_id}" if clean_id else "Messenger group"

    def _candidate_profile_ids(self, message: IncomingMessengerMessage) -> list[str]:
        candidates = [
            message.sender_id,
            message.thread_id,
            self._jid_user(message.sender_jid),
            self._jid_user(message.chat_jid),
            self._jid_user(message.messenger_id),
            message.messenger_id,
        ]

        seen: set[str] = set()
        result: list[str] = []
        for candidate in candidates:
            clean = str(candidate or "").strip()
            if not clean or clean == self.self_id or clean in seen:
                continue
            if not clean.isdigit():
                continue
            seen.add(clean)
            result.append(clean)
        return result

    @staticmethod
    def _jid_user(value: Optional[str]) -> str:
        text = str(value or "").strip()
        if "@" not in text:
            return text
        return text.split("@", 1)[0]

    async def send_text(self, entry: TopicEntry, text: str, quote: Optional[QuoteData] = None) -> dict:
        if self.listener is None:
            raise RuntimeError("Messenger listener is not connected")

        content = text.strip()
        if not content:
            raise ValueError("Cannot send an empty Messenger message")

        if entry.transport == "e2ee":
            reply_id, reply_sender = self._e2ee_reply_parts(quote)
            logger.info(
                "Sending E2EE Messenger text: chat_jid=%s reply_id=%s text_len=%s",
                entry.chat_jid or entry.messenger_id,
                reply_id or "-",
                len(content),
            )
            data = await self.listener.send_e2ee_message(
                entry.chat_jid or entry.messenger_id,
                content,
                reply_to_id=reply_id,
                reply_to_sender_jid=reply_sender,
            )
            return self._normalize_send_result(data)

        reply_id = quote.message_id if quote and quote.transport == "regular" else ""
        logger.info(
            "Sending regular Messenger text: thread_id=%s reply_id=%s text_len=%s",
            entry.thread_id or entry.messenger_id,
            reply_id or "-",
            len(content),
        )
        data = await self.listener.send_message(
            int(entry.thread_id or entry.messenger_id),
            content,
            reply_to_id=reply_id,
        )
        return self._normalize_send_result(data)

    async def send_telegram_sticker(
        self,
        entry: TopicEntry,
        file_data: bytes,
        filename: str,
        mime_type: str,
        width: int = 0,
        height: int = 0,
        quote: Optional[QuoteData] = None,
    ) -> dict:
        if self.listener is None:
            raise RuntimeError("Messenger listener is not connected")
        if not file_data:
            raise ValueError("Telegram sticker file is empty")

        encoded = base64.b64encode(file_data).decode("ascii")
        clean_mime = (mime_type or "application/octet-stream").lower()
        clean_filename = filename or "telegram-sticker.bin"

        if entry.transport == "e2ee":
            reply_id, reply_sender = self._e2ee_reply_parts(quote)
            chat_jid = entry.chat_jid or entry.messenger_id
            logger.info(
                "Sending E2EE Messenger sticker/media: chat_jid=%s filename=%s mime=%s bytes=%s reply_id=%s",
                chat_jid,
                clean_filename,
                clean_mime,
                len(file_data),
                reply_id or "-",
            )
            if clean_mime == "image/webp" or clean_filename.lower().endswith(".webp"):
                return await self._call_bridge_send("sendE2EESticker", {
                    "chatJid": chat_jid,
                    "data": encoded,
                    "mimeType": clean_mime or "image/webp",
                    "width": width,
                    "height": height,
                    "replyToId": reply_id,
                    "replyToSenderJid": reply_sender,
                })
            if clean_mime.startswith("image/"):
                return await self._call_bridge_send("sendE2EEImage", {
                    "chatJid": chat_jid,
                    "data": encoded,
                    "mimeType": clean_mime,
                    "caption": "",
                    "width": width,
                    "height": height,
                    "replyToId": reply_id,
                    "replyToSenderJid": reply_sender,
                })
            if clean_mime.startswith("video/"):
                return await self._call_bridge_send("sendE2EEVideo", {
                    "chatJid": chat_jid,
                    "data": encoded,
                    "mimeType": clean_mime,
                    "caption": "",
                    "width": width,
                    "height": height,
                    "replyToId": reply_id,
                    "replyToSenderJid": reply_sender,
                })
            return await self._call_bridge_send("sendE2EEDocument", {
                "chatJid": chat_jid,
                "data": encoded,
                "filename": clean_filename,
                "mimeType": clean_mime,
                "replyToId": reply_id,
                "replyToSenderJid": reply_sender,
            })

        thread_id = int(entry.thread_id or entry.messenger_id)
        reply_id = quote.message_id if quote and quote.transport == "regular" else ""
        logger.info(
            "Sending regular Messenger sticker/media: thread_id=%s filename=%s mime=%s bytes=%s reply_id=%s",
            thread_id,
            clean_filename,
            clean_mime,
            len(file_data),
            reply_id or "-",
        )
        if clean_mime.startswith("image/"):
            return await self._call_bridge_send("sendImage", {
                "threadId": thread_id,
                "data": encoded,
                "filename": clean_filename,
                "caption": "",
                "replyToId": reply_id,
            })
        return await self._call_bridge_send("sendFile", {
            "threadId": thread_id,
            "data": encoded,
            "filename": clean_filename,
            "mimeType": clean_mime,
            "caption": "Telegram sticker",
            "replyToId": reply_id,
        })

    async def send_reaction(self, quote: QuoteData, emoji: str) -> dict:
        if self.listener is None:
            raise RuntimeError("Messenger listener is not connected")
        if not quote.message_id:
            raise ValueError("Cannot react without a Messenger message ID")

        clean_emoji = str(emoji or "")
        if quote.transport == "e2ee":
            message_id, sender_jid = self._e2ee_reply_parts(quote)
            if not message_id or not sender_jid:
                raise RuntimeError("Cannot send E2EE reaction without quote sender JID")
            chat_jid = quote.chat_jid or quote.messenger_id
            if not chat_jid:
                raise RuntimeError("Cannot send E2EE reaction without chat JID")
            logger.info(
                "Sending E2EE Messenger reaction: chat_jid=%s message_id=%s sender_jid=%s emoji=%s",
                chat_jid,
                message_id,
                sender_jid,
                clean_emoji or "<remove>",
            )
            return await self._call_bridge_send("sendE2EEReaction", {
                "chatJid": chat_jid,
                "messageId": message_id,
                "senderJid": sender_jid,
                "emoji": clean_emoji,
            })

        thread_id = int(quote.thread_id or quote.messenger_id)
        logger.info(
            "Sending regular Messenger reaction: thread_id=%s message_id=%s emoji=%s",
            thread_id,
            quote.message_id,
            clean_emoji or "<remove>",
        )
        return await self._call_bridge_send("sendReaction", {
            "threadId": thread_id,
            "messageId": quote.message_id,
            "emoji": clean_emoji,
        })

    async def _call_bridge_send(self, method: str, params: dict) -> dict:
        bridge = getattr(self.listener, "_bridge", None)
        if bridge is None:
            raise RuntimeError("Messenger bridge RPC is not connected")
        timeout = self.config.fbchat_e2ee_send_timeout if method.startswith("sendE2EE") else 60.0
        logger.debug("Calling Messenger bridge RPC: method=%s timeout=%s", method, timeout)
        data = await bridge.call(method, params, timeout=timeout)
        return self._normalize_send_result(data)

    def _e2ee_reply_parts(self, quote: Optional[QuoteData]) -> tuple[str, str]:
        if quote is None or quote.transport != "e2ee" or not quote.message_id:
            return "", ""

        sender_jid = (quote.sender_jid or "").strip()
        if not sender_jid and quote.sender_id == self.self_id:
            sender_jid = self._self_e2ee_jid()
        if not sender_jid:
            logger.warning("E2EE reply metadata skipped for %s: missing sender JID", quote.message_id)
            return "", ""
        return quote.message_id, sender_jid

    def _self_e2ee_jid(self) -> str:
        return f"{self.self_id}@s.whatsapp.net" if self.self_id else ""

    def self_sender_jid(self, transport: str) -> str:
        return self._self_e2ee_jid() if transport == "e2ee" else ""

    @staticmethod
    def _normalize_send_result(data: dict) -> dict:
        payload = data or {}
        return {
            "success": 1,
            "payload": {
                "messageID": payload.get("messageId") or payload.get("messageID") or payload.get("id"),
                "timestamp": payload.get("timestampMs") or payload.get("timestamp") or 0,
            },
        }
