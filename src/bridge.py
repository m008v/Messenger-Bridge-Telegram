from __future__ import annotations

import asyncio
import logging
import mimetypes
import time
import traceback
from dataclasses import dataclass, replace
from typing import Optional
from urllib.parse import urlparse

import requests
from telegram import Bot, InputFile, Message, MessageReactionUpdated, ReplyParameters
from telegram.constants import ChatAction, ParseMode
from telegram.error import NetworkError, RetryAfter, TelegramError, TimedOut
from telegram.ext import Application

from config import AppConfig
from messenger.client import MessengerClient
from messenger.events import parse_messenger_activity, parse_messenger_event
from models import IncomingMessengerActivity, IncomingMessengerMessage, MessengerAttachment, QuoteData, TopicEntry
from store import BridgeStore
from utils.formatting import (
    escape_html,
    format_messenger_activity,
    format_messenger_message,
    format_topic_intro,
    telegram_message_to_text,
    topic_name,
    truncate,
)


logger = logging.getLogger(__name__)
READ_RECEIPT_TTL_SECONDS = 5.0
TYPING_TTL_SECONDS = 20.0
ACTIVITY_DEDUPE_RETENTION_MS = 10 * 60 * 1000
REACTION_STATE_RETENTION_MS = 6 * 60 * 60 * 1000
PHOTO_CAPTION_LIMIT = 1024
MEDIA_DOWNLOAD_TIMEOUT = (10, 45)
MEDIA_DOWNLOAD_MAX_BYTES = 50 * 1024 * 1024
TOPIC_ICON_USER_COLOR = 0x6FB9F0
TOPIC_ICON_GROUP_COLOR = 0x8EEE98
MEDIA_DOWNLOAD_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
}


def _preview(value: object, limit: int = 180) -> str:
    text = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


@dataclass
class ForwardResult:
    ok: bool
    message: str = ""


class MessengerTelegramBridge:
    def __init__(self, config: AppConfig, store: BridgeStore, messenger: MessengerClient) -> None:
        self.config = config
        self.store = store
        self.messenger = messenger
        self.bot: Optional[Bot] = None
        self.loop: Optional[asyncio.AbstractEventLoop] = None
        self._topic_locks: dict[str, asyncio.Lock] = {}
        self._activity_dedupe: dict[str, int] = {}
        self._reaction_activity_state: dict[str, tuple[str, int]] = {}
        self._typing_messages: dict[str, tuple[int, int]] = {}
        self._ephemeral_delete_tasks: dict[str, asyncio.Task] = {}

    async def start(self, application: Application) -> None:
        self.bot = application.bot
        self.loop = asyncio.get_running_loop()
        logger.info("Bridge starting: telegram_group_id=%s store_topics=%s", self.config.telegram_group_id, len(self.store.all_topics()))
        await self.bot.set_my_commands([
            ("status", "Show bridge status"),
            ("checktopics", "Check Telegram topic permissions"),
            ("topic", "Manage Messenger topic mappings"),
            ("help", "Show bridge help"),
        ])

        await self.messenger.login()
        logger.info("Messenger login OK: facebook_id=%s", self.messenger.self_id or "-")
        await self.messenger.start(self._on_messenger_event_from_thread)
        logger.info("Messenger listener started")
        await self._notify_general(
            "Messenger bridge started. Waiting for E2EE and regular Messenger events."
        )

    async def shutdown(self, application: Application) -> None:
        logger.info("Bridge shutting down")
        for task in self._ephemeral_delete_tasks.values():
            task.cancel()
        self._ephemeral_delete_tasks.clear()
        self._typing_messages.clear()
        await self.messenger.stop()

    def _on_messenger_event_from_thread(self, event: dict) -> None:
        if self.loop is None:
            return
        future = asyncio.run_coroutine_threadsafe(self.handle_messenger_event(event), self.loop)
        future.add_done_callback(self._log_future_error)

    @staticmethod
    def _log_future_error(future: asyncio.Future) -> None:
        try:
            future.result()
        except Exception:  # noqa: BLE001
            traceback.print_exc()

    async def handle_messenger_event(self, event: dict) -> None:
        event_type = event.get("type")
        logger.debug("Messenger raw event: type=%s keys=%s", event_type, sorted(event.keys()))
        if event_type in {"error", "disconnected"}:
            data = event.get("data") or {}
            logger.error("Messenger %s: %s", event_type, data.get("message") or data)
            await self._notify_general(f"Messenger {event_type}: {data.get('message') or data}")
            return

        message = parse_messenger_event(event)
        if message is None:
            activity = parse_messenger_activity(event)
            if activity is not None:
                await self.handle_messenger_activity(activity)
            else:
                logger.debug("Ignored Messenger event: type=%s", event_type)
            return
        if self.config.ignore_self_messages and message.sender_id == self.messenger.self_id:
            logger.debug(
                "Ignored self Messenger message: transport=%s messenger_id=%s message_id=%s",
                message.transport,
                message.messenger_id,
                message.message_id,
            )
            return

        await self._enrich_sender_name(message)
        await self._clear_typing_for_message(message)
        logger.info(
            "Messenger -> Telegram received: transport=%s messenger_id=%s thread_id=%s chat_jid=%s sender=%s message_id=%s reply_to=%s text=%s attachments=%s",
            message.transport,
            message.messenger_id,
            message.thread_id or "-",
            message.chat_jid or "-",
            message.sender_name or message.sender_id or "-",
            message.message_id or "-",
            message.reply_to_message_id or "-",
            _preview(message.text),
            len(message.attachments),
        )

        entry = await self._get_or_create_topic(message)
        reply_to = None
        if message.reply_to_message_id:
            reply_to = self.store.get_tg_message_id(message.transport, message.reply_to_message_id)

        sent = await self._send_inbound_with_topic_recovery(entry, message, reply_to)
        logger.info(
            "Messenger -> Telegram sent: topic_id=%s tg_message_id=%s transport=%s messenger_id=%s messenger_message_id=%s reply_to_tg=%s",
            sent.message_thread_id or entry.topic_id,
            sent.message_id,
            message.transport,
            message.messenger_id,
            message.message_id or "-",
            reply_to or "-",
        )
        if message.message_id:
            quote = QuoteData(
                message_id=message.message_id,
                transport=message.transport,
                messenger_id=message.messenger_id,
                sender_id=message.sender_id,
                sender_jid=message.sender_jid or "",
                chat_jid=message.chat_jid or "",
                thread_id=message.thread_id,
            )
            self.store.save_message(sent.message_id, [message.message_id], quote)

    async def handle_messenger_activity(self, activity: IncomingMessengerActivity) -> None:
        self._resolve_activity_context(activity)
        if not activity.messenger_id:
            logger.warning(
                "Messenger activity cannot be mapped: kind=%s raw_event_type=%s target_message_id=%s",
                activity.kind,
                activity.raw_event_type,
                activity.target_message_id or "-",
            )
            await self._notify_general(
                f"Messenger activity {activity.raw_event_type or activity.kind} could not be mapped to a topic."
            )
            return

        await self._enrich_activity_actor_name(activity)
        if not self._should_forward_activity(activity):
            logger.debug(
                "Messenger activity skipped by config/dedupe: kind=%s transport=%s messenger_id=%s actor=%s target=%s",
                activity.kind,
                activity.transport,
                activity.messenger_id,
                activity.actor_name or activity.actor_id or "-",
                activity.target_message_id or "-",
            )
            return

        if activity.kind == "typing":
            await self._handle_typing_activity(activity)
            return

        logger.info(
            "Messenger -> Telegram activity: kind=%s transport=%s messenger_id=%s thread_id=%s chat_jid=%s actor=%s target=%s text=%s",
            activity.kind,
            activity.transport,
            activity.messenger_id,
            activity.thread_id or "-",
            activity.chat_jid or "-",
            activity.actor_name or activity.actor_id or "-",
            activity.target_message_id or "-",
            _preview(activity.text or activity.reaction or activity.receipt_type),
        )
        topic_message = self._activity_topic_message(activity)
        entry = await self._get_or_create_topic(topic_message)
        reply_to = self._activity_reply_to_tg(activity)
        sent = await self._send_activity_with_topic_recovery(entry, activity, reply_to)
        if activity.kind in {"read_receipt", "e2ee_receipt"}:
            self._schedule_delete_message(
                f"read:{activity.kind}:{sent.chat_id}:{sent.message_id}",
                sent.chat_id,
                sent.message_id,
                READ_RECEIPT_TTL_SECONDS,
                f"{activity.kind} expired",
            )
        logger.info(
            "Messenger -> Telegram activity sent: topic_id=%s kind=%s reply_to_tg=%s",
            entry.topic_id,
            activity.kind,
            reply_to or "-",
        )

    async def _handle_typing_activity(self, activity: IncomingMessengerActivity) -> None:
        key = self._typing_key(activity.transport, activity.messenger_id, activity.actor_id or activity.actor_jid or "")
        if activity.is_typing is False:
            await self._delete_typing_message(key, "typing stopped")
            return

        topic_message = self._activity_topic_message(activity)
        entry = await self._get_or_create_topic(topic_message)
        await self._send_typing_action(entry)

        existing = self._typing_messages.get(key)
        if existing:
            self._schedule_delete_message(key, existing[0], existing[1], TYPING_TTL_SECONDS, "typing stale")
            logger.debug(
                "Typing indicator refreshed: key=%s topic_id=%s message_id=%s",
                key,
                entry.topic_id,
                existing[1],
            )
            return

        logger.info(
            "Messenger -> Telegram typing: transport=%s messenger_id=%s actor=%s topic_id=%s",
            activity.transport,
            activity.messenger_id,
            activity.actor_name or activity.actor_id or "-",
            entry.topic_id,
        )
        sent = await self._send_activity_with_topic_recovery(entry, activity, self._activity_reply_to_tg(activity))
        self._typing_messages[key] = (sent.chat_id, sent.message_id)
        self._schedule_delete_message(key, sent.chat_id, sent.message_id, TYPING_TTL_SECONDS, "typing stale")
        logger.info(
            "Messenger -> Telegram typing sent: topic_id=%s tg_message_id=%s key=%s",
            sent.message_thread_id or entry.topic_id,
            sent.message_id,
            key,
        )

    async def _send_typing_action(self, entry: TopicEntry) -> None:
        if self.bot is None:
            return
        kwargs = {
            "chat_id": self.config.telegram_group_id,
            "action": ChatAction.TYPING,
        }
        if entry.topic_id > 1:
            kwargs["message_thread_id"] = entry.topic_id
        try:
            await self._telegram_call(self.bot.send_chat_action, **kwargs)
        except TelegramError as exc:
            logger.debug("Could not send Telegram typing action: topic_id=%s error=%s", entry.topic_id, exc)

    async def _clear_typing_for_message(self, message: IncomingMessengerMessage) -> None:
        actor_candidates = [message.sender_id, self._jid_user(message.sender_jid), message.sender_jid]
        seen: set[str] = set()
        for actor in actor_candidates:
            clean_actor = str(actor or "").strip()
            if not clean_actor or clean_actor in seen:
                continue
            seen.add(clean_actor)
            await self._delete_typing_message(
                self._typing_key(message.transport, message.messenger_id, clean_actor),
                "new Messenger message",
            )

    async def _delete_typing_message(self, key: str, reason: str) -> None:
        existing = self._typing_messages.pop(key, None)
        task = self._ephemeral_delete_tasks.pop(key, None)
        if task:
            task.cancel()
        if existing is None:
            return
        await self._delete_telegram_message(existing[0], existing[1], reason)

    def _schedule_delete_message(self, key: str, chat_id: int, message_id: int, delay: float, reason: str) -> None:
        old_task = self._ephemeral_delete_tasks.pop(key, None)
        if old_task:
            old_task.cancel()
        self._ephemeral_delete_tasks[key] = asyncio.create_task(
            self._delete_message_later(key, chat_id, message_id, delay, reason)
        )

    async def _delete_message_later(self, key: str, chat_id: int, message_id: int, delay: float, reason: str) -> None:
        try:
            await asyncio.sleep(delay)
            await self._delete_telegram_message(chat_id, message_id, reason)
        except asyncio.CancelledError:
            raise
        finally:
            current_task = asyncio.current_task()
            if self._ephemeral_delete_tasks.get(key) is current_task:
                self._ephemeral_delete_tasks.pop(key, None)
                self._typing_messages.pop(key, None)

    async def _delete_telegram_message(self, chat_id: int, message_id: int, reason: str) -> None:
        if self.bot is None:
            return
        try:
            await self._telegram_call(self.bot.delete_message, chat_id=chat_id, message_id=message_id)
            logger.info("Deleted ephemeral Telegram message: chat_id=%s message_id=%s reason=%s", chat_id, message_id, reason)
        except TelegramError as exc:
            logger.debug("Could not delete ephemeral Telegram message: chat_id=%s message_id=%s reason=%s error=%s", chat_id, message_id, reason, exc)

    @staticmethod
    def _typing_key(transport: str, messenger_id: str, actor_id: str) -> str:
        return f"typing:{transport}:{messenger_id}:{actor_id}"

    @staticmethod
    def _jid_user(value: Optional[str]) -> str:
        text = str(value or "").strip()
        if "@" not in text:
            return text
        return text.split("@", 1)[0]

    async def _get_or_create_topic(self, message: IncomingMessengerMessage) -> TopicEntry:
        existing = self.store.get_topic_by_messenger(message.transport, message.messenger_id)
        if existing:
            logger.debug(
                "Topic mapping found: topic_id=%s transport=%s messenger_id=%s thread_id=%s chat_jid=%s name=%s",
                existing.topic_id,
                existing.transport,
                existing.messenger_id,
                existing.thread_id or "-",
                existing.chat_jid or "-",
                existing.name or "-",
            )
            await self._maybe_rename_existing_topic(existing, message)
            return existing

        key = f"{message.transport}:{message.messenger_id}"
        lock = self._topic_locks.setdefault(key, asyncio.Lock())
        async with lock:
            existing = self.store.get_topic_by_messenger(message.transport, message.messenger_id)
            if existing:
                logger.debug(
                    "Topic mapping found after lock: topic_id=%s transport=%s messenger_id=%s",
                    existing.topic_id,
                    existing.transport,
                    existing.messenger_id,
                )
                await self._maybe_rename_existing_topic(existing, message)
                return existing

            display_name = await self._resolve_topic_display_name(message)
            name = topic_name(display_name, message.transport)
            topic_kind = self._topic_kind(message)
            logger.info(
                "Creating Telegram topic: name=%s transport=%s topic_kind=%s messenger_id=%s thread_id=%s chat_jid=%s",
                name,
                message.transport,
                topic_kind,
                message.messenger_id,
                message.thread_id or "-",
                message.chat_jid or "-",
            )
            topic_id = await self._create_forum_topic(name, message)
            entry = self.store.set_topic(TopicEntry(
                topic_id=topic_id,
                messenger_id=message.messenger_id,
                transport=message.transport,
                name=display_name,
                thread_id=message.thread_id,
                chat_jid=message.chat_jid,
            ))
            logger.info(
                "Topic mapping saved: topic_id=%s transport=%s messenger_id=%s thread_id=%s chat_jid=%s name=%s",
                entry.topic_id,
                entry.transport,
                entry.messenger_id,
                entry.thread_id or "-",
                entry.chat_jid or "-",
                entry.name or "-",
            )
            await self._send_topic_intro(entry, message)
            return entry

    async def _enrich_sender_name(self, message: IncomingMessengerMessage) -> None:
        if not message.sender_id:
            return
        if message.sender_name and message.sender_name != message.sender_id:
            return
        resolved = await self.messenger.resolve_user_name(message.sender_id)
        if resolved:
            message.sender_name = resolved

    async def _enrich_activity_actor_name(self, activity: IncomingMessengerActivity) -> None:
        if not activity.actor_id:
            return
        if activity.actor_name and activity.actor_name != activity.actor_id:
            return
        resolved = await self.messenger.resolve_user_name(activity.actor_id)
        if resolved:
            activity.actor_name = resolved

    def _resolve_activity_context(self, activity: IncomingMessengerActivity) -> None:
        if activity.target_message_id:
            quote = self.store.get_quote_by_messenger(activity.transport, activity.target_message_id)
            if quote is None:
                quote = self.store.find_quote_by_messenger(activity.target_message_id)
            if quote is not None:
                activity.transport = quote.transport
                activity.messenger_id = activity.messenger_id or quote.messenger_id
                activity.thread_id = activity.thread_id or quote.thread_id
                activity.chat_jid = activity.chat_jid or quote.chat_jid or None
                activity.actor_id = activity.actor_id or quote.sender_id
                activity.actor_jid = activity.actor_jid or quote.sender_jid or None
                activity.actor_name = activity.actor_name or activity.actor_id

        if activity.transport == "e2ee":
            activity.messenger_id = activity.messenger_id or activity.chat_jid or ""
            activity.chat_jid = activity.chat_jid or activity.messenger_id or None
        else:
            activity.messenger_id = activity.messenger_id or activity.thread_id
            activity.thread_id = activity.thread_id or activity.messenger_id

    def _activity_topic_message(self, activity: IncomingMessengerActivity) -> IncomingMessengerMessage:
        return IncomingMessengerMessage(
            transport=activity.transport,
            messenger_id=activity.messenger_id,
            thread_id=activity.thread_id,
            chat_jid=activity.chat_jid,
            sender_id=activity.actor_id,
            sender_jid=activity.actor_jid,
            sender_name=activity.actor_name or activity.actor_id or activity.messenger_id,
            text="",
            message_id=activity.target_message_id,
            timestamp_ms=activity.timestamp_ms,
            raw_event_type=activity.raw_event_type,
            raw=activity.raw,
        )

    def _activity_reply_to_tg(self, activity: IncomingMessengerActivity) -> Optional[int]:
        candidates = [activity.target_message_id, *activity.target_message_ids]
        for message_id in candidates:
            if not message_id:
                continue
            tg_message_id = self.store.get_tg_message_id(activity.transport, message_id)
            if tg_message_id:
                return tg_message_id
        return None

    def _should_forward_activity(self, activity: IncomingMessengerActivity) -> bool:
        if activity.kind == "reaction":
            return self._should_forward_reaction_activity(activity)
        if activity.kind == "typing":
            if not self.config.forward_typing_activity:
                return False
            return self._dedupe_activity(
                f"typing:{activity.messenger_id}:{activity.actor_id}:{activity.is_typing}",
                30000,
            )
        if activity.kind in {"read_receipt", "e2ee_receipt"}:
            if not self.config.forward_read_receipts:
                return False
            target = activity.target_message_id or activity.text or activity.receipt_type
            return self._dedupe_activity(
                f"receipt:{activity.kind}:{activity.messenger_id}:{activity.actor_id}:{target}",
                60000,
            )
        return True

    def _should_forward_reaction_activity(self, activity: IncomingMessengerActivity) -> bool:
        if not self.config.forward_messenger_reactions:
            return False

        now = int(time.time() * 1000)
        actor = activity.actor_id or activity.actor_jid or "-"
        target = activity.target_message_id or "-"
        reaction = activity.reaction or "<remove>"
        key = f"{activity.transport}:{activity.messenger_id}:{actor}:{target}"
        previous = self._reaction_activity_state.get(key)
        self._reaction_activity_state[key] = (reaction, now)
        self._cleanup_reaction_activity_state(now)
        if previous is not None and previous[0] == reaction:
            return False
        return True

    def _cleanup_reaction_activity_state(self, now_ms: int) -> None:
        for old_key, (_, seen_at) in list(self._reaction_activity_state.items()):
            if now_ms - seen_at > REACTION_STATE_RETENTION_MS:
                self._reaction_activity_state.pop(old_key, None)

    def _dedupe_activity(self, key: str, window_ms: int) -> bool:
        now = int(time.time() * 1000)
        last = self._activity_dedupe.get(key, 0)
        if now - last < window_ms:
            return False
        self._activity_dedupe[key] = now
        for old_key, seen_at in list(self._activity_dedupe.items()):
            if now - seen_at > ACTIVITY_DEDUPE_RETENTION_MS:
                self._activity_dedupe.pop(old_key, None)
        return True

    async def _resolve_topic_display_name(self, message: IncomingMessengerMessage) -> str:
        resolved = await self.messenger.resolve_topic_display_name(message)
        return resolved or message.sender_name or message.messenger_id

    async def _maybe_rename_existing_topic(self, entry: TopicEntry, message: IncomingMessengerMessage) -> None:
        if entry.topic_id <= 1:
            return
        display_name = await self._resolve_topic_display_name(message)
        if not display_name or display_name == entry.name:
            return
        if not self._should_replace_topic_name(entry.name, display_name, message):
            return

        assert self.bot is not None
        try:
            await self._telegram_call(
                self.bot.edit_forum_topic,
                chat_id=self.config.telegram_group_id,
                message_thread_id=entry.topic_id,
                name=topic_name(display_name, entry.transport),
            )
            self.store.update_topic_name(entry.topic_id, display_name)
            entry.name = display_name
            logger.info("Telegram topic renamed: topic_id=%s name=%s", entry.topic_id, display_name)
        except TelegramError as exc:
            logger.warning("Could not rename Telegram topic %s: %s", entry.topic_id, exc)

    @staticmethod
    def _should_replace_topic_name(old_name: str, new_name: str, message: IncomingMessengerMessage) -> bool:
        old = (old_name or "").strip()
        if not old or old == new_name:
            return False
        generic_names = {
            str(message.messenger_id or ""),
            str(message.sender_id or ""),
            str(message.sender_name or ""),
            str(message.thread_id or ""),
            str(message.chat_jid or ""),
        }
        if MessengerTelegramBridge._is_regular_group_message(message) and old == str(message.sender_name or "").strip():
            return True
        if old.lower().startswith("messenger group "):
            return True
        return old in generic_names or old.isdigit() or "@" in old

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
    def _is_group_message(message: IncomingMessengerMessage) -> bool:
        if MessengerTelegramBridge._is_regular_group_message(message):
            return True
        if message.thread_type not in {0, 1}:
            return True

        thread_id = str(message.thread_id or message.messenger_id or "").strip()
        sender_id = str(message.sender_id or "").strip()
        if message.transport == "e2ee" and message.thread_name and thread_id and sender_id and thread_id != sender_id:
            return True
        return False

    @staticmethod
    def _topic_kind(message: IncomingMessengerMessage) -> str:
        return "group" if MessengerTelegramBridge._is_group_message(message) else "user"

    @staticmethod
    def _topic_icon_color(message: IncomingMessengerMessage) -> int:
        if MessengerTelegramBridge._is_group_message(message):
            return TOPIC_ICON_GROUP_COLOR
        return TOPIC_ICON_USER_COLOR

    async def _create_forum_topic(self, name: str, message: IncomingMessengerMessage) -> int:
        assert self.bot is not None
        icon_color = self._topic_icon_color(message)
        try:
            topic = await self._telegram_call(
                self.bot.create_forum_topic,
                chat_id=self.config.telegram_group_id,
                name=name,
                icon_color=icon_color,
            )
            return topic.message_thread_id
        except TelegramError as exc:
            logger.warning("Could not create Telegram forum topic, using General topic: %s", exc)
            await self._notify_general(
                "Could not create Telegram topic. Using General topic instead. "
                "Run /checktopics to verify the group is a forum supergroup and the bot has Manage Topics permission. "
                f"Telegram error: {exc}"
            )
            return 1

    async def _send_topic_intro(self, entry: TopicEntry, message: IncomingMessengerMessage) -> None:
        assert self.bot is not None
        try:
            await self._telegram_call(
                self.bot.send_message,
                chat_id=self.config.telegram_group_id,
                text=format_topic_intro(message),
                message_thread_id=entry.topic_id,
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=True,
            )
        except TelegramError as exc:
            logger.warning("Could not send topic intro: topic_id=%s error=%s", entry.topic_id, exc)

    async def _send_inbound_to_telegram(
        self,
        entry: TopicEntry,
        message: IncomingMessengerMessage,
        reply_to_tg_message_id: Optional[int],
        *,
        use_topic: bool = True,
    ) -> Message:
        assert self.bot is not None
        image_attachments = self._uploadable_image_attachments(message)
        if image_attachments:
            sent = await self._send_inbound_images_to_telegram(
                entry,
                message,
                image_attachments,
                reply_to_tg_message_id,
                use_topic=use_topic,
            )
            if sent is not None:
                return sent

        return await self._send_inbound_text_to_telegram(entry, message, reply_to_tg_message_id, use_topic=use_topic)

    async def _send_inbound_text_to_telegram(
        self,
        entry: TopicEntry,
        message: IncomingMessengerMessage,
        reply_to_tg_message_id: Optional[int],
        *,
        use_topic: bool = True,
    ) -> Message:
        assert self.bot is not None
        kwargs = self._telegram_send_kwargs(entry, reply_to_tg_message_id, use_topic=use_topic)
        kwargs.update({
            "text": format_messenger_message(message),
            "parse_mode": ParseMode.HTML,
            "disable_web_page_preview": False,
        })
        return await self._telegram_call(self.bot.send_message, **kwargs)

    async def _send_inbound_images_to_telegram(
        self,
        entry: TopicEntry,
        message: IncomingMessengerMessage,
        image_attachments: list[MessengerAttachment],
        reply_to_tg_message_id: Optional[int],
        *,
        use_topic: bool = True,
    ) -> Optional[Message]:
        uploaded_ids = {id(attachment) for attachment in image_attachments}
        caption_attachments = [
            attachment
            for attachment in message.attachments
            if id(attachment) not in uploaded_ids
        ]
        first_caption = self._messenger_photo_caption(message, caption_attachments)
        first_sent: Optional[Message] = None
        failed_attachments: list[MessengerAttachment] = []

        for index, attachment in enumerate(image_attachments, start=1):
            sent = await self._send_image_attachment_to_telegram(
                entry,
                attachment,
                first_caption if first_sent is None else None,
                reply_to_tg_message_id,
                use_topic=use_topic,
                index=index,
            )
            if sent is None:
                failed_attachments.append(attachment)
                continue
            if first_sent is None:
                first_sent = sent

        if first_sent is not None and failed_attachments:
            fallback_message = replace(message, text="", attachments=failed_attachments)
            await self._send_inbound_text_to_telegram(entry, fallback_message, None, use_topic=use_topic)

        return first_sent

    async def _send_image_attachment_to_telegram(
        self,
        entry: TopicEntry,
        attachment: MessengerAttachment,
        caption: Optional[str],
        reply_to_tg_message_id: Optional[int],
        *,
        use_topic: bool,
        index: int,
    ) -> Optional[Message]:
        assert self.bot is not None
        try:
            data, content_type = await asyncio.to_thread(self._download_attachment_bytes, attachment)
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "Messenger image download failed: attachment_index=%s type=%s filename=%s origin=%s error=%s",
                index,
                attachment.type or "-",
                attachment.file_name or "-",
                self._url_origin(attachment.best_url),
                exc,
            )
            return None

        filename = self._attachment_filename(attachment, content_type, index)
        logger.info(
            "Messenger -> Telegram uploading image: topic_id=%s filename=%s mime=%s bytes=%s",
            entry.topic_id,
            filename,
            content_type or attachment.mime_type or "-",
            len(data),
        )

        kwargs = self._telegram_send_kwargs(entry, reply_to_tg_message_id, use_topic=use_topic)
        kwargs["photo"] = InputFile(data, filename=filename)
        if caption:
            kwargs["caption"] = truncate(caption, PHOTO_CAPTION_LIMIT)
            kwargs["parse_mode"] = ParseMode.HTML

        try:
            return await self._telegram_call(self.bot.send_photo, **kwargs)
        except TelegramError as exc:
            if self._is_topic_missing_error(exc):
                raise
            logger.warning(
                "Telegram send_photo failed, retrying as document: topic_id=%s filename=%s error=%s",
                entry.topic_id,
                filename,
                exc,
            )

        document_kwargs = self._telegram_send_kwargs(entry, reply_to_tg_message_id, use_topic=use_topic)
        document_kwargs["document"] = InputFile(data, filename=filename)
        if caption:
            document_kwargs["caption"] = truncate(caption, PHOTO_CAPTION_LIMIT)
            document_kwargs["parse_mode"] = ParseMode.HTML

        try:
            return await self._telegram_call(self.bot.send_document, **document_kwargs)
        except TelegramError as exc:
            if self._is_topic_missing_error(exc):
                raise
            logger.warning(
                "Telegram send_document failed for Messenger image: topic_id=%s filename=%s error=%s",
                entry.topic_id,
                filename,
                exc,
            )
            return None

    def _telegram_send_kwargs(
        self,
        entry: TopicEntry,
        reply_to_tg_message_id: Optional[int],
        *,
        use_topic: bool = True,
    ) -> dict[str, object]:
        kwargs: dict[str, object] = {"chat_id": self.config.telegram_group_id}
        if use_topic:
            kwargs["message_thread_id"] = entry.topic_id
        if reply_to_tg_message_id:
            kwargs["reply_parameters"] = ReplyParameters(
                message_id=reply_to_tg_message_id,
                allow_sending_without_reply=True,
            )
        return kwargs

    @staticmethod
    def _uploadable_image_attachments(message: IncomingMessengerMessage) -> list[MessengerAttachment]:
        return [
            attachment
            for attachment in message.attachments
            if MessengerTelegramBridge._is_uploadable_image_attachment(attachment)
        ]

    @staticmethod
    def _is_uploadable_image_attachment(attachment: MessengerAttachment) -> bool:
        if not attachment.best_url:
            return False
        attachment_type = str(attachment.type or "").strip().lower()
        mime_type = str(attachment.mime_type or "").strip().lower()
        if attachment_type in {"sticker", "gif"}:
            return False
        return attachment_type == "image" or mime_type.startswith("image/")

    @staticmethod
    def _messenger_photo_caption(
        message: IncomingMessengerMessage,
        remaining_attachments: list[MessengerAttachment],
    ) -> str:
        if message.text or remaining_attachments:
            return truncate(format_messenger_message(replace(message, attachments=remaining_attachments)), PHOTO_CAPTION_LIMIT)

        sender = escape_html(truncate(message.sender_name or message.sender_id or "Messenger", 80))
        badge = " <code>E2EE</code>" if message.transport == "e2ee" else ""
        return f"<b>{sender}</b>{badge}"

    @staticmethod
    def _download_attachment_bytes(attachment: MessengerAttachment) -> tuple[bytes, str]:
        url = attachment.best_url
        if not url:
            raise ValueError("attachment has no URL")

        response = requests.get(
            url,
            headers=MEDIA_DOWNLOAD_HEADERS,
            stream=True,
            timeout=MEDIA_DOWNLOAD_TIMEOUT,
        )
        response.raise_for_status()

        content_type = str(response.headers.get("content-type") or attachment.mime_type or "")
        content_type = content_type.split(";", 1)[0].strip().lower()
        if content_type.startswith("text/") or content_type in {"application/json", "application/xml"}:
            raise ValueError(f"downloaded content is not an image: {content_type}")

        data = bytearray()
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            data.extend(chunk)
            if len(data) > MEDIA_DOWNLOAD_MAX_BYTES:
                raise ValueError("image is larger than upload limit")

        if not data:
            raise ValueError("downloaded image is empty")

        return bytes(data), content_type

    @staticmethod
    def _attachment_filename(attachment: MessengerAttachment, content_type: str, index: int) -> str:
        parsed_name = ""
        if attachment.file_name:
            parsed_name = str(attachment.file_name).replace("\\", "/").rsplit("/", 1)[-1].strip()
        if not parsed_name and attachment.best_url:
            parsed_name = urlparse(attachment.best_url).path.rsplit("/", 1)[-1].strip()
        if not parsed_name:
            parsed_name = f"messenger-image-{index}"

        if "." not in parsed_name:
            extension = mimetypes.guess_extension(content_type or attachment.mime_type or "") or ".jpg"
            parsed_name = f"{parsed_name}{extension}"
        return parsed_name

    @staticmethod
    def _url_origin(url: Optional[str]) -> str:
        if not url:
            return "-"
        parsed = urlparse(url)
        return parsed.netloc or "-"

    async def _send_inbound_with_topic_recovery(
        self,
        entry: TopicEntry,
        message: IncomingMessengerMessage,
        reply_to_tg_message_id: Optional[int],
    ) -> Message:
        try:
            return await self._send_inbound_to_telegram(entry, message, reply_to_tg_message_id)
        except TelegramError as exc:
            if not self._is_topic_missing_error(exc):
                raise

            self.store.remove_topic(entry.topic_id)
            logger.warning(
                "Telegram topic stale during message send: topic_id=%s transport=%s messenger_id=%s",
                entry.topic_id,
                entry.transport,
                entry.messenger_id,
            )
            await self._notify_general(
                f"Telegram topic {entry.topic_id} for {entry.name} no longer exists. "
                "Creating a fresh topic and retrying."
            )

        fresh_entry = await self._get_or_create_topic(message)
        try:
            return await self._send_inbound_to_telegram(fresh_entry, message, reply_to_tg_message_id)
        except TelegramError as exc:
            if not self._is_topic_missing_error(exc):
                raise

            await self._notify_general(
                "Telegram still rejected the new topic with 'message thread not found'. "
                "Sending this Messenger message to the group without a topic. Run /checktopics to verify forum permissions."
            )
            return await self._send_inbound_to_telegram(
                fresh_entry,
                message,
                None,
                use_topic=False,
            )

    async def _send_activity_to_telegram(
        self,
        entry: TopicEntry,
        activity: IncomingMessengerActivity,
        reply_to_tg_message_id: Optional[int],
        *,
        use_topic: bool = True,
    ) -> Message:
        assert self.bot is not None
        kwargs = {
            "chat_id": self.config.telegram_group_id,
            "text": format_messenger_activity(activity),
            "parse_mode": ParseMode.HTML,
            "disable_web_page_preview": True,
        }
        if use_topic:
            kwargs["message_thread_id"] = entry.topic_id
        if reply_to_tg_message_id:
            kwargs["reply_parameters"] = ReplyParameters(
                message_id=reply_to_tg_message_id,
                allow_sending_without_reply=True,
            )
        return await self._telegram_call(self.bot.send_message, **kwargs)

    async def _send_activity_with_topic_recovery(
        self,
        entry: TopicEntry,
        activity: IncomingMessengerActivity,
        reply_to_tg_message_id: Optional[int],
    ) -> Message:
        try:
            return await self._send_activity_to_telegram(entry, activity, reply_to_tg_message_id)
        except TelegramError as exc:
            if not self._is_topic_missing_error(exc):
                raise

            self.store.remove_topic(entry.topic_id)
            logger.warning(
                "Telegram topic stale during activity send: topic_id=%s transport=%s messenger_id=%s",
                entry.topic_id,
                entry.transport,
                entry.messenger_id,
            )
            await self._notify_general(
                f"Telegram topic {entry.topic_id} for {entry.name} no longer exists. "
                "Creating a fresh topic and retrying."
            )

        fresh_entry = await self._get_or_create_topic(self._activity_topic_message(activity))
        try:
            return await self._send_activity_to_telegram(fresh_entry, activity, reply_to_tg_message_id)
        except TelegramError as exc:
            if not self._is_topic_missing_error(exc):
                raise

            await self._notify_general(
                "Telegram still rejected the new topic with 'message thread not found'. "
                "Sending this Messenger activity to the group without a topic. Run /checktopics to verify forum permissions."
            )
            return await self._send_activity_to_telegram(
                fresh_entry,
                activity,
                None,
                use_topic=False,
            )

    async def forward_telegram_message(self, message: Message) -> ForwardResult:
        if message.chat_id != self.config.telegram_group_id:
            logger.debug("Ignored Telegram message from another chat: chat_id=%s message_id=%s", message.chat_id, message.message_id)
            return ForwardResult(False)
        if message.from_user and message.from_user.is_bot:
            logger.debug("Ignored Telegram bot message: user_id=%s message_id=%s", message.from_user.id, message.message_id)
            return ForwardResult(False)

        topic_id = message.message_thread_id
        if not topic_id:
            logger.info("Telegram message rejected: no topic message_id=%s", message.message_id)
            return ForwardResult(False, "Send messages inside a mapped forum topic.")

        entry = self.store.get_topic_by_id(topic_id)
        if entry is None:
            logger.info("Telegram message rejected: unmapped topic_id=%s message_id=%s", topic_id, message.message_id)
            return ForwardResult(False, "This topic is not linked to a Messenger conversation.")

        quote = None
        if message.reply_to_message:
            quote = self.store.get_quote_by_tg(message.reply_to_message.message_id)

        if message.sticker:
            return await self._forward_telegram_sticker(message, entry, quote)

        text = telegram_message_to_text(message).strip()
        if not text:
            logger.info("Telegram message rejected: no bridgeable content topic_id=%s tg_message_id=%s", topic_id, message.message_id)
            return ForwardResult(False, "This Telegram message has no bridgeable text content yet.")

        sender = message.from_user.full_name if message.from_user else "-"
        logger.info(
            "Telegram -> Messenger received: topic_id=%s tg_message_id=%s from=%s transport=%s messenger_id=%s thread_id=%s chat_jid=%s reply_quote=%s text=%s",
            topic_id,
            message.message_id,
            sender,
            entry.transport,
            entry.messenger_id,
            entry.thread_id or "-",
            entry.chat_jid or "-",
            quote.message_id if quote else "-",
            _preview(text),
        )
        try:
            result = await self.messenger.send_text(entry, text, quote)
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Telegram -> Messenger failed: topic_id=%s tg_message_id=%s transport=%s messenger_id=%s thread_id=%s",
                topic_id,
                message.message_id,
                entry.transport,
                entry.messenger_id,
                entry.thread_id or "-",
            )
            return ForwardResult(False, f"Messenger send failed: {exc}")

        if not isinstance(result, dict) or result.get("error"):
            payload = result.get("payload") if isinstance(result, dict) else result
            logger.error(
                "Telegram -> Messenger returned error: topic_id=%s tg_message_id=%s transport=%s messenger_id=%s payload=%s",
                topic_id,
                message.message_id,
                entry.transport,
                entry.messenger_id,
                payload,
            )
            return ForwardResult(False, f"Messenger send failed: {payload}")

        payload = result.get("payload") or {}
        messenger_message_id = str(payload.get("messageID") or "")
        logger.info(
            "Telegram -> Messenger sent: topic_id=%s tg_message_id=%s messenger_message_id=%s transport=%s messenger_id=%s thread_id=%s",
            topic_id,
            message.message_id,
            messenger_message_id or "-",
            entry.transport,
            entry.messenger_id,
            entry.thread_id or "-",
        )
        if messenger_message_id:
            self.store.save_message(
                message.message_id,
                [messenger_message_id],
                QuoteData(
                    message_id=messenger_message_id,
                    transport=entry.transport,
                    messenger_id=entry.messenger_id,
                    sender_id=self.messenger.self_id,
                    sender_jid=self.messenger.self_sender_jid(entry.transport),
                    chat_jid=entry.chat_jid or "",
                    thread_id=entry.thread_id or "",
                ),
            )

        return ForwardResult(True)

    async def forward_telegram_reaction(self, reaction: MessageReactionUpdated) -> ForwardResult:
        chat = getattr(reaction, "chat", None)
        chat_id = getattr(chat, "id", None)
        if chat_id != self.config.telegram_group_id:
            logger.debug(
                "Ignored Telegram reaction from another chat: chat_id=%s message_id=%s",
                chat_id,
                reaction.message_id,
            )
            return ForwardResult(False)

        user = getattr(reaction, "user", None)
        if user is not None and getattr(user, "is_bot", False):
            logger.debug(
                "Ignored Telegram bot reaction: user_id=%s message_id=%s",
                getattr(user, "id", "-"),
                reaction.message_id,
            )
            return ForwardResult(False)

        quote = self.store.get_quote_by_tg(reaction.message_id)
        if quote is None:
            logger.debug("Telegram reaction ignored: tg_message_id=%s has no Messenger quote mapping", reaction.message_id)
            return ForwardResult(False)

        old_emoji = self._telegram_reaction_emoji(reaction.old_reaction)
        new_emoji = self._telegram_reaction_emoji(reaction.new_reaction)
        if old_emoji == new_emoji:
            logger.debug("Telegram reaction ignored: unchanged tg_message_id=%s emoji=%s", reaction.message_id, new_emoji or "<none>")
            return ForwardResult(False)

        actor_name = "-"
        if user is not None:
            actor_name = getattr(user, "full_name", "") or str(getattr(user, "id", "-"))
        actor_chat = getattr(reaction, "actor_chat", None)
        if actor_chat is not None:
            actor_name = getattr(actor_chat, "title", "") or str(getattr(actor_chat, "id", "-"))

        logger.info(
            "Telegram -> Messenger reaction received: tg_message_id=%s actor=%s old=%s new=%s transport=%s messenger_id=%s messenger_message_id=%s",
            reaction.message_id,
            actor_name,
            old_emoji or "<none>",
            new_emoji or "<remove>",
            quote.transport,
            quote.messenger_id,
            quote.message_id,
        )
        try:
            result = await self.messenger.send_reaction(quote, new_emoji)
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Telegram -> Messenger reaction failed: tg_message_id=%s transport=%s messenger_id=%s messenger_message_id=%s",
                reaction.message_id,
                quote.transport,
                quote.messenger_id,
                quote.message_id,
            )
            return ForwardResult(False, f"Messenger reaction failed: {exc}")

        if not isinstance(result, dict) or result.get("error"):
            payload = result.get("payload") if isinstance(result, dict) else result
            logger.error(
                "Telegram -> Messenger reaction returned error: tg_message_id=%s transport=%s messenger_id=%s payload=%s",
                reaction.message_id,
                quote.transport,
                quote.messenger_id,
                payload,
            )
            return ForwardResult(False, f"Messenger reaction failed: {payload}")

        logger.info(
            "Telegram -> Messenger reaction sent: tg_message_id=%s messenger_message_id=%s emoji=%s",
            reaction.message_id,
            quote.message_id,
            new_emoji or "<remove>",
        )
        return ForwardResult(True)

    @staticmethod
    def _telegram_reaction_emoji(reactions: object) -> str:
        if not reactions:
            return ""
        if isinstance(reactions, list):
            reaction_items = reactions
        else:
            try:
                reaction_items = list(reactions)
            except TypeError:
                reaction_items = [reactions]
        for reaction in reversed(reaction_items):
            emoji = str(getattr(reaction, "emoji", "") or "").strip()
            if emoji:
                return emoji
        return ""

    async def _forward_telegram_sticker(
        self,
        message: Message,
        entry: TopicEntry,
        quote: Optional[QuoteData],
    ) -> ForwardResult:
        assert self.bot is not None
        sticker = message.sticker
        if sticker is None:
            return ForwardResult(False)

        filename, mime_type, width, height = self._telegram_sticker_meta(sticker)
        logger.info(
            "Telegram -> Messenger sticker received: topic_id=%s tg_message_id=%s transport=%s messenger_id=%s filename=%s mime=%s reply_quote=%s",
            entry.topic_id,
            message.message_id,
            entry.transport,
            entry.messenger_id,
            filename,
            mime_type,
            quote.message_id if quote else "-",
        )
        try:
            file_data, filename, mime_type, width, height = await self._download_telegram_sticker(
                sticker, filename, mime_type, width, height
            )
            result = await self.messenger.send_telegram_sticker(
                entry,
                file_data,
                filename,
                mime_type,
                width,
                height,
                quote,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception(
                "Telegram -> Messenger sticker failed: topic_id=%s tg_message_id=%s transport=%s messenger_id=%s",
                entry.topic_id,
                message.message_id,
                entry.transport,
                entry.messenger_id,
            )
            return ForwardResult(False, f"Messenger sticker send failed: {exc}")

        if not isinstance(result, dict) or result.get("error"):
            payload = result.get("payload") if isinstance(result, dict) else result
            logger.error(
                "Telegram -> Messenger sticker returned error: topic_id=%s tg_message_id=%s transport=%s messenger_id=%s payload=%s",
                entry.topic_id,
                message.message_id,
                entry.transport,
                entry.messenger_id,
                payload,
            )
            return ForwardResult(False, f"Messenger sticker send failed: {payload}")

        payload = result.get("payload") or {}
        messenger_message_id = str(payload.get("messageID") or "")
        logger.info(
            "Telegram -> Messenger sticker sent: topic_id=%s tg_message_id=%s messenger_message_id=%s transport=%s messenger_id=%s",
            entry.topic_id,
            message.message_id,
            messenger_message_id or "-",
            entry.transport,
            entry.messenger_id,
        )
        if messenger_message_id:
            self.store.save_message(
                message.message_id,
                [messenger_message_id],
                QuoteData(
                    message_id=messenger_message_id,
                    transport=entry.transport,
                    messenger_id=entry.messenger_id,
                    sender_id=self.messenger.self_id,
                    sender_jid=self.messenger.self_sender_jid(entry.transport),
                    chat_jid=entry.chat_jid or "",
                    thread_id=entry.thread_id or "",
                ),
            )

        return ForwardResult(True)

    async def _download_telegram_sticker(
        self,
        sticker,
        filename: str,
        mime_type: str,
        width: int,
        height: int,
    ) -> tuple[bytes, str, str, int, int]:
        """Download a sticker in a form Messenger can display.

        Messenger cannot render animated ``.tgs`` (Lottie) stickers and shows them as
        a file attachment, so those are replaced by the static thumbnail Telegram
        provides for every sticker.
        """
        assert self.bot is not None
        thumbnail = getattr(sticker, "thumbnail", None)
        if getattr(sticker, "is_animated", False) and thumbnail is not None:
            thumb_file = await self._telegram_call(self.bot.get_file, thumbnail.file_id)
            thumb_data = bytes(await thumb_file.download_as_bytearray())
            thumb_mime = self._image_mime_from_bytes(thumb_data)
            if thumb_mime:
                extension = "webp" if thumb_mime == "image/webp" else thumb_mime.split("/", 1)[1]
                stem = filename.rsplit(".", 1)[0]
                logger.info(
                    "Telegram animated sticker sent as static thumbnail: filename=%s mime=%s bytes=%s",
                    filename,
                    thumb_mime,
                    len(thumb_data),
                )
                return (
                    thumb_data,
                    f"{stem}.{extension}",
                    thumb_mime,
                    int(getattr(thumbnail, "width", 0) or 0),
                    int(getattr(thumbnail, "height", 0) or 0),
                )
            logger.warning("Telegram sticker thumbnail has unknown format, sending original: filename=%s", filename)

        telegram_file = await self._telegram_call(self.bot.get_file, sticker.file_id)
        return bytes(await telegram_file.download_as_bytearray()), filename, mime_type, width, height

    @staticmethod
    def _image_mime_from_bytes(data: bytes) -> Optional[str]:
        if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
            return "image/webp"
        if data[:3] == b"\xff\xd8\xff":
            return "image/jpeg"
        if data[:8] == b"\x89PNG\r\n\x1a\n":
            return "image/png"
        return None

    @staticmethod
    def _telegram_sticker_meta(sticker) -> tuple[str, str, int, int]:
        unique_id = str(getattr(sticker, "file_unique_id", "sticker") or "sticker")
        if getattr(sticker, "is_video", False):
            extension = "webm"
            mime_type = "video/webm"
        elif getattr(sticker, "is_animated", False):
            extension = "tgs"
            mime_type = "application/x-tgsticker"
        else:
            extension = "webp"
            mime_type = "image/webp"

        filename = f"telegram-sticker-{unique_id}.{extension}"
        width = int(getattr(sticker, "width", 0) or 0)
        height = int(getattr(sticker, "height", 0) or 0)
        return filename, mime_type, width, height

    def status_html(self) -> str:
        status = "running" if self.messenger.is_running else "stopped"
        err = self.messenger.last_error
        err_line = f"\nlast_error: <code>{escape_html(str(err))}</code>" if err else ""
        return (
            "<b>Messenger bridge status</b>\n"
            f"listener: <code>{status}</code>\n"
            f"facebook_id: <code>{escape_html(self.messenger.self_id or '-')}</code>\n"
            f"topics: <code>{len(self.store.all_topics())}</code>"
            f"{err_line}"
        )

    def topic_list_html(self) -> str:
        topics = self.store.all_topics()
        if not topics:
            return "No Messenger topics have been linked yet."
        lines = ["<b>Messenger topics</b>"]
        for entry in topics:
            lines.append(
                f"- <b>{escape_html(truncate(entry.name, 80))}</b> "
                f"topic=<code>{entry.topic_id}</code> "
                f"transport=<code>{entry.transport}</code> "
                f"id=<code>{escape_html(truncate(entry.messenger_id, 80))}</code>"
            )
        return truncate("\n".join(lines), 4096)

    async def topic_permissions_html(self) -> str:
        assert self.bot is not None
        chat = await self._telegram_call(self.bot.get_chat, self.config.telegram_group_id)
        me = await self._telegram_call(self.bot.get_me)
        member = await self._telegram_call(self.bot.get_chat_member, self.config.telegram_group_id, me.id)

        raw_chat_type = getattr(chat, "type", "")
        chat_type = getattr(raw_chat_type, "value", str(raw_chat_type))
        raw_status = getattr(member, "status", "")
        status = getattr(raw_status, "value", str(raw_status))
        is_forum = bool(getattr(chat, "is_forum", False))
        is_admin = status in {"administrator", "creator", "owner"}
        can_manage_topics = status in {"creator", "owner"} or bool(getattr(member, "can_manage_topics", False))

        lines = [
            "<b>Telegram topic check</b>",
            f"chat_type: <code>{escape_html(chat_type)}</code>",
            f"is_forum: <code>{str(is_forum).lower()}</code>",
            f"bot_status: <code>{escape_html(status)}</code>",
            f"can_manage_topics: <code>{str(can_manage_topics).lower()}</code>",
            "",
        ]

        problems: list[str] = []
        if chat_type != "supergroup":
            problems.append("Group must be a supergroup.")
        if not is_forum:
            problems.append("Enable Topics in Telegram group settings.")
        if not is_admin:
            problems.append("Promote the bot to admin.")
        if is_admin and not can_manage_topics:
            problems.append("Grant the bot the Manage Topics permission.")

        if problems:
            lines.append("<b>Needs fixing</b>")
            lines.extend(f"- {escape_html(problem)}" for problem in problems)
        else:
            lines.append("<b>OK</b>: bot can create forum topics automatically.")

        return "\n".join(lines)

    def topic_info_html(self, topic_id: int) -> str:
        entry = self.store.get_topic_by_id(topic_id)
        if entry is None:
            return "This topic is not linked to a Messenger conversation."
        return (
            f"<b>{escape_html(entry.name)}</b>\n"
            f"topic: <code>{entry.topic_id}</code>\n"
            f"transport: <code>{entry.transport}</code>\n"
            f"messenger_id: <code>{escape_html(entry.messenger_id)}</code>\n"
            f"thread_id: <code>{escape_html(entry.thread_id or '-')}</code>\n"
            f"chat_jid: <code>{escape_html(entry.chat_jid or '-')}</code>"
        )

    def delete_topic_mapping_html(self, topic_id: int) -> str:
        removed = self.store.remove_topic(topic_id)
        if removed is None:
            return "This topic is not linked to a Messenger conversation."
        logger.info(
            "Topic mapping deleted: topic_id=%s transport=%s messenger_id=%s thread_id=%s chat_jid=%s name=%s",
            removed.topic_id,
            removed.transport,
            removed.messenger_id,
            removed.thread_id or "-",
            removed.chat_jid or "-",
            removed.name or "-",
        )
        return f"Deleted mapping for <b>{escape_html(removed.name)}</b>."

    @staticmethod
    def _is_topic_missing_error(exc: TelegramError) -> bool:
        text = str(exc).lower()
        return (
            "message thread not found" in text
            or "thread not found" in text
            or "topic_closed" in text
            or "topic closed" in text
        )

    async def _notify_general(self, text: str) -> None:
        if self.bot is None:
            return
        try:
            logger.info("Telegram notify general: %s", _preview(text))
            await self._telegram_call(
                self.bot.send_message,
                chat_id=self.config.telegram_group_id,
                text=escape_html(text),
                parse_mode=ParseMode.HTML,
            )
        except TelegramError as exc:
            logger.warning("Telegram notification failed: %s", exc)

    @staticmethod
    async def _telegram_call(fn, *args, **kwargs):
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                logger.debug("Telegram API call: fn=%s attempt=%s", getattr(fn, "__name__", fn), attempt + 1)
                return await fn(*args, **kwargs)
            except RetryAfter as exc:
                if attempt == max_attempts - 1:
                    raise
                logger.warning("Telegram RetryAfter calling %s: retry_after=%s", getattr(fn, "__name__", fn), exc.retry_after)
                await asyncio.sleep(float(exc.retry_after) + 1.0)
            except TimedOut as exc:
                if attempt == max_attempts - 1:
                    raise
                delay = 2.0 + attempt
                logger.warning("Telegram timed out calling %s: %s. Retrying in %.1fs", getattr(fn, "__name__", fn), exc, delay)
                await asyncio.sleep(delay)
            except NetworkError as exc:
                if attempt == max_attempts - 1:
                    raise
                delay = min(10.0, 2.0 + (attempt * 2.0))
                logger.warning("Telegram network error calling %s: %s. Retrying in %.1fs", getattr(fn, "__name__", fn), exc, delay)
                await asyncio.sleep(delay)
