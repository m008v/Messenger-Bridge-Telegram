from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from bridge import MessengerTelegramBridge  # noqa: E402
from models import IncomingMessengerMessage, QuoteData, TopicEntry  # noqa: E402
from store import BridgeStore  # noqa: E402
from test_async_fbchat_client import make_config  # noqa: E402

SELF_ID = "100000000000001"
FRIEND_ID = "100000000000002"
CHAT_JID = "100000000000002@msgr"
GROUP_ID = -100123
TOPIC_ID = 42


def incoming(text: str, *, message_id: str, sender_id: str = SELF_ID) -> IncomingMessengerMessage:
    return IncomingMessengerMessage(
        transport="e2ee",
        messenger_id=CHAT_JID,
        thread_id="",
        chat_jid=CHAT_JID,
        sender_id=sender_id,
        sender_jid=None,
        sender_name="",
        text=text,
        message_id=message_id,
    )


def telegram_text(text: str, *, message_id: int = 7) -> SimpleNamespace:
    return SimpleNamespace(
        chat_id=GROUP_ID,
        from_user=SimpleNamespace(is_bot=False, full_name="Tester"),
        message_thread_id=TOPIC_ID,
        reply_to_message=None,
        sticker=None,
        text=text,
        caption=None,
        message_id=message_id,
    )


class SelfMessageDedupeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = BridgeStore(Path(self.tmp.name) / "bridge-store.json", message_cache_limit=100)
        self.entry = self.store.set_topic(TopicEntry(
            topic_id=TOPIC_ID,
            messenger_id=CHAT_JID,
            transport="e2ee",
            name="Friend",
            chat_jid=CHAT_JID,
        ))
        self.messenger = SimpleNamespace(
            self_id=SELF_ID,
            send_text=AsyncMock(return_value={"payload": {"messageID": "sent-mid"}}),
            self_sender_jid=lambda transport: f"{SELF_ID}@msgr",
        )
        config = make_config(ignore_self_messages=False, telegram_group_id=GROUP_ID)
        self.bridge = MessengerTelegramBridge(config, self.store, self.messenger)
        self.bridge._enrich_sender_name = AsyncMock()
        self.bridge._clear_typing_for_message = AsyncMock()
        self.bridge._get_or_create_topic = AsyncMock(return_value=self.entry)
        self.bridge._send_inbound_with_topic_recovery = AsyncMock(
            return_value=SimpleNamespace(message_id=99, message_thread_id=TOPIC_ID)
        )

    async def deliver(self, message: IncomingMessengerMessage) -> None:
        with patch("bridge.parse_messenger_event", return_value=message):
            await self.bridge.handle_messenger_event({"type": "e2eeMessage"})

    @property
    def forwarded(self) -> int:
        return self.bridge._send_inbound_with_topic_recovery.await_count

    async def test_self_message_sent_from_phone_is_forwarded(self) -> None:
        await self.deliver(incoming("hello from my phone", message_id="phone-mid"))

        self.assertEqual(self.forwarded, 1)

    async def test_echo_after_send_returns_is_skipped(self) -> None:
        await self.bridge.forward_telegram_message(telegram_text("hi there"))

        await self.deliver(incoming("hi there", message_id="sent-mid"))

        self.assertEqual(self.forwarded, 0)

    async def test_echo_while_send_in_flight_is_skipped(self) -> None:
        async def send_and_echo(entry, text, quote):
            await self.deliver(incoming(text, message_id="sent-mid"))
            return {"payload": {"messageID": "sent-mid"}}

        self.messenger.send_text.side_effect = send_and_echo

        await self.bridge.forward_telegram_message(telegram_text("racing echo"))

        self.assertEqual(self.forwarded, 0)
        self.assertEqual(self.bridge._pending_outbound, {})

    async def test_different_self_text_during_send_is_forwarded(self) -> None:
        async def send_while_phone_types(entry, text, quote):
            await self.deliver(incoming("typed on phone", message_id="phone-mid"))
            return {"payload": {"messageID": "sent-mid"}}

        self.messenger.send_text.side_effect = send_while_phone_types

        await self.bridge.forward_telegram_message(telegram_text("from telegram"))

        self.assertEqual(self.forwarded, 1)

    async def test_pending_send_is_released_when_send_fails(self) -> None:
        self.messenger.send_text.side_effect = RuntimeError("boom")

        result = await self.bridge.forward_telegram_message(telegram_text("will fail"))

        self.assertFalse(result.ok)
        self.assertEqual(self.bridge._pending_outbound, {})
        await self.deliver(incoming("will fail", message_id="phone-mid"))
        self.assertEqual(self.forwarded, 1)

    async def test_redelivered_incoming_message_is_skipped(self) -> None:
        message = incoming("hey", message_id="friend-mid", sender_id=FRIEND_ID)

        await self.deliver(message)
        await self.deliver(message)

        self.assertEqual(self.forwarded, 1)

    async def test_ignore_self_messages_still_drops_all_self_messages(self) -> None:
        self.bridge.config = make_config(ignore_self_messages=True, telegram_group_id=GROUP_ID)

        await self.deliver(incoming("hello from my phone", message_id="phone-mid"))

        self.assertEqual(self.forwarded, 0)


class StoreHasMessageTests(unittest.TestCase):
    def test_has_message_checks_both_transports(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = BridgeStore(Path(tmp) / "bridge-store.json")
            store.save_message(5, ["regular-mid"], QuoteData(message_id="regular-mid", transport="regular", messenger_id="1"))

            self.assertTrue(store.has_message("regular-mid"))
            self.assertFalse(store.has_message("other-mid"))
            self.assertFalse(store.has_message(""))


if __name__ == "__main__":
    unittest.main()
