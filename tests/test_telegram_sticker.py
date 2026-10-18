from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from bridge import MessengerTelegramBridge  # noqa: E402
from store import BridgeStore  # noqa: E402
from test_async_fbchat_client import make_config  # noqa: E402

WEBP = b"RIFF\x10\x00\x00\x00WEBPVP8 fake"
JPEG = b"\xff\xd8\xff\xe0fake-jpeg"
TGS = b"\x1f\x8bfake-gzipped-lottie"


def fake_bot(files: dict[str, bytes]) -> SimpleNamespace:
    async def get_file(file_id):
        return SimpleNamespace(download_as_bytearray=AsyncMock(return_value=bytearray(files[file_id])))

    return SimpleNamespace(get_file=get_file)


def sticker(*, animated: bool = False, video: bool = False, thumbnail=None) -> SimpleNamespace:
    return SimpleNamespace(
        file_id="sticker-file",
        file_unique_id="UNIQ",
        is_animated=animated,
        is_video=video,
        width=512,
        height=512,
        thumbnail=thumbnail,
    )


class TelegramStickerDownloadTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = BridgeStore(Path(tmp.name) / "bridge-store.json")
        messenger = SimpleNamespace(self_id="1")
        self.bridge = MessengerTelegramBridge(make_config(), store, messenger)

    async def download(self, item, files):
        self.bridge.bot = fake_bot(files)
        meta = self.bridge._telegram_sticker_meta(item)
        return await self.bridge._download_telegram_sticker(item, *meta)

    async def test_animated_sticker_uses_static_webp_thumbnail(self) -> None:
        thumb = SimpleNamespace(file_id="thumb-file", width=128, height=128)
        item = sticker(animated=True, thumbnail=thumb)

        data, filename, mime, width, height = await self.download(
            item, {"sticker-file": TGS, "thumb-file": WEBP}
        )

        self.assertEqual((data, filename, mime, width, height), (WEBP, "telegram-sticker-UNIQ.webp", "image/webp", 128, 128))

    async def test_animated_sticker_with_jpeg_thumbnail(self) -> None:
        thumb = SimpleNamespace(file_id="thumb-file", width=100, height=90)

        data, filename, mime, _, _ = await self.download(
            sticker(animated=True, thumbnail=thumb), {"sticker-file": TGS, "thumb-file": JPEG}
        )

        self.assertEqual((data, filename, mime), (JPEG, "telegram-sticker-UNIQ.jpeg", "image/jpeg"))

    async def test_animated_sticker_without_thumbnail_keeps_original(self) -> None:
        data, filename, mime, _, _ = await self.download(sticker(animated=True), {"sticker-file": TGS})

        self.assertEqual((data, filename, mime), (TGS, "telegram-sticker-UNIQ.tgs", "application/x-tgsticker"))

    async def test_unknown_thumbnail_format_keeps_original(self) -> None:
        thumb = SimpleNamespace(file_id="thumb-file", width=128, height=128)

        data, _, mime, _, _ = await self.download(
            sticker(animated=True, thumbnail=thumb), {"sticker-file": TGS, "thumb-file": b"not-an-image"}
        )

        self.assertEqual((data, mime), (TGS, "application/x-tgsticker"))

    async def test_static_sticker_is_sent_as_is(self) -> None:
        thumb = SimpleNamespace(file_id="thumb-file", width=128, height=128)

        data, filename, mime, width, _ = await self.download(
            sticker(thumbnail=thumb), {"sticker-file": WEBP, "thumb-file": JPEG}
        )

        self.assertEqual((data, filename, mime, width), (WEBP, "telegram-sticker-UNIQ.webp", "image/webp", 512))


class E2EEStickerRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def test_non_webp_image_goes_through_e2ee_image(self) -> None:
        from messenger.client import MessengerClient
        from models import TopicEntry

        client = MessengerClient(make_config())
        client.listener = object()
        client._call_bridge_send = AsyncMock(return_value={"payload": {}})
        entry = TopicEntry(topic_id=1, messenger_id="1@msgr", transport="e2ee", name="x", chat_jid="1@msgr")

        await client.send_telegram_sticker(entry, JPEG, "s.jpeg", "image/jpeg", 100, 90)

        method, params = client._call_bridge_send.await_args.args
        self.assertEqual(method, "sendE2EEImage")
        self.assertEqual((params["mimeType"], params["width"], params["height"]), ("image/jpeg", 100, 90))


if __name__ == "__main__":
    unittest.main()
