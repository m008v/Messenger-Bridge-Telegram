from __future__ import annotations

import json
import time
from dataclasses import asdict
from pathlib import Path
from threading import RLock
from typing import Any, Optional

from models import QuoteData, TopicEntry, Transport


class BridgeStore:
    def __init__(self, path: Path, *, message_cache_limit: int = 3000) -> None:
        self.path = path
        self.message_cache_limit = message_cache_limit
        self._lock = RLock()
        self._data: dict[str, Any] = {
            "version": 1,
            "topics": {},
            "messenger_index": {},
            "messages": {},
            "message_order": [],
            "tg_quotes": {},
        }
        self._load()

    @staticmethod
    def _messenger_key(transport: Transport, messenger_id: str) -> str:
        return f"{transport}:{messenger_id}"

    @staticmethod
    def _message_key(transport: Transport, message_id: str) -> str:
        return f"{transport}:{message_id}"

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            loaded = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return
        if isinstance(loaded, dict):
            self._data.update({
                "topics": loaded.get("topics") or {},
                "messenger_index": loaded.get("messenger_index") or {},
                "messages": loaded.get("messages") or {},
                "message_order": loaded.get("message_order") or [],
                "tg_quotes": loaded.get("tg_quotes") or {},
            })

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._data, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self.path)

    def all_topics(self) -> list[TopicEntry]:
        with self._lock:
            return [TopicEntry(**entry) for entry in self._data["topics"].values()]

    def get_topic_by_messenger(self, transport: Transport, messenger_id: str) -> Optional[TopicEntry]:
        with self._lock:
            topic_id = self._data["messenger_index"].get(self._messenger_key(transport, messenger_id))
            if topic_id is None:
                return None
            raw = self._data["topics"].get(str(topic_id))
            return TopicEntry(**raw) if raw else None

    def get_topic_by_id(self, topic_id: int) -> Optional[TopicEntry]:
        with self._lock:
            raw = self._data["topics"].get(str(topic_id))
            return TopicEntry(**raw) if raw else None

    def set_topic(self, entry: TopicEntry) -> TopicEntry:
        with self._lock:
            now = int(time.time())
            existing = self._data["topics"].get(str(entry.topic_id))
            if existing and not entry.created_at:
                entry.created_at = int(existing.get("created_at") or now)
            elif not entry.created_at:
                entry.created_at = now
            entry.updated_at = now

            self._data["topics"][str(entry.topic_id)] = asdict(entry)
            self._data["messenger_index"][self._messenger_key(entry.transport, entry.messenger_id)] = entry.topic_id
            self._save()
            return entry

    def update_topic_name(self, topic_id: int, name: str) -> Optional[TopicEntry]:
        with self._lock:
            raw = self._data["topics"].get(str(topic_id))
            if not raw:
                return None
            raw["name"] = name
            raw["updated_at"] = int(time.time())
            self._save()
            return TopicEntry(**raw)

    def remove_topic(self, topic_id: int) -> Optional[TopicEntry]:
        with self._lock:
            raw = self._data["topics"].pop(str(topic_id), None)
            if not raw:
                return None
            entry = TopicEntry(**raw)
            key = self._messenger_key(entry.transport, entry.messenger_id)
            if self._data["messenger_index"].get(key) == topic_id:
                self._data["messenger_index"].pop(key, None)
            self._save()
            return entry

    def save_message(
        self,
        tg_message_id: int,
        messenger_message_ids: list[str],
        quote: QuoteData,
    ) -> None:
        valid_ids = [mid for mid in messenger_message_ids if mid]
        if not valid_ids:
            return

        with self._lock:
            quote_dict = asdict(quote)
            self._data["tg_quotes"][str(tg_message_id)] = quote_dict
            for message_id in valid_ids:
                key = self._message_key(quote.transport, message_id)
                self._data["messages"][key] = {
                    "tg_message_id": tg_message_id,
                    "quote": quote_dict,
                }
                if key not in self._data["message_order"]:
                    self._data["message_order"].append(key)
            self._evict_messages()
            self._save()

    def has_message(self, messenger_message_id: str) -> bool:
        if not messenger_message_id:
            return False
        with self._lock:
            return any(
                self._message_key(transport, messenger_message_id) in self._data["messages"]
                for transport in ("e2ee", "regular")
            )

    def get_tg_message_id(self, transport: Transport, messenger_message_id: str) -> Optional[int]:
        with self._lock:
            raw = self._data["messages"].get(self._message_key(transport, messenger_message_id))
            if not raw:
                return None
            return int(raw["tg_message_id"])

    def get_quote_by_messenger(self, transport: Transport, messenger_message_id: str) -> Optional[QuoteData]:
        with self._lock:
            raw = self._data["messages"].get(self._message_key(transport, messenger_message_id))
            if not raw:
                return None
            quote = raw.get("quote") or {}
            return QuoteData(**quote) if quote else None

    def find_quote_by_messenger(self, messenger_message_id: str) -> Optional[QuoteData]:
        with self._lock:
            for transport in ("e2ee", "regular"):
                raw = self._data["messages"].get(self._message_key(transport, messenger_message_id))
                if not raw:
                    continue
                quote = raw.get("quote") or {}
                return QuoteData(**quote) if quote else None
            return None

    def get_quote_by_tg(self, tg_message_id: int) -> Optional[QuoteData]:
        with self._lock:
            raw = self._data["tg_quotes"].get(str(tg_message_id))
            return QuoteData(**raw) if raw else None

    def _evict_messages(self) -> None:
        order = self._data["message_order"]
        while len(order) > self.message_cache_limit:
            old_key = order.pop(0)
            old = self._data["messages"].pop(old_key, None)
            if old:
                self._data["tg_quotes"].pop(str(old.get("tg_message_id")), None)