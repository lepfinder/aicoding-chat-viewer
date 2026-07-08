"""会话详情与消息列表的进程内 LRU 缓存。"""

from __future__ import annotations

from collections import OrderedDict
from typing import Any, Dict, List, Optional


class ConversationCache:
    def __init__(self, max_entries: int = 120) -> None:
        self.max_entries = max_entries
        self._entries: OrderedDict[str, Dict[str, Any]] = OrderedDict()

    def get(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        if not conversation_id:
            return None
        item = self._entries.get(conversation_id)
        if item is None:
            return None
        self._entries.move_to_end(conversation_id)
        return item

    def put(self, conversation_id: str, conv: Dict[str, Any], messages: List[Dict[str, Any]]) -> None:
        if not conversation_id:
            return
        self._entries[conversation_id] = {
            "conv": conv,
            "messages": messages,
        }
        self._entries.move_to_end(conversation_id)
        while len(self._entries) > self.max_entries:
            self._entries.popitem(last=False)

    def clear(self) -> None:
        self._entries.clear()

    def stats(self) -> Dict[str, int]:
        return {"cached_conversations": len(self._entries)}
