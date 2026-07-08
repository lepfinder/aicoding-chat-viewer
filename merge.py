"""合并 Antigravity SQLite 与 Cursor 只读数据源。"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from config import IDE_DATA_DIR
from cursor_reader import CursorReader, is_cursor_conversation_id
from parser import enrich_messages_images


def _sort_key(item: Dict[str, Any]) -> str:
    return item.get("updated_at") or item.get("created_at") or item.get("id") or ""


def _empty_bucket(path: str) -> Dict[str, Any]:
    return {
        "workspace_path": path,
        "cnt": 0,
        "message_count": 0,
        "user_message_count": 0,
        "ok_count": 0,
        "last_updated": None,
        "ag_cnt": 0,
        "cursor_cnt": 0,
    }


def merge_stats(ag_stats: Dict[str, Any], cursor_stats: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "total": ag_stats.get("total", 0) + cursor_stats.get("total", 0),
        "ok": ag_stats.get("ok", 0) + cursor_stats.get("ok", 0),
        "encrypted_only": ag_stats.get("encrypted_only", 0),
        "messages": ag_stats.get("messages", 0) + cursor_stats.get("messages", 0),
        "user_messages": ag_stats.get("user_messages", 0) + cursor_stats.get("user_messages", 0),
        "cursor_total": cursor_stats.get("total", 0),
        "antigravity_total": ag_stats.get("total", 0),
    }


def merge_workspaces(
    ag_workspaces: List[Dict[str, Any]],
    cursor_workspaces: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    buckets: Dict[str, Dict[str, Any]] = {}

    for ws in ag_workspaces:
        path = ws.get("workspace_path") or ""
        bucket = buckets.setdefault(path, _empty_bucket(path))
        bucket["cnt"] += ws.get("cnt", 0)
        bucket["message_count"] += ws.get("message_count", 0) or 0
        bucket["user_message_count"] += ws.get("user_message_count", 0) or 0
        bucket["ok_count"] += ws.get("ok_count", ws.get("cnt", 0))
        bucket["ag_cnt"] += ws.get("cnt", 0)
        ts = ws.get("last_updated")
        if ts and (not bucket["last_updated"] or ts > bucket["last_updated"]):
            bucket["last_updated"] = ts

    for ws in cursor_workspaces:
        path = ws.get("workspace_path") or ""
        bucket = buckets.setdefault(path, _empty_bucket(path))
        bucket["cnt"] += ws.get("cnt", 0)
        bucket["message_count"] += ws.get("message_count", 0) or 0
        bucket["user_message_count"] += ws.get("user_message_count", 0) or 0
        bucket["ok_count"] += ws.get("ok_count", ws.get("cnt", 0))
        bucket["cursor_cnt"] += ws.get("cnt", 0)
        ts = ws.get("last_updated")
        if ts and (not bucket["last_updated"] or ts > bucket["last_updated"]):
            bucket["last_updated"] = ts

    rows = list(buckets.values())
    rows.sort(key=lambda x: (x.get("last_updated") is None, x.get("last_updated") or ""), reverse=True)
    return rows


def tag_antigravity_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    tagged = []
    for row in rows:
        item = dict(row)
        item["source_app"] = "antigravity"
        tagged.append(item)
    return tagged


def merge_conversations(
    ag_conversations: List[Dict[str, Any]],
    cursor_conversations: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    rows = tag_antigravity_rows(ag_conversations) + cursor_conversations
    rows.sort(key=_sort_key, reverse=True)
    return rows


def get_conversation(
    storage,
    cursor: CursorReader,
    conversation_id: str,
) -> Optional[Dict[str, Any]]:
    if is_cursor_conversation_id(conversation_id):
        return cursor.get_conversation(conversation_id)
    conv = storage.get_conversation(conversation_id)
    if conv:
        conv = dict(conv)
        conv["source_app"] = "antigravity"
    return conv


def get_messages(
    storage,
    cursor: CursorReader,
    conversation_id: str,
) -> List[Dict[str, Any]]:
    if is_cursor_conversation_id(conversation_id):
        return cursor.list_messages(conversation_id)
    messages = storage.list_messages(conversation_id)
    enrich_messages_images(messages, conversation_id, IDE_DATA_DIR)
    return messages
