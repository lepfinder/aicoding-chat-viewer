"""按工作目录收集全部用户消息（AG + Cursor）。"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from cursor_reader import CursorReader, is_cursor_conversation_id
from merge import get_messages
from storage import Storage

_NOISE_EXACT = {
    "继续",
    "好的",
    "可以",
    "执行",
    "执行吧",
    "嗯",
    "好",
    "ok",
    "OK",
    "行",
    "去吧",
    "开始吧",
    "谢谢",
    "感谢",
}


def _parse_sort_key(created_at: str | None) -> str:
    if not created_at:
        return ""
    return created_at


def is_noise_message(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return True
    if t in _NOISE_EXACT:
        return True
    if len(t) <= 2:
        return True
    return False


def _normalize_user_row(
    content: str,
    created_at: str | None,
    conversation_id: str,
    conversation_title: str,
    source: str,
) -> Dict[str, Any]:
    text = (content or "").strip()
    return {
        "content": text,
        "created_at": created_at,
        "conversation_id": conversation_id,
        "conversation_title": conversation_title or conversation_id,
        "source": source,
    }


def list_ag_user_messages(storage: Storage, workspace_path: str) -> List[Dict[str, Any]]:
    sql = """
        SELECT m.content, m.created_at, m.conversation_id, c.title AS conversation_title
        FROM messages m
        JOIN conversations c ON c.id = m.conversation_id
        WHERE c.workspace_path = ? AND m.role = 'user' AND m.content IS NOT NULL
        ORDER BY m.created_at, m.id
    """
    with storage.connect() as conn:
        rows = conn.execute(sql, (workspace_path,)).fetchall()
    return [
        _normalize_user_row(
            row["content"],
            row["created_at"],
            row["conversation_id"],
            row["conversation_title"],
            "antigravity",
        )
        for row in rows
    ]


def list_cursor_user_messages(
    storage: Storage,
    cursor: CursorReader,
    workspace_path: str,
) -> List[Dict[str, Any]]:
    if not cursor.available():
        return []

    rows: List[Dict[str, Any]] = []
    for conv in cursor.list_conversations(workspace_path):
        cid = conv["id"]
        title = conv.get("title") or cid
        for msg in get_messages(storage, cursor, cid):
            if msg.get("role") != "user":
                continue
            content = (msg.get("content") or "").strip()
            if not content:
                continue
            rows.append(
                _normalize_user_row(
                    content,
                    msg.get("created_at"),
                    cid,
                    title,
                    "cursor",
                )
            )
    return rows


def list_workspace_user_messages(
    storage: Storage,
    cursor: CursorReader,
    workspace_path: str,
    *,
    drop_noise: bool = True,
) -> List[Dict[str, Any]]:
    rows = list_ag_user_messages(storage, workspace_path)
    rows.extend(list_cursor_user_messages(storage, cursor, workspace_path))
    rows.sort(key=lambda r: _parse_sort_key(r.get("created_at")))

    if not drop_noise:
        return rows

    seen: set[str] = set()
    filtered: List[Dict[str, Any]] = []
    for row in rows:
        text = row["content"]
        if is_noise_message(text):
            continue
        key = text[:200]
        if key in seen and len(text) < 40:
            continue
        seen.add(key)
        filtered.append(row)
    return filtered


def message_fingerprint(messages: List[Dict[str, Any]]) -> str:
    import hashlib

    if not messages:
        return ""
    parts = [f"{m.get('created_at','')}|{m.get('conversation_id','')}|{m['content'][:80]}" for m in messages]
    digest = hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()
    return digest[:16]


def message_prefix_fingerprint(messages: List[Dict[str, Any]], count: int) -> str:
    """前 count 条消息的锚点指纹，用于增量提取时校验历史未变。"""
    if count <= 0:
        return ""
    return message_fingerprint(messages[:count])


def resolve_anchor_message_count(
    messages: List[Dict[str, Any]],
    cached: Dict[str, Any],
) -> int:
    """解析锚点对应的消息条数（message_fingerprint 所覆盖的前缀长度）。"""
    anchor_fp = (cached or {}).get("message_fingerprint") or ""
    if not anchor_fp or not messages:
        return 0

    stored = int((cached or {}).get("anchor_message_count") or 0)
    if stored > 0 and message_prefix_fingerprint(messages, stored) == anchor_fp:
        return stored

    candidates: List[int] = []
    for value in (
        stored,
        int((cached or {}).get("last_extracted_message_count") or 0),
        int((cached or {}).get("user_message_count") or 0),
        len(messages),
    ):
        if value > 0 and value not in candidates:
            candidates.append(value)

    for count in sorted(candidates, reverse=True):
        if message_prefix_fingerprint(messages, count) == anchor_fp:
            return count

    for count in range(len(messages), 0, -1):
        if message_prefix_fingerprint(messages, count) == anchor_fp:
            return count
    return 0
