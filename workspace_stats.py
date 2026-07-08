"""工作目录维度统计与日历热点图数据。"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from cursor_reader import CursorReader
from storage import Storage


def _parse_day(value: str | None) -> Optional[str]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except ValueError:
        return value[:10] if len(value) >= 10 else None


def _merge_daily_counts(*maps: Dict[str, int]) -> Dict[str, int]:
    merged: Dict[str, int] = defaultdict(int)
    for m in maps:
        for day, count in m.items():
            if day:
                merged[day] += count
    return dict(merged)


def _storage_daily_activity(storage: Storage, workspace_path: str) -> Dict[str, int]:
    sql = """
        SELECT substr(m.created_at, 1, 10) AS day, COUNT(*) AS cnt
        FROM messages m
        JOIN conversations c ON c.id = m.conversation_id
        WHERE c.workspace_path = ?
          AND m.role = 'user'
          AND m.created_at IS NOT NULL
          AND length(m.created_at) >= 10
        GROUP BY day
    """
    with storage.connect() as conn:
        rows = conn.execute(sql, (workspace_path,)).fetchall()
    return {row["day"]: row["cnt"] for row in rows if row["day"]}


def _cursor_daily_activity(cursor: CursorReader, workspace_path: str) -> Dict[str, int]:
    counts: Dict[str, int] = defaultdict(int)
    for conv in cursor.list_conversations(workspace_path):
        day = _parse_day(conv.get("updated_at") or conv.get("created_at"))
        if day:
            user_cnt = int(conv.get("user_message_count") or 1)
            counts[day] += max(user_cnt, 1)
    return dict(counts)


def _level_for_count(count: int, max_count: int) -> int:
    if count <= 0 or max_count <= 0:
        return 0
    ratio = count / max_count
    if ratio <= 0.25:
        return 1
    if ratio <= 0.5:
        return 2
    if ratio <= 0.75:
        return 3
    return 4


def build_heatmap(daily: Dict[str, int], days: int = 371) -> Dict[str, Any]:
    """生成 GitHub 风格周列日历（含星期对齐）。"""
    end = date.today()
    start = end - timedelta(days=days - 1)
    start -= timedelta(days=(start.weekday() + 1) % 7)

    max_count = max(daily.values()) if daily else 0
    cells: List[Dict[str, Any]] = []
    current = start
    while current <= end:
        key = current.isoformat()
        count = daily.get(key, 0)
        cells.append(
            {
                "date": key,
                "count": count,
                "level": _level_for_count(count, max_count),
            }
        )
        current += timedelta(days=1)

    weeks: List[List[Dict[str, Any]]] = []
    for i in range(0, len(cells), 7):
        weeks.append(cells[i : i + 7])

    month_labels: List[Dict[str, str]] = []
    last_month = ""
    for wi, week in enumerate(weeks):
        if not week:
            continue
        month = week[0]["date"][5:7]
        if month != last_month:
            month_labels.append({"week_index": wi, "label": f"{int(month)}月"})
            last_month = month

    return {
        "weeks": weeks,
        "month_labels": month_labels,
        "max_count": max_count,
        "total_days_active": sum(1 for c in cells if c["count"] > 0),
    }


def build_workspace_stats(
    workspace_path: str,
    storage: Storage,
    cursor: CursorReader,
    conversations: List[Dict[str, Any]],
) -> Dict[str, Any]:
    ag_convs = [c for c in conversations if c.get("source_app") != "cursor"]
    cursor_convs = [c for c in conversations if c.get("source_app") == "cursor"]

    total_conversations = len(conversations)
    user_messages = sum(int(c.get("user_message_count") or 0) for c in conversations)
    all_messages = sum(int(c.get("message_count") or 0) for c in conversations)

    dates: List[str] = []
    for c in conversations:
        for key in ("created_at", "updated_at"):
            day = _parse_day(c.get(key))
            if day:
                dates.append(day)

    ag_daily = _storage_daily_activity(storage, workspace_path) if ag_convs else {}
    cursor_daily = (
        _cursor_daily_activity(cursor, workspace_path) if cursor.available() and cursor_convs else {}
    )
    daily = _merge_daily_counts(ag_daily, cursor_daily)
    heatmap = build_heatmap(daily)

    peak_day = max(daily.items(), key=lambda x: x[1]) if daily else None
    recent = sorted(
        conversations,
        key=lambda c: c.get("updated_at") or c.get("created_at") or "",
        reverse=True,
    )[:8]

    return {
        "workspace_path": workspace_path,
        "workspace_short": workspace_path.rstrip("/").split("/")[-1] if workspace_path else "未分类",
        "conversation_count": total_conversations,
        "ag_conversation_count": len(ag_convs),
        "cursor_conversation_count": len(cursor_convs),
        "user_message_count": user_messages,
        "message_count": all_messages,
        "first_active": min(dates) if dates else None,
        "last_active": max(dates) if dates else None,
        "active_days": heatmap["total_days_active"],
        "peak_day": {"date": peak_day[0], "count": peak_day[1]} if peak_day else None,
        "daily": daily,
        "heatmap": heatmap,
        "recent_conversations": [
            {
                "id": c["id"],
                "title": c.get("title") or c["id"],
                "source_app": c.get("source_app", "antigravity"),
                "updated_at": c.get("updated_at"),
                "user_message_count": c.get("user_message_count", 0),
            }
            for c in recent
        ],
    }


def stats_summary_for_ai(stats: Dict[str, Any]) -> str:
    lines = [
        f"工作目录: {stats.get('workspace_path') or '未分类'}",
        f"会话总数: {stats.get('conversation_count')} (AG {stats.get('ag_conversation_count')} / Cursor {stats.get('cursor_conversation_count')})",
        f"用户消息: {stats.get('user_message_count')}，全部消息: {stats.get('message_count')}",
        f"活跃区间: {stats.get('first_active') or '—'} ~ {stats.get('last_active') or '—'}",
        f"有活动的天数: {stats.get('active_days')}",
    ]
    peak = stats.get("peak_day")
    if peak:
        lines.append(f"最活跃日: {peak['date']} ({peak['count']} 条用户消息)")
    recent = stats.get("recent_conversations") or []
    if recent:
        lines.append("近期会话标题:")
        for item in recent[:5]:
            lines.append(f"- {item.get('title')}")
    return "\n".join(lines)
