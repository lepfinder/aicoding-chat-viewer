"""研发 Blocks + 总报告生成管线。"""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import date, datetime
from typing import Any, Dict, Iterator, List, Optional, Tuple

from ai_client import chat_json, chat_text
from config import OPENAI_MAX_TOKENS_JSON, OPENAI_MAX_TOKENS_REPORT
from cursor_reader import CursorReader
from storage import Storage
from user_messages import (
    list_workspace_user_messages,
    message_fingerprint,
    message_prefix_fingerprint,
    resolve_anchor_message_count,
)
from workspace_stats import build_workspace_stats, stats_summary_for_ai

logger = logging.getLogger(__name__)

# 每批送入 LLM 的用户消息条数（超过则走分批 extract → merge）
BATCH_SIZE = 80
BATCH_MSG_MAX_CHARS = 320

_BLOCKS_SCHEMA = """
{
  "blocks": [
    {
      "id": "blk-1",
      "type": "module",
      "title": "功能/模块名称（中文）",
      "summary": "2-3句话说明做了什么、解决什么问题",
      "start_date": "YYYY-MM-DD 或 null",
      "end_date": "YYYY-MM-DD 或 null",
      "status": "completed|ongoing|paused|unknown",
      "keywords": ["关键词1", "关键词2"],
      "evidence": [
        {
          "date": "YYYY-MM-DD 或 null",
          "conversation_title": "会话标题",
          "snippet": "用户原话摘要，不超过120字"
        }
      ],
      "confidence": 0.0,
      "child_fine_ids": ["细粒度 block 的 title 或 id"]
    }
  ]
}
"""

_PARTIAL_BLOCKS_SCHEMA = """
{
  "blocks": [
    {
      "title": "功能/模块名称",
      "summary": "本批消息中该模块做了什么",
      "type": "module|feature",
      "keywords": ["关键词"],
      "start_date": "YYYY-MM-DD 或 null",
      "end_date": "YYYY-MM-DD 或 null",
      "evidence": [
        {
          "date": "YYYY-MM-DD 或 null",
          "conversation_title": "会话标题",
          "snippet": "用户原话摘要"
        }
      ]
    }
  ]
}
"""


def _parse_day(value: str | None) -> Optional[str]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return dt.date().isoformat()
    except ValueError:
        return value[:10] if value and len(value) >= 10 else None


def _iso_week(day: str) -> str:
    d = date.fromisoformat(day)
    year, week, _ = d.isocalendar()
    return f"{year}-W{week:02d}"


def build_weekly_chunks(
    messages: List[Dict[str, Any]],
    max_samples: int = 10,
    max_sample_len: int = 220,
) -> List[Dict[str, Any]]:
    if len(messages) > 800:
        max_samples, max_sample_len = 5, 140
    elif len(messages) > 300:
        max_samples, max_sample_len = 7, 180
    buckets: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for msg in messages:
        day = _parse_day(msg.get("created_at"))
        if not day:
            buckets["unknown"].append(msg)
            continue
        buckets[_iso_week(day)].append(msg)

    chunks: List[Dict[str, Any]] = []
    for period in sorted(buckets.keys()):
        items = buckets[period]
        days = [_parse_day(m.get("created_at")) for m in items]
        days = [d for d in days if d]
        titles = list(dict.fromkeys(m.get("conversation_title") or "" for m in items if m.get("conversation_title")))
        samples: List[str] = []
        for msg in items:
            text = msg["content"].replace("\n", " ").strip()
            if len(text) > max_sample_len:
                text = text[: max_sample_len - 1] + "…"
            label = msg.get("conversation_title") or ""
            samples.append(f"[{label}] {text}")
            if len(samples) >= max_samples:
                break
        chunks.append(
            {
                "period": period,
                "date_range": [min(days), max(days)] if days else [None, None],
                "message_count": len(items),
                "conversation_titles": titles[:12],
                "samples": samples,
            }
        )
    return chunks


def split_message_batches(
    messages: List[Dict[str, Any]],
    batch_size: int = BATCH_SIZE,
) -> List[Dict[str, Any]]:
    batches: List[Dict[str, Any]] = []
    current: List[Dict[str, Any]] = []
    for msg in messages:
        current.append(msg)
        if len(current) >= batch_size:
            batches.append(_batch_meta(current, len(batches)))
            current = []
    if current:
        batches.append(_batch_meta(current, len(batches)))
    return batches


def split_incremental_batches(
    messages: List[Dict[str, Any]],
    *,
    start_message_index: int,
    start_batch_index: int,
    batch_size: int = BATCH_SIZE,
) -> List[Dict[str, Any]]:
    pending = messages[start_message_index:]
    if not pending:
        return []
    batches: List[Dict[str, Any]] = []
    current: List[Dict[str, Any]] = []
    batch_index = start_batch_index
    for msg in pending:
        current.append(msg)
        if len(current) >= batch_size:
            batches.append(_batch_meta(current, batch_index))
            batch_index += 1
            current = []
    if current:
        batches.append(_batch_meta(current, batch_index))
    return batches


def _batch_meta(msgs: List[Dict[str, Any]], index: int) -> Dict[str, Any]:
    days = [_parse_day(m.get("created_at")) for m in msgs]
    days = [d for d in days if d]
    return {
        "batch_index": index,
        "message_count": len(msgs),
        "date_range": [min(days), max(days)] if days else [None, None],
        "messages": msgs,
    }


def _format_batch_messages(
    msgs: List[Dict[str, Any]],
    max_chars: int = BATCH_MSG_MAX_CHARS,
) -> str:
    rows: List[Dict[str, str]] = []
    for msg in msgs:
        text = (msg.get("content") or "").replace("\n", " ").strip()
        if len(text) > max_chars:
            text = text[: max_chars - 1] + "…"
        rows.append(
            {
                "date": _parse_day(msg.get("created_at")) or "",
                "conversation_title": (msg.get("conversation_title") or "")[:120],
                "text": text,
            }
        )
    return json.dumps(rows, ensure_ascii=False, indent=2)


def _slim_blocks_for_merge(blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    slim: List[Dict[str, Any]] = []
    for raw in blocks:
        if not isinstance(raw, dict):
            continue
        evidence = raw.get("evidence") or []
        if not isinstance(evidence, list):
            evidence = []
        slim.append(
            {
                "id": raw.get("id") or "",
                "title": (raw.get("title") or "")[:120],
                "summary": (raw.get("summary") or "")[:200],
                "type": raw.get("type") or "module",
                "keywords": [str(k)[:40] for k in (raw.get("keywords") or [])[:6]],
                "start_date": raw.get("start_date"),
                "end_date": raw.get("end_date"),
                "evidence": [
                    {
                        "date": ev.get("date") if isinstance(ev, dict) else None,
                        "conversation_title": (
                            (ev.get("conversation_title") or "")[:80] if isinstance(ev, dict) else ""
                        ),
                        "snippet": ((ev.get("snippet") or "")[:100] if isinstance(ev, dict) else ""),
                    }
                    for ev in evidence[:2]
                    if isinstance(ev, dict)
                ],
            }
        )
    return slim


def _extract_batch_blocks(
    batch: Dict[str, Any],
    workspace_path: str,
) -> List[Dict[str, Any]]:
    batch_no = batch["batch_index"] + 1
    date_range = batch.get("date_range") or [None, None]
    prompt = f"""从以下「一批」用户消息中提炼局部研发 Blocks（仅覆盖本批，不要臆测全项目）。

要求：
- 产出 2～8 个 block，每个代表本批消息中的功能模块或能力点
- type 优先 "module"，细粒度用 "feature"
- 同主题合并为一个 block
- evidence 必须从下方消息选取，不要编造会话标题
- summary、snippet 各不超过 80 字
- 仅输出 JSON，不要分析过程或 markdown 列表

项目：{workspace_path or '未分类'}
批次：第 {batch_no} 批
时间范围：{date_range[0] or '—'} ~ {date_range[1] or '—'}
本批消息数：{batch['message_count']}

用户消息（JSON）：
{_format_batch_messages(batch['messages'])}

输出结构：
{_PARTIAL_BLOCKS_SCHEMA}
"""
    data = chat_json(
        messages=[
            {
                "role": "system",
                "content": "你只输出合法 JSON 对象，含 blocks 数组。禁止输出解释文字。",
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
        max_tokens=OPENAI_MAX_TOKENS_JSON,
        label=f"blocks-batch-{batch_no}",
        retries=2,
    )
    blocks = data.get("blocks")
    if not isinstance(blocks, list):
        return []
    return blocks


def _merge_partial_blocks(
    partial_blocks: List[Dict[str, Any]],
    stats: Dict[str, Any],
    workspace_path: str,
    message_count: int,
    batch_count: int,
) -> List[Dict[str, Any]]:
    slim = _slim_blocks_for_merge(partial_blocks)
    prompt = f"""你是研发过程分析助手。请将多批「局部 Blocks」合并为一份全局研发模块 Blocks。

要求：
- 产出 5～15 个 block，以功能模块/能力点为主线
- 跨批同主题必须合并，不要把每批当成一个 block
- 时间 start_date/end_date 取覆盖范围，不确定填 null
- 数字以统计数据为准，不要编造
- 仅输出 JSON

项目：{workspace_path or '未分类'}
用户消息总数：{message_count}
局部分批数：{batch_count}
局部 block 条目数：{len(slim)}

项目统计：
{stats_summary_for_ai(stats)}

局部 Blocks JSON：
{json.dumps(slim, ensure_ascii=False, indent=2)}

输出结构：
{_BLOCKS_SCHEMA}
"""
    data = chat_json(
        messages=[
            {
                "role": "system",
                "content": "你只输出合法 JSON 对象，含 blocks 数组。禁止输出解释文字。",
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
        max_tokens=OPENAI_MAX_TOKENS_JSON,
        label="blocks-merge",
        retries=2,
    )
    blocks = data.get("blocks")
    if not isinstance(blocks, list):
        raise ValueError("合并阶段 LLM 返回的 blocks 格式无效")
    return _normalize_modules(blocks)


def _normalize_fine_block(
    raw: Dict[str, Any],
    *,
    batch_index: int,
    seq: int,
) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        raw = {}
    block_type = raw.get("type") or "feature"
    if block_type not in ("module", "feature", "phase"):
        block_type = "feature"
    status = raw.get("status") or "unknown"
    if status not in ("completed", "ongoing", "paused", "unknown"):
        status = "unknown"
    evidence = raw.get("evidence") or []
    if not isinstance(evidence, list):
        evidence = []
    clean_evidence = []
    for ev in evidence[:5]:
        if not isinstance(ev, dict):
            continue
        clean_evidence.append(
            {
                "date": ev.get("date"),
                "conversation_title": (ev.get("conversation_title") or "")[:120],
                "snippet": (ev.get("snippet") or "")[:200],
            }
        )
    keywords = raw.get("keywords") or []
    if not isinstance(keywords, list):
        keywords = []
    return {
        "id": raw.get("id") or f"fine-{batch_index + 1}-{seq + 1}",
        "batch_index": batch_index,
        "type": block_type,
        "title": (raw.get("title") or f"功能 {seq + 1}")[:120],
        "summary": (raw.get("summary") or "")[:800],
        "start_date": raw.get("start_date"),
        "end_date": raw.get("end_date"),
        "status": status,
        "keywords": [str(k)[:40] for k in keywords[:8]],
        "evidence": clean_evidence,
        "confidence": float(raw.get("confidence") or 0.7),
    }


def _normalize_modules(blocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for i, raw in enumerate(blocks):
        if not isinstance(raw, dict):
            continue
        block_type = raw.get("type") or "module"
        if block_type not in ("module", "feature", "phase"):
            block_type = "module"
        status = raw.get("status") or "unknown"
        if status not in ("completed", "ongoing", "paused", "unknown"):
            status = "unknown"
        evidence = raw.get("evidence") or []
        if not isinstance(evidence, list):
            evidence = []
        clean_evidence = []
        for ev in evidence[:5]:
            if not isinstance(ev, dict):
                continue
            clean_evidence.append(
                {
                    "date": ev.get("date"),
                    "conversation_title": (ev.get("conversation_title") or "")[:120],
                    "snippet": (ev.get("snippet") or "")[:200],
                }
            )
        keywords = raw.get("keywords") or []
        if not isinstance(keywords, list):
            keywords = []
        child_ids = raw.get("child_fine_ids") or []
        if not isinstance(child_ids, list):
            child_ids = []
        normalized.append(
            {
                "id": raw.get("id") or f"mod-{i + 1}",
                "type": block_type,
                "title": (raw.get("title") or f"模块 {i + 1}")[:120],
                "summary": (raw.get("summary") or "")[:800],
                "start_date": raw.get("start_date"),
                "end_date": raw.get("end_date"),
                "status": status,
                "keywords": [str(k)[:40] for k in keywords[:8]],
                "evidence": clean_evidence,
                "confidence": float(raw.get("confidence") or 0.7),
                "child_fine_ids": [str(x)[:120] for x in child_ids[:30]],
            }
        )
    return normalized


_normalize_blocks = _normalize_modules


def generate_blocks(
    workspace_path: str,
    stats: Dict[str, Any],
    messages: List[Dict[str, Any]],
    storage: Storage,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], int]:
    """返回 (blocks_fine, blocks_modules, llm_calls)。"""
    fine: List[Dict[str, Any]] = []
    modules: List[Dict[str, Any]] = []
    llm_calls = 0
    for event in iter_blocks_pipeline(workspace_path, stats, messages, storage):
        if event.get("type") == "done":
            fine = event.get("blocks_fine") or []
            modules = event.get("blocks_modules") or []
            llm_calls = int(event.get("llm_calls") or 0)
    return fine, modules, llm_calls


def _anchor_history_error(
    messages: List[Dict[str, Any]],
    cached: Dict[str, Any],
) -> Optional[str]:
    anchor_fp = (cached or {}).get("message_fingerprint") or ""
    if not anchor_fp:
        return None
    anchor_count = resolve_anchor_message_count(messages, cached)
    if anchor_count <= 0:
        return "历史消息已变化，请使用「重新提取」"
    if message_prefix_fingerprint(messages, anchor_count) != anchor_fp:
        return "历史消息已变化，请使用「重新提取」"
    return None


def _prepare_extract_state(
    storage: Storage,
    workspace_path: str,
    fingerprint: str,
    cached: Dict[str, Any],
    *,
    force: bool,
) -> None:
    anchor_fp = cached.get("message_fingerprint") or ""
    last_extracted = int(cached.get("last_extracted_message_count") or 0)

    if force:
        if anchor_fp:
            storage.clear_blocks_fine(workspace_path, anchor_fp)
            storage.clear_blocks_modules(workspace_path, anchor_fp)
        elif fingerprint:
            storage.clear_blocks_fine(workspace_path, fingerprint)
            storage.clear_blocks_modules(workspace_path, fingerprint)
        return

    if last_extracted > 0 and anchor_fp:
        return

    if anchor_fp and anchor_fp != fingerprint:
        if storage.list_blocks_fine(workspace_path, anchor_fp) or storage.list_blocks_modules(
            workspace_path, anchor_fp
        ):
            return
        storage.clear_blocks_fine(workspace_path, anchor_fp)
        storage.clear_blocks_modules(workspace_path, anchor_fp)


def _plan_extract_batches(
    messages: List[Dict[str, Any]],
    storage: Storage,
    workspace_path: str,
    cached: Dict[str, Any],
    *,
    force: bool,
) -> Tuple[str, List[Dict[str, Any]], int, int, Optional[str], int]:
    """返回 anchor_fp, 待处理 batches, total_batches, message_cursor, error, anchor_message_count。"""
    message_count = len(messages)
    current_fp = message_fingerprint(messages)

    if force:
        batches = split_message_batches(messages)
        return current_fp, batches, len(batches), 0, None, message_count

    last_extracted = int(cached.get("last_extracted_message_count") or 0)
    anchor_fp = cached.get("message_fingerprint") or ""
    completed_indices = (
        set(storage.get_completed_batch_indices(workspace_path, anchor_fp))
        if anchor_fp
        else set()
    )

    if last_extracted <= 0 and completed_indices and anchor_fp:
        last_extracted = int(cached.get("user_message_count") or message_count)

    if last_extracted > 0 and anchor_fp:
        if last_extracted > message_count:
            return anchor_fp, [], 0, last_extracted, "用户消息数量减少，请使用「重新提取」", 0
        anchor_count = int(cached.get("anchor_message_count") or 0)
        if anchor_count <= 0:
            anchor_count = resolve_anchor_message_count(messages, cached)
        if anchor_count <= 0:
            anchor_count = last_extracted
        if message_prefix_fingerprint(messages, anchor_count) != anchor_fp:
            return anchor_fp, [], 0, last_extracted, "历史消息已变化，请使用「重新提取」", 0
        pending = messages[last_extracted:]
        max_batch = storage.get_max_batch_index(workspace_path, anchor_fp)
        total_batches = (max_batch + 1) if max_batch is not None else len(completed_indices)
        if not pending:
            return anchor_fp, [], total_batches, last_extracted, None, anchor_count
        start_batch = (max_batch + 1) if max_batch is not None else 0
        batches = split_incremental_batches(
            messages,
            start_message_index=last_extracted,
            start_batch_index=start_batch,
        )
        total_batches = start_batch + len(batches)
        return anchor_fp, batches, total_batches, last_extracted, None, anchor_count

    batches = split_message_batches(messages)
    return current_fp, batches, len(batches), 0, None, message_count


def iter_extract_pipeline(
    workspace_path: str,
    stats: Dict[str, Any],
    messages: List[Dict[str, Any]],
    storage: Storage,
    *,
    cached: Optional[Dict[str, Any]] = None,
    fingerprint: str = "",
    force: bool = False,
) -> Iterator[Dict[str, Any]]:
    if not messages:
        yield {"type": "error", "error": "该项目没有可用的用户消息，无法提取 Blocks"}
        return

    cached = cached or {}
    current_fp = fingerprint or message_fingerprint(messages)
    _prepare_extract_state(storage, workspace_path, current_fp, cached, force=force)

    anchor_fp, batches, total_batches, message_cursor, plan_error, anchor_message_count = _plan_extract_batches(
        messages, storage, workspace_path, cached, force=force
    )
    if plan_error:
        yield {"type": "error", "error": plan_error}
        return

    if anchor_message_count > 0:
        storage.ensure_anchor_message_count(workspace_path, anchor_message_count)

    storage.repair_analysis_state(
        workspace_path,
        message_fingerprint=current_fp,
        total_batches=total_batches,
        user_message_count=len(messages),
    )
    completed = set(storage.get_completed_batch_indices(workspace_path, anchor_fp))

    if not batches:
        fine = storage.list_blocks_fine(workspace_path, anchor_fp)
        if fine and message_cursor >= len(messages):
            yield {
                "type": "cached",
                "detail": (
                    f"提取已完成：{len(completed)}/{total_batches} 批，"
                    f"{len(fine)} 个细粒度 block"
                ),
            }
            yield {
                "type": "done",
                "blocks_fine": fine,
                "blocks_modules": [],
                "blocks": [],
                "from_cache": True,
                "llm_calls": 0,
                "stage": "extract",
            }
            return

    incremental = message_cursor > 0 or (
        int(cached.get("last_extracted_message_count") or 0) > 0 and not force
    )
    logger.info(
        "消息量 %d，%s提取 %d 批（锚点已完成 %d/%d 批，消息水位 %d）",
        len(messages),
        "增量" if incremental else "全量",
        len(batches),
        len(completed),
        total_batches,
        message_cursor,
    )
    yield {
        "type": "progress",
        "stage": "collect",
        "detail": (
            f"共 {len(messages)} 条用户消息，"
            f"{'增量' if incremental else '全量'}待处理 {len(batches)} 批"
            f"（已完成 {len(completed)}/{total_batches} 批）"
        ),
        "current": len(completed),
        "total": total_batches,
    }

    llm_calls = 0
    for batch in batches:
        batch_index = batch["batch_index"]
        batch_no = batch_index + 1
        date_range = batch.get("date_range") or [None, None]

        if batch_index in completed and not force:
            message_cursor += batch["message_count"]
            yield {
                "type": "progress",
                "stage": "batch",
                "detail": f"跳过第 {batch_no}/{total_batches} 批（已入库）",
                "current": batch_no,
                "total": total_batches,
                "skipped": True,
            }
            continue

        yield {
            "type": "progress",
            "stage": "batch",
            "detail": f"第 {batch_no}/{total_batches} 批（{date_range[0] or '—'} ~ {date_range[1] or '—'}）",
            "current": batch_no,
            "total": total_batches,
        }
        logger.info(
            "批次 %d/%d 开始 messages=%d",
            batch_no,
            total_batches,
            batch["message_count"],
        )
        partial = _extract_batch_blocks(batch, workspace_path)
        fine_batch: List[Dict[str, Any]] = []
        for i, p in enumerate(partial):
            fine_batch.append(
                _normalize_fine_block(
                    p,
                    batch_index=batch_index,
                    seq=batch_index * 100 + i,
                )
            )
        message_cursor += batch["message_count"]
        storage.save_blocks_fine_batch(
            workspace_path,
            anchor_fp,
            batch_index,
            fine_batch,
            len(messages),
            total_batches,
            extracted_message_count=message_cursor,
            current_message_fingerprint=current_fp,
            anchor_message_count=anchor_message_count,
        )
        completed.add(batch_index)
        llm_calls += 1
        fine_total = len(storage.list_blocks_fine(workspace_path, anchor_fp))
        logger.info(
            "批次 %d/%d 完成 fine_blocks=%d 累计=%d 消息水位=%d",
            batch_no,
            total_batches,
            len(fine_batch),
            fine_total,
            message_cursor,
        )
        yield {
            "type": "batch_saved",
            "stage": "batch",
            "detail": f"第 {batch_no}/{total_batches} 批已入库（+{len(fine_batch)}，累计 {fine_total}）",
            "current": batch_no,
            "total": total_batches,
            "batch_index": batch_index,
            "batch_block_count": len(fine_batch),
            "fine_total": fine_total,
        }

    fine_all = storage.list_blocks_fine(workspace_path, anchor_fp)
    if not fine_all:
        yield {"type": "error", "error": "各批次均未产出有效的细粒度 Blocks"}
        return

    yield {
        "type": "done",
        "blocks_fine": fine_all,
        "blocks_modules": [],
        "blocks": [],
        "from_cache": False,
        "llm_calls": llm_calls,
        "stage": "extract",
    }


def _pending_message_hint(messages: List[Dict[str, Any]], cached: Dict[str, Any]) -> str:
    message_count = len(messages)
    last_extracted = int((cached or {}).get("last_extracted_message_count") or 0)
    pending = max(0, message_count - last_extracted)
    if pending <= 0:
        return ""
    return f"（尚有 {pending} 条新消息未纳入分析）"


def iter_merge_pipeline(
    workspace_path: str,
    stats: Dict[str, Any],
    messages: List[Dict[str, Any]],
    storage: Storage,
    *,
    cached: Optional[Dict[str, Any]] = None,
    fingerprint: str = "",
    force: bool = False,
) -> Iterator[Dict[str, Any]]:
    cached = cached or {}
    message_count = len(messages)
    anchor_fp = cached.get("message_fingerprint") or fingerprint
    last_extracted = int(cached.get("last_extracted_message_count") or 0)
    pending_hint = _pending_message_hint(messages, cached)

    history_error = _anchor_history_error(messages, cached)
    if history_error:
        yield {"type": "error", "error": history_error}
        return

    anchor_count = resolve_anchor_message_count(messages, cached)
    if anchor_count > 0:
        storage.ensure_anchor_message_count(workspace_path, anchor_count)

    if last_extracted <= 0:
        batches = split_message_batches(messages)
        total_batches = len(batches)
        completed = set(storage.get_completed_batch_indices(workspace_path, anchor_fp))
        if total_batches > 0 and len(completed) < total_batches:
            yield {
                "type": "error",
                "error": (
                    f"提取未完成（{len(completed)}/{total_batches} 批），"
                    "请先点击「提取 Blocks」或「继续提取」"
                ),
            }
            return

    fine_all = storage.list_blocks_fine(workspace_path, anchor_fp)
    if not fine_all:
        yield {"type": "error", "error": "没有可合并的细粒度 Blocks，请先完成提取"}
        return

    modules_cached = storage.list_blocks_modules(workspace_path, anchor_fp)
    cached_umc = int(cached.get("user_message_count") or 0)
    pending = max(0, message_count - last_extracted)
    if (
        not force
        and modules_cached
        and pending == 0
        and last_extracted >= message_count
        and cached_umc >= message_count
    ):
        yield {
            "type": "cached",
            "detail": f"命中模块缓存：{len(modules_cached)} 个模块",
        }
        yield {
            "type": "done",
            "blocks_fine": fine_all,
            "blocks_modules": modules_cached,
            "blocks": modules_cached,
            "from_cache": True,
            "llm_calls": 0,
            "stage": "merge",
        }
        return

    if force:
        storage.clear_blocks_modules(workspace_path, anchor_fp)

    max_batch = storage.get_max_batch_index(workspace_path, anchor_fp)
    total_batches = (max_batch + 1) if max_batch is not None else 0

    yield {
        "type": "progress",
        "stage": "merge",
        "detail": f"合并 {len(fine_all)} 个细粒度 block 为模块总览{pending_hint}",
        "current": 0,
        "total": 1,
    }
    logger.info("合并 %d 个细粒度 block → 模块总览", len(fine_all))
    modules = _merge_partial_blocks(
        fine_all,
        stats,
        workspace_path,
        len(messages),
        total_batches,
    )
    storage.save_blocks_modules(
        workspace_path,
        anchor_fp,
        modules,
        clear_report=True,
    )
    yield {
        "type": "done",
        "blocks_fine": fine_all,
        "blocks_modules": modules,
        "blocks": modules,
        "from_cache": False,
        "llm_calls": 1,
        "stage": "merge",
    }


def iter_blocks_pipeline(
    workspace_path: str,
    stats: Dict[str, Any],
    messages: List[Dict[str, Any]],
    storage: Storage,
    *,
    cached: Optional[Dict[str, Any]] = None,
    fingerprint: str = "",
    force: bool = False,
) -> Iterator[Dict[str, Any]]:
    """兼容：依次执行 extract + merge。"""
    extract_done: Dict[str, Any] = {}
    for event in iter_extract_pipeline(
        workspace_path,
        stats,
        messages,
        storage,
        cached=cached,
        fingerprint=fingerprint,
        force=force,
    ):
        if event.get("type") == "error":
            yield event
            return
        if event.get("type") in ("progress", "batch_saved", "cached"):
            yield event
        if event.get("type") == "done":
            extract_done = event

    if not extract_done:
        yield {"type": "error", "error": "提取未完成"}
        return

    for event in iter_merge_pipeline(
        workspace_path,
        stats,
        messages,
        storage,
        cached=storage.get_workspace_analysis(workspace_path),
        fingerprint=fingerprint,
        force=force,
    ):
        yield event


def iter_report_pipeline(
    workspace_path: str,
    stats: Dict[str, Any],
    messages: List[Dict[str, Any]],
    *,
    cached: Optional[Dict[str, Any]] = None,
    fingerprint: str = "",
    force: bool = False,
) -> Iterator[Dict[str, Any]]:
    modules = (cached or {}).get("blocks_modules") or (cached or {}).get("blocks") or []
    if not modules:
        yield {"type": "error", "error": "请先生成研发 Blocks，再生成报告"}
        return

    message_count = len(messages)
    last_extracted = int((cached or {}).get("last_extracted_message_count") or 0)
    pending_hint = _pending_message_hint(messages, cached or {})
    cached_umc = int((cached or {}).get("user_message_count") or 0)

    if (
        not force
        and cached
        and cached.get("report_md")
        and cached_umc >= message_count
        and last_extracted >= message_count
    ):
        yield {"type": "cached", "detail": "命中报告缓存"}
        yield {
            "type": "done",
            "report_md": cached.get("report_md") or "",
            "blocks_modules": modules,
            "blocks": modules,
            "from_cache": True,
            "llm_calls": 0,
        }
        return

    yield {
        "type": "progress",
        "stage": "report",
        "detail": f"基于 {len(modules)} 个模块撰写报告{pending_hint}",
        "current": 0,
        "total": 1,
    }
    logger.info("调用 LLM 生成 Markdown 报告 modules=%d", len(modules))
    report_md = generate_report_markdown(workspace_path, stats, modules)
    logger.info("报告 Markdown 完成 chars=%d", len(report_md))
    yield {
        "type": "done",
        "report_md": report_md,
        "blocks_modules": modules,
        "blocks": modules,
        "from_cache": False,
        "llm_calls": 1,
    }


def generate_report_markdown(
    workspace_path: str,
    stats: Dict[str, Any],
    blocks: List[Dict[str, Any]],
) -> str:
    blocks_json = json.dumps(blocks, ensure_ascii=False, indent=2)
    prompt = f"""你是技术写作助手。请基于「研发 Blocks」和「项目统计数据」撰写一份中文研发过程报告（Markdown）。

要求：
1. 以功能模块为主线叙述研发进程，时间轴为辅
2. 结构必须包含以下二级标题（##）：
   - 项目概览
   - 研发模块总览
   - 模块与功能演进（按 block 逐个简述，引用其 title 与 summary）
   - 研发节奏与投入（结合活跃天数、峰值日等统计，不要编造数字）
   - 总结与后续建议
3. 不要编造 blocks 和统计中不存在的内容
4. 语气专业简洁，适合开发者复盘

项目：{workspace_path or '未分类'}

统计数据：
{stats_summary_for_ai(stats)}

Blocks JSON：
{blocks_json}
"""

    return chat_text(
        messages=[
            {"role": "system", "content": "你输出结构清晰的 Markdown 中文报告。"},
            {"role": "user", "content": prompt},
        ],
        temperature=0.35,
        max_tokens=OPENAI_MAX_TOKENS_REPORT,
        label="report_md",
    )


def _load_report_context(
    workspace_path: str,
    storage: Storage,
    cursor: CursorReader,
    conversations: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]], str, Dict[str, Any]]:
    stats = build_workspace_stats(workspace_path, storage, cursor, conversations)
    messages = list_workspace_user_messages(storage, cursor, workspace_path)
    fingerprint = message_fingerprint(messages)
    cached = storage.get_workspace_analysis(workspace_path) or {}
    return stats, messages, fingerprint, cached


def _analysis_payload(
    workspace_path: str,
    storage: Storage,
    fingerprint: str,
    messages: List[Dict[str, Any]],
    *,
    blocks_fine: Optional[List[Dict[str, Any]]] = None,
    blocks_modules: Optional[List[Dict[str, Any]]] = None,
    report_md: Optional[str] = None,
    from_cache: bool = False,
    llm_calls: int = 0,
    stage: str = "blocks",
) -> Dict[str, Any]:
    saved = storage.get_workspace_analysis(workspace_path) or {}
    return {
        **saved,
        "workspace_path": workspace_path,
        "message_fingerprint": fingerprint,
        "user_message_count": len(messages) or saved.get("user_message_count", 0),
        "blocks_fine": blocks_fine if blocks_fine is not None else saved.get("blocks_fine") or [],
        "blocks_modules": blocks_modules if blocks_modules is not None else saved.get("blocks_modules") or [],
        "blocks": blocks_modules if blocks_modules is not None else saved.get("blocks_modules") or saved.get("blocks") or [],
        "report_md": report_md if report_md is not None else saved.get("report_md") or "",
        "from_cache": from_cache,
        "llm_called": not from_cache and llm_calls > 0,
        "llm_calls": llm_calls,
        "stage": stage,
    }


def run_extract_report(
    workspace_path: str,
    storage: Storage,
    cursor: CursorReader,
    conversations: List[Dict[str, Any]],
    *,
    force: bool = False,
) -> Dict[str, Any]:
    logger.info("Blocks 提取开始 workspace=%s force=%s", workspace_path, force)
    stats, messages, fingerprint, cached = _load_report_context(
        workspace_path, storage, cursor, conversations
    )
    result: Dict[str, Any] = {}
    for event in iter_extract_pipeline(
        workspace_path,
        stats,
        messages,
        storage,
        cached=cached,
        fingerprint=fingerprint,
        force=force,
    ):
        if event.get("type") == "error":
            raise ValueError(event.get("error") or "Blocks 提取失败")
        if event.get("type") == "done":
            result = event
    if not result:
        raise ValueError("Blocks 提取未完成")
    return _analysis_payload(
        workspace_path,
        storage,
        fingerprint,
        messages,
        blocks_fine=result.get("blocks_fine") or [],
        from_cache=bool(result.get("from_cache")),
        llm_calls=int(result.get("llm_calls") or 0),
        stage="extract",
    )


def run_merge_report(
    workspace_path: str,
    storage: Storage,
    cursor: CursorReader,
    conversations: List[Dict[str, Any]],
    *,
    force: bool = False,
) -> Dict[str, Any]:
    logger.info("Blocks 合并开始 workspace=%s force=%s", workspace_path, force)
    stats, messages, fingerprint, cached = _load_report_context(
        workspace_path, storage, cursor, conversations
    )
    result: Dict[str, Any] = {}
    for event in iter_merge_pipeline(
        workspace_path,
        stats,
        messages,
        storage,
        cached=cached,
        fingerprint=fingerprint,
        force=force,
    ):
        if event.get("type") == "error":
            raise ValueError(event.get("error") or "Blocks 合并失败")
        if event.get("type") == "done":
            result = event
    if not result:
        raise ValueError("Blocks 合并未完成")
    return _analysis_payload(
        workspace_path,
        storage,
        fingerprint,
        messages,
        blocks_fine=result.get("blocks_fine") or [],
        blocks_modules=result.get("blocks_modules") or [],
        from_cache=bool(result.get("from_cache")),
        llm_calls=int(result.get("llm_calls") or 0),
        stage="merge",
    )


def run_blocks_report(
    workspace_path: str,
    storage: Storage,
    cursor: CursorReader,
    conversations: List[Dict[str, Any]],
    *,
    force: bool = False,
) -> Dict[str, Any]:
    logger.info("Blocks 生成开始 workspace=%s force=%s", workspace_path, force)
    stats, messages, fingerprint, cached = _load_report_context(
        workspace_path, storage, cursor, conversations
    )
    logger.info(
        "消息收集完成 user_messages=%d fingerprint=%s",
        len(messages),
        fingerprint[:16],
    )

    result: Dict[str, Any] = {}
    for event in iter_blocks_pipeline(
        workspace_path,
        stats,
        messages,
        storage,
        cached=cached,
        fingerprint=fingerprint,
        force=force,
    ):
        if event.get("type") == "error":
            raise ValueError(event.get("error") or "Blocks 生成失败")
        if event.get("type") == "done":
            result = event

    if not result:
        raise ValueError("Blocks 生成未完成")

    return _analysis_payload(
        workspace_path,
        storage,
        fingerprint,
        messages,
        blocks_fine=result.get("blocks_fine") or [],
        blocks_modules=result.get("blocks_modules") or [],
        from_cache=bool(result.get("from_cache")),
        llm_calls=int(result.get("llm_calls") or 0),
        stage="blocks",
    )


def run_report_only(
    workspace_path: str,
    storage: Storage,
    cursor: CursorReader,
    conversations: List[Dict[str, Any]],
    *,
    force: bool = False,
) -> Dict[str, Any]:
    logger.info("报告生成开始 workspace=%s force=%s", workspace_path, force)
    stats, messages, fingerprint, cached = _load_report_context(
        workspace_path, storage, cursor, conversations
    )

    result: Dict[str, Any] = {}
    for event in iter_report_pipeline(
        workspace_path,
        stats,
        messages,
        cached=cached,
        fingerprint=fingerprint,
        force=force,
    ):
        if event.get("type") == "error":
            raise ValueError(event.get("error") or "报告生成失败")
        if event.get("type") == "done":
            result = event

    if not result:
        raise ValueError("报告生成未完成")

    if result.get("from_cache"):
        return _analysis_payload(
            workspace_path,
            storage,
            fingerprint,
            messages,
            from_cache=True,
            llm_calls=0,
            stage="report",
        )

    report_md = result.get("report_md") or ""
    storage.save_analysis_report(workspace_path, fingerprint, report_md, len(messages))
    return _analysis_payload(
        workspace_path,
        storage,
        fingerprint,
        messages,
        report_md=report_md,
        from_cache=False,
        llm_calls=1,
        stage="report",
    )


def run_full_report(
    workspace_path: str,
    storage: Storage,
    cursor: CursorReader,
    conversations: List[Dict[str, Any]],
    *,
    force: bool = False,
) -> Dict[str, Any]:
    blocks_result = run_blocks_report(
        workspace_path, storage, cursor, conversations, force=force
    )
    if blocks_result.get("report_md") and not force and blocks_result.get("from_cache"):
        cached = storage.get_workspace_analysis(workspace_path) or {}
        if cached.get("report_md") and cached.get("message_fingerprint") == blocks_result.get("message_fingerprint"):
            return {**blocks_result, "stage": "full", "from_cache": True, "llm_called": False}

    report_result = run_report_only(
        workspace_path, storage, cursor, conversations, force=force
    )
    return {
        **report_result,
        "blocks_fine": blocks_result.get("blocks_fine") or [],
        "stage": "full",
        "llm_calls": (blocks_result.get("llm_calls") or 0) + (report_result.get("llm_calls") or 0),
        "from_cache": bool(blocks_result.get("from_cache") and report_result.get("from_cache")),
        "llm_called": bool(blocks_result.get("llm_called") or report_result.get("llm_called")),
    }
