#!/usr/bin/env python3
"""Antigravity + Cursor 历史会话本地 Web 浏览服务（纯 Python 服务端渲染）。"""

from __future__ import annotations

import argparse
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterator
from urllib.parse import quote, unquote
from zoneinfo import ZoneInfo

from flask import Flask, abort, flash, jsonify, redirect, render_template, request, send_file, url_for

from ai_client import ai_available
from analysis_stream import format_sse, sse_response
from config import CURSOR_DB_PATH, DB_PATH, HOST, IDE_DATA_DIR, LEGACY_DATA_DIR, PORT
from parser import resolve_ag_image_path
from conversation_cache import ConversationCache
from cursor_reader import CursorReader
from merge import (
    get_conversation,
    get_messages,
    merge_conversations,
    merge_stats,
    merge_workspaces,
)
from storage import (
    ANALYSIS_STATUS_EXTRACT,
    ANALYSIS_STATUS_MERGE,
    ANALYSIS_STATUS_REPORT,
    Storage,
)
from sync import run_sync
from workspace_stats import build_workspace_stats
from report_pipeline import (
    _load_report_context,
    iter_extract_pipeline,
    iter_merge_pipeline,
    iter_report_pipeline,
    run_blocks_report,
    run_extract_report,
    run_full_report,
    run_merge_report,
    run_report_only,
    split_message_batches,
)

logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = "antigravity-chat-viewer-local"
storage = Storage(DB_PATH)
cursor_reader = CursorReader()
conversation_cache = ConversationCache()

DISPLAY_TZ = ZoneInfo("Asia/Shanghai")


def _format_display_time(value: str | None, *, with_seconds: bool = False) -> str:
    if not value:
        return "—"
    fmt = "%Y-%m-%d %H:%M:%S" if with_seconds else "%Y-%m-%d %H:%M"
    fallback_len = 19 if with_seconds else 16
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(DISPLAY_TZ).strftime(fmt)
    except ValueError:
        return value[:fallback_len] if value else "—"


@app.template_filter("fmt_time")
def fmt_time(value: str | None) -> str:
    return _format_display_time(value, with_seconds=False)


@app.template_filter("fmt_msg_time")
def fmt_msg_time(value: str | None) -> str:
    return _format_display_time(value, with_seconds=True)


@app.template_filter("role_label")
def role_label(role: str) -> str:
    return {
        "user": "用户",
        "assistant": "AI",
        "assistant_tool": "工具",
        "system": "系统",
        "tool_event": "事件",
        "other": "其他",
    }.get(role, role)


@app.template_filter("urlencode_path")
def urlencode_path(value: str | None) -> str:
    return quote(value or "", safe="")


@app.template_filter("workspace_label")
def workspace_label(value: str | None) -> str:
    if not value:
        return "未分类"
    return value


@app.template_filter("workspace_short")
def workspace_short(value: str | None) -> str:
    if not value:
        return "未分类"
    parts = value.rstrip("/").split("/")
    return parts[-1] if parts else value


@app.template_filter("block_type_label")
def block_type_label(value: str | None) -> str:
    return {"module": "模块", "feature": "功能", "phase": "阶段"}.get(value or "", value or "模块")


@app.template_filter("block_status_label")
def block_status_label(value: str | None) -> str:
    return {
        "completed": "已完成",
        "ongoing": "进行中",
        "paused": "暂停",
        "unknown": "未知",
    }.get(value or "", value or "未知")


@app.template_filter("source_label")
def source_label(source_app: str | None) -> str:
    if source_app == "cursor":
        return "Cursor"
    return "AG"


def _combined_stats() -> dict:
    ag_stats = storage.stats()
    cursor_stats = cursor_reader.stats() if cursor_reader.available() else {
        "total": 0,
        "ok": 0,
        "messages": 0,
        "user_messages": 0,
    }
    return merge_stats(ag_stats, cursor_stats)


@app.context_processor
def inject_globals():
    return {
        "stats": _combined_stats(),
        "last_sync": storage.last_sync_run(),
        "cursor_available": cursor_reader.available(),
    }


def _browse_url(workspace: str = "", conversation_id: str = "", wq: str = "", cq: str = "") -> str:
    params = []
    if workspace is not None:
        params.append(f"workspace={quote(workspace, safe='')}")
    if conversation_id:
        params.append(f"conversation={quote(conversation_id, safe='')}")
    if wq:
        params.append(f"wq={quote(wq, safe='')}")
    if cq:
        params.append(f"cq={quote(cq, safe='')}")
    query = "&".join(params)
    return url_for("index") + (f"?{query}" if query else "")


def _list_workspaces(wq: str) -> list:
    ag_ws = storage.list_workspaces(q=wq)
    cursor_ws = cursor_reader.list_workspaces(q=wq) if cursor_reader.available() else []
    return merge_workspaces(ag_ws, cursor_ws)


def _list_conversations(workspace: str, cq: str) -> list:
    ag_rows = storage.list_conversations_by_workspace(workspace, q=cq)
    cursor_rows = cursor_reader.list_conversations(workspace, q=cq) if cursor_reader.available() else []
    return merge_conversations(ag_rows, cursor_rows)


def _group_messages_into_turns(messages: list) -> list:
    """按用户消息分段，后续非用户消息归入该轮的 replies。"""
    turns: list = []
    current: dict | None = None

    for msg in messages:
        if msg.get("role") == "user":
            if current:
                turns.append(current)
            current = {"user": msg, "replies": []}
        elif current is None:
            turns.append({"user": None, "replies": [msg]})
        else:
            current["replies"].append(msg)

    if current:
        turns.append(current)
    return turns


def _load_conversation_bundle(conversation_id: str) -> tuple[dict | None, list]:
    cached = conversation_cache.get(conversation_id)
    if cached:
        return cached["conv"], cached["messages"]

    conv = get_conversation(storage, cursor_reader, conversation_id)
    if not conv:
        return None, []

    messages = get_messages(storage, cursor_reader, conversation_id)
    conversation_cache.put(conversation_id, conv, messages)
    return conv, messages


@app.route("/")
def index():
    wq = request.args.get("wq", "").strip()
    workspaces = _list_workspaces(wq)
    workspace = unquote(request.args.get("workspace", ""))
    conversation_id = request.args.get("conversation", "").strip()
    cq = request.args.get("cq", "").strip()

    if workspaces and "workspace" not in request.args:
        workspace = workspaces[0]["workspace_path"]

    conversations: list = []
    if workspaces or "workspace" in request.args:
        conversations = _list_conversations(workspace, cq)

    conv = None
    messages: list = []
    workspace_stats = None
    show_workspace_stats = False

    if conversation_id:
        conv, messages = _load_conversation_bundle(conversation_id)
        if conv:
            workspace = conv.get("workspace_path") or workspace
        else:
            conversation_id = ""
    else:
        show_workspace_stats = True
        if conversations:
            workspace_stats = build_workspace_stats(
                workspace, storage, cursor_reader, conversations
            )

    workspace_report = None
    if show_workspace_stats and workspace:
        _repair_workspace_analysis(workspace)
        workspace_report = storage.get_workspace_report(workspace)

    return render_template(
        "browse.html",
        workspaces=workspaces,
        workspace=workspace,
        conversations=conversations,
        conversation_id=conversation_id,
        conv=conv,
        messages=messages,
        message_turns=_group_messages_into_turns(messages),
        workspace_stats=workspace_stats,
        workspace_report=workspace_report,
        show_workspace_stats=show_workspace_stats,
        ai_available=ai_available(),
        wq=wq,
        cq=cq,
        browse_url=_browse_url,
    )


@app.route("/workspace")
def workspace_redirect():
    path = request.args.get("path", "")
    return redirect(_browse_url(workspace=unquote(path)))


@app.route("/conversation/<conversation_id>")
def conversation_redirect(conversation_id: str):
    conv = get_conversation(storage, cursor_reader, conversation_id)
    if not conv:
        return render_template("error.html", message="会话不存在。"), 404
    return redirect(
        _browse_url(
            workspace=conv.get("workspace_path") or "",
            conversation_id=conversation_id,
        )
    )


def _repair_workspace_analysis(workspace: str) -> int:
    if not workspace:
        return 0
    conversations = _list_conversations(workspace, "")
    _stats, messages, fingerprint, _cached = _load_report_context(
        workspace, storage, cursor_reader, conversations
    )
    total_batches = len(split_message_batches(messages)) if messages else None
    storage.repair_analysis_state(
        workspace,
        message_fingerprint=fingerprint,
        total_batches=total_batches,
        user_message_count=len(messages) if messages else None,
    )
    return len(messages)


def _report_json(result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "blocks_fine": result.get("blocks_fine") or [],
        "blocks_modules": result.get("blocks_modules") or result.get("blocks") or [],
        "blocks": result.get("blocks_modules") or result.get("blocks") or [],
        "report_md": result.get("report_md") or "",
        "generated_at": result.get("generated_at"),
        "user_message_count": result.get("user_message_count"),
        "message_fingerprint": result.get("message_fingerprint"),
        "from_cache": bool(result.get("from_cache")),
        "llm_called": bool(result.get("llm_called")),
        "llm_calls": result.get("llm_calls"),
        "stage": result.get("stage"),
    }


def _stream_pipeline_events(
    workspace: str,
    force: bool,
    *,
    lock_task: str,
    busy_error: str,
    event_iter_factory,
    stage: str,
) -> Iterator[str]:
    conversations = _list_conversations(workspace, "")
    stats, messages, fingerprint, cached = _load_report_context(
        workspace, storage, cursor_reader, conversations
    )
    lock_token = str(uuid.uuid4())
    lock_acquired = False
    try:
        for event in event_iter_factory(stats, messages, fingerprint, cached):
            event_type = event.get("type")
            if event_type == "cached":
                yield format_sse("cached", event)
                continue
            if event_type == "error":
                yield format_sse("failed", event)
                return
            if event_type in ("progress", "batch_saved"):
                if not lock_acquired:
                    if not storage.try_acquire_analysis_lock(
                        workspace, lock_task, lock_token
                    ):
                        yield format_sse("failed", {"error": busy_error})
                        return
                    lock_acquired = True
                storage.update_analysis_progress(
                    workspace,
                    stage=event.get("stage") or "",
                    stage_detail=event.get("detail") or "",
                    progress_current=int(event.get("current") or 0),
                    progress_total=int(event.get("total") or 0),
                )
                yield format_sse(event_type, event)
                continue
            if event_type == "done":
                payload = _analysis_payload_from_event(
                    workspace, fingerprint, messages, event, stage=stage
                )
                yield format_sse("done", {"ok": True, "report": _report_json(payload)})
    except Exception as exc:
        logger.exception("%s SSE 失败 workspace=%s: %s", stage, workspace, exc)
        if lock_acquired:
            storage.set_analysis_error(workspace, str(exc))
        yield format_sse("failed", {"error": str(exc)})
    finally:
        if lock_acquired:
            storage.release_analysis_lock(workspace, lock_token)


def _stream_extract_events(workspace: str, force: bool) -> Iterator[str]:
    def factory(stats, messages, fingerprint, cached):
        return iter_extract_pipeline(
            workspace,
            stats,
            messages,
            storage,
            cached=cached,
            fingerprint=fingerprint,
            force=force,
        )

    return _stream_pipeline_events(
        workspace,
        force,
        lock_task=ANALYSIS_STATUS_EXTRACT,
        busy_error="提取任务正在进行中，请稍候再试",
        event_iter_factory=factory,
        stage="extract",
    )


def _stream_merge_events(workspace: str, force: bool) -> Iterator[str]:
    def factory(stats, messages, fingerprint, cached):
        return iter_merge_pipeline(
            workspace,
            stats,
            messages,
            storage,
            cached=cached,
            fingerprint=fingerprint,
            force=force,
        )

    return _stream_pipeline_events(
        workspace,
        force,
        lock_task=ANALYSIS_STATUS_MERGE,
        busy_error="合并任务正在进行中，请稍候再试",
        event_iter_factory=factory,
        stage="merge",
    )


def _stream_blocks_events(workspace: str, force: bool) -> Iterator[str]:
    """兼容旧端点：仅提取，不自动合并。"""
    yield from _stream_extract_events(workspace, force)


def _stream_report_events(workspace: str, force: bool) -> Iterator[str]:
    conversations = _list_conversations(workspace, "")
    stats, messages, fingerprint, cached = _load_report_context(
        workspace, storage, cursor_reader, conversations
    )
    lock_token = str(uuid.uuid4())
    lock_acquired = False
    try:
        for event in iter_report_pipeline(
            workspace,
            stats,
            messages,
            cached=cached,
            fingerprint=fingerprint,
            force=force,
        ):
            event_type = event.get("type")
            if event_type == "cached":
                yield format_sse("cached", event)
                continue
            if event_type == "error":
                yield format_sse("failed", event)
                return
            if event_type == "progress":
                if not lock_acquired:
                    if not storage.try_acquire_analysis_lock(
                        workspace, ANALYSIS_STATUS_REPORT, lock_token
                    ):
                        yield format_sse(
                            "failed",
                            {"error": "报告生成正在进行中，请稍候再试"},
                        )
                        return
                    lock_acquired = True
                storage.update_analysis_progress(
                    workspace,
                    stage=event.get("stage") or "",
                    stage_detail=event.get("detail") or "",
                    progress_current=int(event.get("current") or 0),
                    progress_total=int(event.get("total") or 0),
                )
                yield format_sse("progress", event)
                continue
            if event_type == "done":
                if not event.get("from_cache"):
                    storage.save_analysis_report(
                        workspace,
                        fingerprint,
                        event.get("report_md") or "",
                        len(messages),
                    )
                payload = _analysis_payload_from_event(
                    workspace, fingerprint, messages, event, stage="report"
                )
                yield format_sse("done", {"ok": True, "report": _report_json(payload)})
    except Exception as exc:
        logger.exception("报告 SSE 失败 workspace=%s: %s", workspace, exc)
        if lock_acquired:
            storage.set_analysis_error(workspace, str(exc))
        yield format_sse("failed", {"error": str(exc)})
    finally:
        if lock_acquired:
            storage.release_analysis_lock(workspace, lock_token)


def _analysis_payload_from_event(
    workspace: str,
    fingerprint: str,
    messages: list,
    event: Dict[str, Any],
    *,
    stage: str = "blocks",
) -> Dict[str, Any]:
    saved = storage.get_workspace_analysis(workspace) or {}
    return {
        **saved,
        "workspace_path": workspace,
        "message_fingerprint": fingerprint,
        "user_message_count": len(messages) or saved.get("user_message_count", 0),
        "blocks_fine": event.get("blocks_fine") or saved.get("blocks_fine") or [],
        "blocks_modules": event.get("blocks_modules")
        or saved.get("blocks_modules")
        or [],
        "blocks": event.get("blocks_modules") or saved.get("blocks_modules") or [],
        "report_md": event.get("report_md") or saved.get("report_md") or "",
        "from_cache": bool(event.get("from_cache")),
        "llm_called": not event.get("from_cache"),
        "llm_calls": event.get("llm_calls"),
        "stage": stage,
    }


@app.route("/api/workspace/analysis")
def workspace_analysis_api():
    workspace = unquote(request.args.get("workspace", ""))
    message_count = _repair_workspace_analysis(workspace)
    status = storage.get_analysis_status(workspace)
    analysis = storage.get_workspace_analysis(workspace)
    if analysis:
        status["blocks_fine"] = analysis.get("blocks_fine") or []
        status["blocks_modules"] = analysis.get("blocks_modules") or []
        status["report_md"] = analysis.get("report_md") or ""
        status["generated_at"] = analysis.get("generated_at")
    last_extracted = int(status.get("last_extracted_message_count") or 0)
    status["user_message_count"] = message_count
    status["pending_new_messages"] = max(0, message_count - last_extracted)
    status["extract_complete"] = message_count > 0 and last_extracted >= message_count
    return jsonify({"ok": True, "analysis": status})


@app.route("/api/workspace/batch-messages")
def workspace_batch_messages_api():
    workspace = unquote(request.args.get("workspace", ""))
    try:
        batch_index = int(request.args.get("batch_index", "-1"))
    except ValueError:
        return jsonify({"ok": False, "error": "batch_index 无效"}), 400

    conversations = _list_conversations(workspace, "")
    _stats, messages, _fp, _cached = _load_report_context(
        workspace, storage, cursor_reader, conversations
    )
    if not messages:
        return jsonify({"ok": False, "error": "没有可用的用户消息"}), 404

    batches = split_message_batches(messages)
    if batch_index < 0 or batch_index >= len(batches):
        return jsonify({"ok": False, "error": "批次不存在"}), 404

    batch = batches[batch_index]
    date_range = batch.get("date_range") or [None, None]
    payload_messages = []
    for msg in batch.get("messages") or []:
        payload_messages.append(
            {
                "conversation_id": msg.get("conversation_id") or "",
                "conversation_title": msg.get("conversation_title") or "",
                "created_at": msg.get("created_at"),
                "content": msg.get("content") or "",
                "source": msg.get("source") or "",
            }
        )
    return jsonify(
        {
            "ok": True,
            "batch_index": batch_index,
            "batch_no": batch_index + 1,
            "batch_total": len(batches),
            "message_count": len(payload_messages),
            "date_range": date_range,
            "messages": payload_messages,
        }
    )


@app.route("/api/workspace/extract-stream")
def workspace_extract_stream_api():
    workspace = unquote(request.args.get("workspace", ""))
    force = request.args.get("force") == "1"
    logger.info("GET /api/workspace/extract-stream workspace=%s force=%s", workspace, force)
    return sse_response(_stream_extract_events(workspace, force))


@app.route("/api/workspace/merge-stream")
def workspace_merge_stream_api():
    workspace = unquote(request.args.get("workspace", ""))
    force = request.args.get("force") == "1"
    logger.info("GET /api/workspace/merge-stream workspace=%s force=%s", workspace, force)
    return sse_response(_stream_merge_events(workspace, force))


@app.route("/api/workspace/blocks-stream")
def workspace_blocks_stream_api():
    workspace = unquote(request.args.get("workspace", ""))
    force = request.args.get("force") == "1"
    logger.info("GET /api/workspace/blocks-stream workspace=%s force=%s", workspace, force)
    return sse_response(_stream_blocks_events(workspace, force))


@app.route("/api/workspace/report-stream")
def workspace_report_stream_api():
    workspace = unquote(request.args.get("workspace", ""))
    force = request.args.get("force") == "1"
    logger.info("GET /api/workspace/report-stream workspace=%s force=%s", workspace, force)
    return sse_response(_stream_report_events(workspace, force))


@app.route("/api/workspace/extract", methods=["POST"])
def workspace_extract_api():
    workspace = unquote(request.args.get("workspace", ""))
    force = request.args.get("force") == "1"
    logger.info("POST /api/workspace/extract workspace=%s force=%s", workspace, force)
    conversations = _list_conversations(workspace, "")
    try:
        result = run_extract_report(
            workspace, storage, cursor_reader, conversations, force=force
        )
        return jsonify({"ok": True, "report": _report_json(result)})
    except Exception as exc:
        logger.exception("提取 API 失败 workspace=%s: %s", workspace, exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/workspace/merge", methods=["POST"])
def workspace_merge_api():
    workspace = unquote(request.args.get("workspace", ""))
    force = request.args.get("force") == "1"
    logger.info("POST /api/workspace/merge workspace=%s force=%s", workspace, force)
    conversations = _list_conversations(workspace, "")
    try:
        result = run_merge_report(
            workspace, storage, cursor_reader, conversations, force=force
        )
        return jsonify({"ok": True, "report": _report_json(result)})
    except Exception as exc:
        logger.exception("合并 API 失败 workspace=%s: %s", workspace, exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/workspace/blocks", methods=["POST"])
def workspace_blocks_api():
    workspace = unquote(request.args.get("workspace", ""))
    force = request.args.get("force") == "1"
    logger.info("POST /api/workspace/blocks workspace=%s force=%s", workspace, force)
    conversations = _list_conversations(workspace, "")
    try:
        result = run_blocks_report(
            workspace, storage, cursor_reader, conversations, force=force
        )
        return jsonify({"ok": True, "report": _report_json(result)})
    except Exception as exc:
        logger.exception("Blocks API 失败 workspace=%s: %s", workspace, exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/workspace/report-md", methods=["POST"])
def workspace_report_md_api():
    workspace = unquote(request.args.get("workspace", ""))
    force = request.args.get("force") == "1"
    logger.info("POST /api/workspace/report-md workspace=%s force=%s", workspace, force)
    conversations = _list_conversations(workspace, "")
    try:
        result = run_report_only(
            workspace, storage, cursor_reader, conversations, force=force
        )
        return jsonify({"ok": True, "report": _report_json(result)})
    except Exception as exc:
        logger.exception("报告 Markdown API 失败 workspace=%s: %s", workspace, exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/api/workspace/report", methods=["POST"])
def workspace_report_api():
    workspace = unquote(request.args.get("workspace", ""))
    force = request.args.get("force") == "1"
    logger.info("POST /api/workspace/report workspace=%s force=%s", workspace, force)
    conversations = _list_conversations(workspace, "")
    try:
        result = run_full_report(
            workspace,
            storage,
            cursor_reader,
            conversations,
            force=force,
        )
        logger.info(
            "报告 API 成功 workspace=%s from_cache=%s blocks=%d md_chars=%d",
            workspace,
            result.get("from_cache"),
            len(result.get("blocks") or []),
            len(result.get("report_md") or ""),
        )
        return jsonify({"ok": True, "report": _report_json(result)})
    except Exception as exc:
        logger.exception("报告 API 失败 workspace=%s: %s", workspace, exc)
        return jsonify({"ok": False, "error": str(exc)}), 500


@app.route("/ag-image")
def ag_image():
    path = resolve_ag_image_path(request.args.get("path", ""))
    if not path:
        abort(404)
    return send_file(path, conditional=True)


@app.route("/cursor-image/<image_uuid>")
def cursor_image(image_uuid: str):
    path = cursor_reader.get_image_path(image_uuid)
    if not path:
        abort(404)
    return send_file(path, conditional=True)


@app.route("/sync", methods=["POST"])
def sync():
    incremental = request.form.get("mode", "incremental") != "full"
    force = request.form.get("force") == "1"
    conversation_id = request.form.get("conversation_id", "").strip() or None
    workspace = request.form.get("workspace", "")
    next_url = request.form.get("next") or request.referrer or url_for("index")
    result = run_sync(
        storage,
        ide_dir=IDE_DATA_DIR,
        legacy_dir=LEGACY_DATA_DIR,
        incremental=incremental,
        force=force,
        conversation_id=conversation_id,
    )
    cursor_reader.invalidate_cache()
    conversation_cache.clear()
    flash(result.message, "success")
    if conversation_id and force:
        return redirect(_browse_url(workspace=workspace, conversation_id=conversation_id))
    return redirect(next_url)


@app.route("/health")
def health():
    return {
        "ok": True,
        "data_dir": str(IDE_DATA_DIR),
        "db": str(DB_PATH),
        "cursor_db": str(CURSOR_DB_PATH),
        "cursor_available": cursor_reader.available(),
        "stats": _combined_stats(),
        "cache": conversation_cache.stats(),
    }


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = argparse.ArgumentParser(description="Antigravity / Cursor 会话浏览 Web 服务")
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--sync-on-start", action="store_true", help="启动时执行 Antigravity 增量同步")
    parser.add_argument("--full-sync", action="store_true", help="启动时 Antigravity 全量同步")
    args = parser.parse_args()

    if args.sync_on_start or args.full_sync:
        result = run_sync(storage, incremental=not args.full_sync, force=args.full_sync)
        print(result.message)

    print(f"Antigravity 数据: {IDE_DATA_DIR}")
    print(f"Antigravity 库:   {DB_PATH}")
    print(f"Cursor 库:        {CURSOR_DB_PATH} ({'可读' if cursor_reader.available() else '不可用'})")
    print(f"访问:             http://{args.host}:{args.port}")
    app.config["TEMPLATES_AUTO_RELOAD"] = True
    app.run(host=args.host, port=args.port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
