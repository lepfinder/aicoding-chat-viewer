"""增量同步 Antigravity 本地会话到 SQLite。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Set

from config import IDE_DATA_DIR, LEGACY_DATA_DIR
from parser import (
    collect_source_files,
    discover_conversation_ids,
    has_readable_sources,
    load_summaries,
    parse_conversation,
)
from storage import Storage


@dataclass
class SyncResult:
    mode: str
    new_count: int = 0
    updated_count: int = 0
    skipped_count: int = 0
    error_count: int = 0
    message: str = ""


def _source_type(path: Path) -> str:
    name = path.name
    if name == "overview.txt":
        return "overview"
    if name == "transcript.jsonl":
        return "transcript"
    if name.endswith(".db"):
        return "sqlite_db"
    return "unknown"


def _needs_sync(storage: Storage, path: Path, incremental: bool) -> bool:
    if not incremental:
        return True
    st = path.stat()
    prev = storage.get_sync_state(str(path.resolve()))
    if prev is None:
        return True
    return prev["file_mtime"] != st.st_mtime or prev["file_size"] != st.st_size


def _conversation_needs_sync(
    storage: Storage,
    conversation_id: str,
    ide_dir: Path,
    incremental: bool,
    force: bool,
) -> bool:
    if force:
        return True
    if not incremental:
        return True
    files = collect_source_files(conversation_id, ide_dir)
    if not files:
        existing = storage.get_conversation(conversation_id)
        return existing is None
    return any(_needs_sync(storage, p, incremental=True) for p in files)


def run_sync(
    storage: Storage,
    ide_dir: Path = IDE_DATA_DIR,
    legacy_dir: Path = LEGACY_DATA_DIR,
    incremental: bool = True,
    force: bool = False,
    conversation_id: Optional[str] = None,
) -> SyncResult:
    mode = "incremental" if incremental and not force else "full"
    result = SyncResult(mode=mode)
    summaries = load_summaries(legacy_dir)

    if conversation_id:
        ids: Set[str] = {conversation_id}
    else:
        ids = discover_conversation_ids(ide_dir)

    run_id = storage.record_sync_run(mode, 0, 0, 0, 0, "sync started")

    for cid in sorted(ids):
        try:
            sources = has_readable_sources(cid, ide_dir)
            pb_only = (ide_dir / "conversations" / f"{cid}.pb").exists() and not sources

            if pb_only:
                existing = storage.get_conversation(cid)
                if existing and incremental and not force:
                    result.skipped_count += 1
                    continue
                conv = parse_conversation(cid, ide_dir, summaries)
                storage.save_conversation(conv)
                if existing:
                    result.updated_count += 1
                else:
                    result.new_count += 1
                continue

            if not _conversation_needs_sync(storage, cid, ide_dir, incremental, force):
                result.skipped_count += 1
                continue

            existing = storage.get_conversation(cid)
            conv = parse_conversation(cid, ide_dir, summaries)
            storage.save_conversation(conv)

            for path in collect_source_files(cid, ide_dir):
                st = path.stat()
                storage.upsert_sync_state(
                    str(path.resolve()),
                    cid,
                    _source_type(path),
                    st.st_mtime,
                    st.st_size,
                )

            if existing:
                result.updated_count += 1
            else:
                result.new_count += 1
        except Exception as exc:
            result.error_count += 1
            result.message += f"{cid}: {exc}; "

    summaries_path = legacy_dir / "agyhub_summaries_proto.pb"
    if summaries_path.exists() and (not incremental or force or _needs_sync(storage, summaries_path, incremental)):
        for cid, meta in summaries.items():
            existing = storage.get_conversation(cid)
            if not existing:
                continue
            if meta.get("title") and not existing.get("title"):
                conv = parse_conversation(cid, ide_dir, summaries)
                storage.save_conversation(conv)
        if summaries_path.exists():
            st = summaries_path.stat()
            storage.upsert_sync_state(
                str(summaries_path.resolve()),
                "_summaries",
                "summaries",
                st.st_mtime,
                st.st_size,
            )

    summary = (
        f"新增 {result.new_count}，更新 {result.updated_count}，"
        f"跳过 {result.skipped_count}，错误 {result.error_count}"
    )
    if result.message:
        summary += f"。{result.message}"
    result.message = summary
    storage.record_sync_run(
        mode,
        result.new_count,
        result.updated_count,
        result.skipped_count,
        result.error_count,
        summary,
        run_id=run_id,
    )
    return result
