"""SQLite 存储与查询。"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional


SCHEMA = """
CREATE TABLE IF NOT EXISTS conversations (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL DEFAULT '',
    workspace_path TEXT NOT NULL DEFAULT '',
    created_at TEXT,
    updated_at TEXT,
    parse_status TEXT NOT NULL DEFAULT 'ok',
    source_types TEXT NOT NULL DEFAULT '[]',
    message_count INTEGER NOT NULL DEFAULT 0,
    user_message_count INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS messages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT NOT NULL,
    step_index INTEGER NOT NULL DEFAULT 0,
    role TEXT NOT NULL,
    message_type TEXT NOT NULL DEFAULT '',
    content TEXT NOT NULL DEFAULT '',
    thinking TEXT,
    tool_name TEXT,
    tool_args TEXT,
    created_at TEXT,
    source TEXT NOT NULL DEFAULT '',
    is_truncated INTEGER NOT NULL DEFAULT 0,
    images TEXT,
    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_messages_conv ON messages(conversation_id, step_index);

CREATE TABLE IF NOT EXISTS sync_state (
    source_path TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL,
    source_type TEXT NOT NULL,
    file_mtime REAL NOT NULL,
    file_size INTEGER NOT NULL,
    synced_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sync_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    mode TEXT NOT NULL,
    new_count INTEGER DEFAULT 0,
    updated_count INTEGER DEFAULT 0,
    skipped_count INTEGER DEFAULT 0,
    error_count INTEGER DEFAULT 0,
    message TEXT
);

CREATE TABLE IF NOT EXISTS workspace_reports (
    workspace_path TEXT PRIMARY KEY,
    message_fingerprint TEXT NOT NULL DEFAULT '',
    user_message_count INTEGER NOT NULL DEFAULT 0,
    blocks_json TEXT NOT NULL DEFAULT '[]',
    report_md TEXT NOT NULL DEFAULT '',
    generated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workspace_analysis (
    workspace_path TEXT PRIMARY KEY,
    message_fingerprint TEXT NOT NULL DEFAULT '',
    user_message_count INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'idle',
    stage TEXT NOT NULL DEFAULT '',
    stage_detail TEXT NOT NULL DEFAULT '',
    progress_current INTEGER NOT NULL DEFAULT 0,
    progress_total INTEGER NOT NULL DEFAULT 0,
    lock_token TEXT,
    locked_at TEXT,
    error_message TEXT,
    blocks_fine_at TEXT,
    blocks_modules_at TEXT,
    report_md TEXT NOT NULL DEFAULT '',
    report_at TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS workspace_blocks_fine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workspace_path TEXT NOT NULL,
    message_fingerprint TEXT NOT NULL,
    block_id TEXT NOT NULL,
    batch_index INTEGER,
    type TEXT NOT NULL DEFAULT 'feature',
    title TEXT NOT NULL,
    summary TEXT NOT NULL DEFAULT '',
    start_date TEXT,
    end_date TEXT,
    status TEXT NOT NULL DEFAULT 'unknown',
    keywords_json TEXT NOT NULL DEFAULT '[]',
    evidence_json TEXT NOT NULL DEFAULT '[]',
    confidence REAL,
    sort_order INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_blocks_fine_ws
    ON workspace_blocks_fine(workspace_path, message_fingerprint);

CREATE TABLE IF NOT EXISTS workspace_blocks_modules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    workspace_path TEXT NOT NULL,
    message_fingerprint TEXT NOT NULL,
    module_id TEXT NOT NULL,
    type TEXT NOT NULL DEFAULT 'module',
    title TEXT NOT NULL,
    summary TEXT NOT NULL DEFAULT '',
    start_date TEXT,
    end_date TEXT,
    status TEXT NOT NULL DEFAULT 'unknown',
    keywords_json TEXT NOT NULL DEFAULT '[]',
    evidence_json TEXT NOT NULL DEFAULT '[]',
    child_fine_ids_json TEXT NOT NULL DEFAULT '[]',
    confidence REAL,
    sort_order INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_blocks_modules_ws
    ON workspace_blocks_modules(workspace_path, message_fingerprint);
"""

LOCK_TTL_SECONDS = 7200
ANALYSIS_STATUS_IDLE = "idle"
ANALYSIS_STATUS_EXTRACT = "extract"
ANALYSIS_STATUS_MERGE = "merge"
ANALYSIS_STATUS_BLOCKS = "blocks"
ANALYSIS_STATUS_REPORT = "report"
ANALYSIS_STATUS_FAILED = "failed"
ACTIVE_LOCK_STATUSES = (
    ANALYSIS_STATUS_EXTRACT,
    ANALYSIS_STATUS_MERGE,
    ANALYSIS_STATUS_BLOCKS,
    ANALYSIS_STATUS_REPORT,
)
_STALE_EXTRACT_ERROR_MARKERS = (
    "未分批入库",
    "上次任务中断",
)


def _should_clear_stale_error(
    error_message: str, completed: int, total: int
) -> bool:
    if not error_message:
        return False
    if total > 0 and completed >= total:
        return True
    if completed > 0 and any(m in error_message for m in _STALE_EXTRACT_ERROR_MARKERS):
        return True
    return False


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Storage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(SCHEMA)
            self._migrate(conn)

    def _migrate(self, conn: sqlite3.Connection) -> None:
        cols = {row[1] for row in conn.execute("PRAGMA table_info(conversations)")}
        if "user_message_count" not in cols:
            conn.execute(
                "ALTER TABLE conversations ADD COLUMN user_message_count INTEGER NOT NULL DEFAULT 0"
            )
            conn.execute(
                """
                UPDATE conversations SET user_message_count = (
                    SELECT COUNT(*) FROM messages m
                    WHERE m.conversation_id = conversations.id AND m.role = 'user'
                )
                """
            )
        msg_cols = {row[1] for row in conn.execute("PRAGMA table_info(messages)")}
        if "images" not in msg_cols:
            conn.execute("ALTER TABLE messages ADD COLUMN images TEXT")
        analysis_cols = {row[1] for row in conn.execute("PRAGMA table_info(workspace_analysis)")}
        if "extract_total_batches" not in analysis_cols:
            conn.execute(
                "ALTER TABLE workspace_analysis ADD COLUMN extract_total_batches INTEGER NOT NULL DEFAULT 0"
            )
        if "extract_completed_batches" not in analysis_cols:
            conn.execute(
                "ALTER TABLE workspace_analysis ADD COLUMN extract_completed_batches INTEGER NOT NULL DEFAULT 0"
            )
        if "last_extracted_message_count" not in analysis_cols:
            conn.execute(
                "ALTER TABLE workspace_analysis ADD COLUMN last_extracted_message_count INTEGER NOT NULL DEFAULT 0"
            )
        if "current_message_fingerprint" not in analysis_cols:
            conn.execute(
                "ALTER TABLE workspace_analysis ADD COLUMN current_message_fingerprint TEXT NOT NULL DEFAULT ''"
            )
        if "anchor_message_count" not in analysis_cols:
            conn.execute(
                "ALTER TABLE workspace_analysis ADD COLUMN anchor_message_count INTEGER NOT NULL DEFAULT 0"
            )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_blocks_fine_batch
            ON workspace_blocks_fine(workspace_path, message_fingerprint, batch_index)
            """
        )

    def _row_to_block(self, row: sqlite3.Row, *, child_key: Optional[str] = None) -> Dict[str, Any]:
        data = dict(row)
        for key in ("keywords_json", "evidence_json", child_key or ""):
            if not key:
                continue
            json_key = key.replace("_json", "")
            if json_key == "child_fine_ids":
                json_key = "child_fine_ids"
            try:
                data[json_key] = json.loads(data.pop(key, "[]") or "[]")
            except json.JSONDecodeError:
                data[json_key] = []
        if child_key and child_key in data:
            try:
                data["child_fine_ids"] = json.loads(data.pop(child_key) or "[]")
            except json.JSONDecodeError:
                data["child_fine_ids"] = []
        return data

    def get_workspace_analysis(self, workspace_path: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM workspace_analysis WHERE workspace_path = ?",
                (workspace_path,),
            ).fetchone()
        if not row:
            legacy = self._get_workspace_report_legacy(workspace_path)
            if legacy:
                return self._legacy_to_analysis(legacy)
            return None

        meta = dict(row)
        fp = self.resolve_active_fingerprint(
            workspace_path, meta.get("message_fingerprint") or ""
        )
        blocks_fine = self.list_blocks_fine(workspace_path, fp)
        blocks_modules = self.list_blocks_modules(workspace_path, fp)
        if fp and fp != (meta.get("message_fingerprint") or ""):
            meta["message_fingerprint"] = fp
        return {
            **meta,
            "blocks_fine": blocks_fine,
            "blocks_modules": blocks_modules,
            "blocks": blocks_modules,
            "generated_at": meta.get("report_at")
            or meta.get("blocks_modules_at")
            or meta.get("blocks_fine_at"),
        }

    def _legacy_to_analysis(self, legacy: Dict[str, Any]) -> Dict[str, Any]:
        modules = legacy.get("blocks") or []
        return {
            "workspace_path": legacy.get("workspace_path"),
            "message_fingerprint": legacy.get("message_fingerprint", ""),
            "user_message_count": legacy.get("user_message_count", 0),
            "status": ANALYSIS_STATUS_IDLE,
            "stage": "",
            "stage_detail": "",
            "progress_current": 0,
            "progress_total": 0,
            "blocks_fine": [],
            "blocks_modules": modules,
            "blocks": modules,
            "report_md": legacy.get("report_md") or "",
            "generated_at": legacy.get("generated_at"),
            "report_at": legacy.get("generated_at") if legacy.get("report_md") else None,
            "blocks_modules_at": legacy.get("generated_at") if modules else None,
        }

    def _get_workspace_report_legacy(self, workspace_path: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM workspace_reports WHERE workspace_path = ?",
                (workspace_path,),
            ).fetchone()
        if not row:
            return None
        data = dict(row)
        data["workspace_path"] = workspace_path
        try:
            data["blocks"] = json.loads(data.pop("blocks_json") or "[]")
        except json.JSONDecodeError:
            data["blocks"] = []
        return data

    def get_workspace_report(self, workspace_path: str) -> Optional[Dict[str, Any]]:
        return self.get_workspace_analysis(workspace_path)

    def list_stored_block_fingerprints(self, workspace_path: str) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    f.message_fingerprint AS fingerprint,
                    COUNT(DISTINCT f.id) AS fine_count,
                    (
                        SELECT COUNT(*)
                        FROM workspace_blocks_modules m
                        WHERE m.workspace_path = f.workspace_path
                          AND m.message_fingerprint = f.message_fingerprint
                    ) AS module_count
                FROM workspace_blocks_fine f
                WHERE f.workspace_path = ?
                GROUP BY f.message_fingerprint
                ORDER BY fine_count DESC, module_count DESC
                """,
                (workspace_path,),
            ).fetchall()
        if rows:
            return [dict(row) for row in rows]
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    m.message_fingerprint AS fingerprint,
                    0 AS fine_count,
                    COUNT(DISTINCT m.id) AS module_count
                FROM workspace_blocks_modules m
                WHERE m.workspace_path = ?
                GROUP BY m.message_fingerprint
                ORDER BY module_count DESC
                """,
                (workspace_path,),
            ).fetchall()
        return [dict(row) for row in rows]

    def resolve_active_fingerprint(
        self, workspace_path: str, preferred: str = ""
    ) -> str:
        preferred = (preferred or "").strip()
        if preferred and (
            self.list_blocks_fine(workspace_path, preferred)
            or self.list_blocks_modules(workspace_path, preferred)
        ):
            return preferred
        candidates = self.list_stored_block_fingerprints(workspace_path)
        if candidates:
            return str(candidates[0].get("fingerprint") or "")
        return preferred

    def list_blocks_fine(self, workspace_path: str, fingerprint: str) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM workspace_blocks_fine
                WHERE workspace_path = ? AND message_fingerprint = ?
                ORDER BY sort_order, id
                """,
                (workspace_path, fingerprint),
            ).fetchall()
        return [self._decode_fine_row(dict(r)) for r in rows]

    def list_blocks_modules(self, workspace_path: str, fingerprint: str) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM workspace_blocks_modules
                WHERE workspace_path = ? AND message_fingerprint = ?
                ORDER BY sort_order, id
                """,
                (workspace_path, fingerprint),
            ).fetchall()
        return [self._decode_module_row(dict(r)) for r in rows]

    def get_completed_batch_indices(
        self, workspace_path: str, fingerprint: str
    ) -> List[int]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT batch_index FROM workspace_blocks_fine
                WHERE workspace_path = ? AND message_fingerprint = ?
                  AND batch_index IS NOT NULL
                ORDER BY batch_index
                """,
                (workspace_path, fingerprint),
            ).fetchall()
        return [int(r[0]) for r in rows]

    def get_max_batch_index(
        self, workspace_path: str, fingerprint: str
    ) -> Optional[int]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT MAX(batch_index) FROM workspace_blocks_fine
                WHERE workspace_path = ? AND message_fingerprint = ?
                  AND batch_index IS NOT NULL
                """,
                (workspace_path, fingerprint),
            ).fetchone()
        if not row or row[0] is None:
            return None
        return int(row[0])

    def clear_blocks_fine(self, workspace_path: str, fingerprint: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                DELETE FROM workspace_blocks_fine
                WHERE workspace_path = ? AND message_fingerprint = ?
                """,
                (workspace_path, fingerprint),
            )
            conn.execute(
                """
                UPDATE workspace_analysis SET
                    extract_total_batches = 0,
                    extract_completed_batches = 0,
                    last_extracted_message_count = 0,
                    anchor_message_count = 0,
                    current_message_fingerprint = '',
                    message_fingerprint = CASE
                        WHEN message_fingerprint = ? THEN ''
                        ELSE message_fingerprint
                    END,
                    blocks_fine_at = NULL,
                    updated_at = ?
                WHERE workspace_path = ?
                """,
                (fingerprint, utc_now(), workspace_path),
            )

    def clear_blocks_modules(self, workspace_path: str, fingerprint: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                DELETE FROM workspace_blocks_modules
                WHERE workspace_path = ? AND message_fingerprint = ?
                """,
                (workspace_path, fingerprint),
            )
            conn.execute(
                """
                UPDATE workspace_analysis SET
                    blocks_modules_at = NULL, updated_at = ?
                WHERE workspace_path = ?
                """,
                (utc_now(), workspace_path),
            )

    def ensure_anchor_message_count(self, workspace_path: str, anchor_message_count: int) -> None:
        if anchor_message_count <= 0:
            return
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE workspace_analysis SET
                    anchor_message_count = ?
                WHERE workspace_path = ?
                  AND (anchor_message_count IS NULL OR anchor_message_count = 0)
                """,
                (anchor_message_count, workspace_path),
            )

    def save_blocks_fine_batch(
        self,
        workspace_path: str,
        fingerprint: str,
        batch_index: int,
        blocks: List[Dict[str, Any]],
        user_message_count: int,
        total_batches: int,
        *,
        extracted_message_count: int,
        current_message_fingerprint: str = "",
        anchor_message_count: int = 0,
    ) -> None:
        now = utc_now()
        with self.connect() as conn:
            conn.execute(
                """
                DELETE FROM workspace_blocks_fine
                WHERE workspace_path = ? AND message_fingerprint = ? AND batch_index = ?
                """,
                (workspace_path, fingerprint, batch_index),
            )
            for i, block in enumerate(blocks):
                conn.execute(
                    """
                    INSERT INTO workspace_blocks_fine (
                        workspace_path, message_fingerprint, block_id, batch_index,
                        type, title, summary, start_date, end_date, status,
                        keywords_json, evidence_json, confidence, sort_order
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        workspace_path,
                        fingerprint,
                        block.get("id") or f"fine-{batch_index + 1}-{i + 1}",
                        batch_index,
                        block.get("type") or "feature",
                        block.get("title") or f"功能 {i + 1}",
                        block.get("summary") or "",
                        block.get("start_date"),
                        block.get("end_date"),
                        block.get("status") or "unknown",
                        json.dumps(block.get("keywords") or [], ensure_ascii=False),
                        json.dumps(block.get("evidence") or [], ensure_ascii=False),
                        block.get("confidence"),
                        batch_index * 100 + i,
                    ),
                )
            completed = conn.execute(
                """
                SELECT COUNT(DISTINCT batch_index) FROM workspace_blocks_fine
                WHERE workspace_path = ? AND message_fingerprint = ?
                  AND batch_index IS NOT NULL
                """,
                (workspace_path, fingerprint),
            ).fetchone()[0]
            conn.execute(
                """
                INSERT INTO workspace_analysis (
                    workspace_path, message_fingerprint, current_message_fingerprint,
                    user_message_count, last_extracted_message_count, anchor_message_count,
                    extract_total_batches, extract_completed_batches,
                    blocks_fine_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(workspace_path) DO UPDATE SET
                    message_fingerprint = excluded.message_fingerprint,
                    current_message_fingerprint = excluded.current_message_fingerprint,
                    user_message_count = excluded.user_message_count,
                    last_extracted_message_count = excluded.last_extracted_message_count,
                    anchor_message_count = CASE
                        WHEN workspace_analysis.anchor_message_count > 0
                        THEN workspace_analysis.anchor_message_count
                        ELSE excluded.anchor_message_count
                    END,
                    extract_total_batches = excluded.extract_total_batches,
                    extract_completed_batches = excluded.extract_completed_batches,
                    blocks_fine_at = excluded.blocks_fine_at,
                    updated_at = excluded.updated_at
                """,
                (
                    workspace_path,
                    fingerprint,
                    current_message_fingerprint or fingerprint,
                    user_message_count,
                    extracted_message_count,
                    anchor_message_count,
                    total_batches,
                    completed,
                    now,
                    now,
                ),
            )

    def _decode_fine_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        try:
            row["keywords"] = json.loads(row.pop("keywords_json", "[]") or "[]")
        except json.JSONDecodeError:
            row["keywords"] = []
        try:
            row["evidence"] = json.loads(row.pop("evidence_json", "[]") or "[]")
        except json.JSONDecodeError:
            row["evidence"] = []
        row["id"] = row.get("block_id") or row.get("module_id")
        return row

    def _decode_module_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        block = self._decode_fine_row(dict(row))
        try:
            block["child_fine_ids"] = json.loads(row.get("child_fine_ids_json") or "[]")
        except json.JSONDecodeError:
            block["child_fine_ids"] = []
        block.pop("child_fine_ids_json", None)
        block["id"] = block.get("module_id") or block.get("block_id")
        return block

    def ensure_analysis_row(self, workspace_path: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO workspace_analysis (workspace_path, updated_at)
                VALUES (?, ?)
                ON CONFLICT(workspace_path) DO NOTHING
                """,
                (workspace_path, utc_now()),
            )

    def _progress_stale(self, workspace_path: str, minutes: int = 30) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT updated_at FROM workspace_analysis WHERE workspace_path = ?",
                (workspace_path,),
            ).fetchone()
        if not row or not row["updated_at"]:
            return True
        try:
            dt = datetime.fromisoformat(row["updated_at"].replace("Z", "+00:00"))
            age = (datetime.now(timezone.utc) - dt).total_seconds()
            return age > minutes * 60
        except ValueError:
            return True

    def try_acquire_analysis_lock(
        self, workspace_path: str, task: str, lock_token: str
    ) -> bool:
        self.ensure_analysis_row(workspace_path)
        now = utc_now()
        with self.connect() as conn:
            row = conn.execute(
                "SELECT status, lock_token, locked_at FROM workspace_analysis WHERE workspace_path = ?",
                (workspace_path,),
            ).fetchone()
            if row and row["status"] in ACTIVE_LOCK_STATUSES:
                locked_at = row["locked_at"]
                stale = (locked_at and self._lock_stale(locked_at)) or self._progress_stale(
                    workspace_path
                )
                if stale:
                    pass
                elif row["lock_token"] != lock_token:
                    return False
            conn.execute(
                """
                UPDATE workspace_analysis SET
                    status = ?, lock_token = ?, locked_at = ?,
                    stage = 'starting', stage_detail = '', error_message = '',
                    progress_current = 0, progress_total = 0, updated_at = ?
                WHERE workspace_path = ?
                """,
                (task, lock_token, now, now, workspace_path),
            )
        return True

    def _lock_stale(self, locked_at: str) -> bool:
        try:
            dt = datetime.fromisoformat(locked_at.replace("Z", "+00:00"))
            age = (datetime.now(timezone.utc) - dt).total_seconds()
            return age > LOCK_TTL_SECONDS
        except ValueError:
            return True

    def release_analysis_lock(self, workspace_path: str, lock_token: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE workspace_analysis SET
                    status = CASE WHEN error_message != '' THEN ? ELSE ? END,
                    lock_token = NULL, locked_at = NULL,
                    stage = CASE WHEN error_message != '' THEN stage ELSE '' END,
                    updated_at = ?
                WHERE workspace_path = ? AND lock_token = ?
                """,
                (
                    ANALYSIS_STATUS_FAILED,
                    ANALYSIS_STATUS_IDLE,
                    utc_now(),
                    workspace_path,
                    lock_token,
                ),
            )

    def update_analysis_progress(
        self,
        workspace_path: str,
        *,
        stage: str,
        stage_detail: str = "",
        progress_current: int = 0,
        progress_total: int = 0,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE workspace_analysis SET
                    stage = ?, stage_detail = ?,
                    progress_current = ?, progress_total = ?,
                    updated_at = ?
                WHERE workspace_path = ?
                """,
                (
                    stage,
                    stage_detail,
                    progress_current,
                    progress_total,
                    utc_now(),
                    workspace_path,
                ),
            )

    def set_analysis_error(self, workspace_path: str, message: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE workspace_analysis SET
                    error_message = ?, status = ?, updated_at = ?
                WHERE workspace_path = ?
                """,
                (message[:500], ANALYSIS_STATUS_FAILED, utc_now(), workspace_path),
            )

    def save_blocks_fine(
        self,
        workspace_path: str,
        fingerprint: str,
        blocks: List[Dict[str, Any]],
        user_message_count: int,
    ) -> None:
        now = utc_now()
        with self.connect() as conn:
            conn.execute(
                "DELETE FROM workspace_blocks_fine WHERE workspace_path = ? AND message_fingerprint = ?",
                (workspace_path, fingerprint),
            )
            for i, block in enumerate(blocks):
                conn.execute(
                    """
                    INSERT INTO workspace_blocks_fine (
                        workspace_path, message_fingerprint, block_id, batch_index,
                        type, title, summary, start_date, end_date, status,
                        keywords_json, evidence_json, confidence, sort_order
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        workspace_path,
                        fingerprint,
                        block.get("id") or f"fine-{i + 1}",
                        block.get("batch_index"),
                        block.get("type") or "feature",
                        block.get("title") or f"功能 {i + 1}",
                        block.get("summary") or "",
                        block.get("start_date"),
                        block.get("end_date"),
                        block.get("status") or "unknown",
                        json.dumps(block.get("keywords") or [], ensure_ascii=False),
                        json.dumps(block.get("evidence") or [], ensure_ascii=False),
                        block.get("confidence"),
                        i,
                    ),
                )
            conn.execute(
                """
                INSERT INTO workspace_analysis (
                    workspace_path, message_fingerprint, user_message_count,
                    blocks_fine_at, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(workspace_path) DO UPDATE SET
                    message_fingerprint = excluded.message_fingerprint,
                    user_message_count = excluded.user_message_count,
                    blocks_fine_at = excluded.blocks_fine_at,
                    updated_at = excluded.updated_at
                """,
                (workspace_path, fingerprint, user_message_count, now, now),
            )

    def save_blocks_modules(
        self,
        workspace_path: str,
        fingerprint: str,
        blocks: List[Dict[str, Any]],
        *,
        clear_report: bool = True,
    ) -> None:
        now = utc_now()
        with self.connect() as conn:
            conn.execute(
                "DELETE FROM workspace_blocks_modules WHERE workspace_path = ? AND message_fingerprint = ?",
                (workspace_path, fingerprint),
            )
            for i, block in enumerate(blocks):
                conn.execute(
                    """
                    INSERT INTO workspace_blocks_modules (
                        workspace_path, message_fingerprint, module_id,
                        type, title, summary, start_date, end_date, status,
                        keywords_json, evidence_json, child_fine_ids_json,
                        confidence, sort_order
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        workspace_path,
                        fingerprint,
                        block.get("id") or f"mod-{i + 1}",
                        block.get("type") or "module",
                        block.get("title") or f"模块 {i + 1}",
                        block.get("summary") or "",
                        block.get("start_date"),
                        block.get("end_date"),
                        block.get("status") or "unknown",
                        json.dumps(block.get("keywords") or [], ensure_ascii=False),
                        json.dumps(block.get("evidence") or [], ensure_ascii=False),
                        json.dumps(block.get("child_fine_ids") or [], ensure_ascii=False),
                        block.get("confidence"),
                        i,
                    ),
                )
            sql = """
                UPDATE workspace_analysis SET
                    blocks_modules_at = ?, updated_at = ?
            """
            params: List[Any] = [now, now]
            if clear_report:
                sql += ", report_md = '', report_at = NULL"
            sql += " WHERE workspace_path = ?"
            params.append(workspace_path)
            conn.execute(sql, params)
            self._sync_legacy_workspace_report(conn, workspace_path, fingerprint, blocks)

    def save_analysis_report(
        self,
        workspace_path: str,
        fingerprint: str,
        report_md: str,
        user_message_count: int,
    ) -> None:
        now = utc_now()
        anchor_fp = self.resolve_active_fingerprint(workspace_path, fingerprint)
        modules = self.list_blocks_modules(workspace_path, anchor_fp)
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE workspace_analysis SET
                    report_md = ?, report_at = ?, updated_at = ?,
                    user_message_count = ?,
                    current_message_fingerprint = ?
                WHERE workspace_path = ?
                """,
                (
                    report_md,
                    now,
                    now,
                    user_message_count,
                    fingerprint,
                    workspace_path,
                ),
            )
            self._sync_legacy_workspace_report(
                conn, workspace_path, anchor_fp, modules, report_md
            )

    def _sync_legacy_workspace_report(
        self,
        conn: sqlite3.Connection,
        workspace_path: str,
        fingerprint: str,
        modules: List[Dict[str, Any]],
        report_md: str = "",
    ) -> None:
        existing = conn.execute(
            "SELECT report_md FROM workspace_analysis WHERE workspace_path = ?",
            (workspace_path,),
        ).fetchone()
        md = report_md or (existing["report_md"] if existing else "") or ""
        conn.execute(
            """
            INSERT INTO workspace_reports (
                workspace_path, message_fingerprint, user_message_count,
                blocks_json, report_md, generated_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(workspace_path) DO UPDATE SET
                message_fingerprint = excluded.message_fingerprint,
                user_message_count = excluded.user_message_count,
                blocks_json = excluded.blocks_json,
                report_md = excluded.report_md,
                generated_at = excluded.generated_at
            """,
            (
                workspace_path,
                fingerprint,
                conn.execute(
                    "SELECT user_message_count FROM workspace_analysis WHERE workspace_path = ?",
                    (workspace_path,),
                ).fetchone()[0],
                json.dumps(modules, ensure_ascii=False),
                md,
                utc_now(),
            ),
        )

    def repair_analysis_state(
        self,
        workspace_path: str,
        *,
        message_fingerprint: str | None = None,
        total_batches: int | None = None,
        user_message_count: int | None = None,
    ) -> bool:
        """根据 fine blocks 入库情况校正提取进度，并清理过期锁/错误信息。"""
        self.ensure_analysis_row(workspace_path)
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM workspace_analysis WHERE workspace_path = ?",
                (workspace_path,),
            ).fetchone()
            if not row:
                return False

            data = dict(row)
            anchor_fp = data.get("message_fingerprint") or ""
            active_fp = self.resolve_active_fingerprint(workspace_path, anchor_fp)
            fp = active_fp or anchor_fp or message_fingerprint or ""
            if not fp:
                return False

            status = data.get("status") or ANALYSIS_STATUS_IDLE
            locked_at = data.get("locked_at")
            lock_stale = bool(locked_at and self._lock_stale(locked_at))
            progress_stale = self._progress_stale(workspace_path)
            if status in ACTIVE_LOCK_STATUSES and not lock_stale and not progress_stale:
                return False

            completed = int(
                conn.execute(
                    """
                    SELECT COUNT(DISTINCT batch_index) FROM workspace_blocks_fine
                    WHERE workspace_path = ? AND message_fingerprint = ?
                      AND batch_index IS NOT NULL
                    """,
                    (workspace_path, fp),
                ).fetchone()[0]
                or 0
            )

            stored_total = int(data.get("extract_total_batches") or 0)
            stored_completed = int(data.get("extract_completed_batches") or 0)
            resolved_total = (
                int(total_batches)
                if total_batches and total_batches > 0
                else stored_total
            )
            error_message = data.get("error_message") or ""
            updates: Dict[str, Any] = {}
            now = utc_now()

            if active_fp and active_fp != anchor_fp:
                updates["message_fingerprint"] = active_fp

            if status in ACTIVE_LOCK_STATUSES and (lock_stale or progress_stale):
                updates["lock_token"] = None
                updates["locked_at"] = None
                if not error_message:
                    updates["status"] = ANALYSIS_STATUS_IDLE
                    updates["stage"] = ""
                    updates["stage_detail"] = ""

            if completed != stored_completed:
                updates["extract_completed_batches"] = completed
            if resolved_total > 0 and resolved_total != stored_total:
                updates["extract_total_batches"] = resolved_total

            final_total = int(updates.get("extract_total_batches", resolved_total) or 0)
            final_completed = int(
                updates.get("extract_completed_batches", completed) or 0
            )
            if _should_clear_stale_error(error_message, final_completed, final_total):
                updates["error_message"] = ""
                updates["status"] = ANALYSIS_STATUS_IDLE
                updates["stage"] = ""
                updates["stage_detail"] = ""

            if completed > 0 and not data.get("blocks_fine_at"):
                updates["blocks_fine_at"] = now

            stored_last = int(data.get("last_extracted_message_count") or 0)
            user_count = int(data.get("user_message_count") or 0)
            if (
                stored_last <= 0
                and completed > 0
                and final_completed >= final_total > 0
                and user_count > 0
            ):
                updates["last_extracted_message_count"] = user_count

            if message_fingerprint:
                updates["current_message_fingerprint"] = message_fingerprint
            if user_message_count is not None and user_message_count > 0:
                if int(data.get("user_message_count") or 0) != user_message_count:
                    updates["user_message_count"] = user_message_count

            if not updates:
                return False

            updates["updated_at"] = now
            set_clause = ", ".join(f"{key} = ?" for key in updates)
            conn.execute(
                f"UPDATE workspace_analysis SET {set_clause} WHERE workspace_path = ?",
                (*updates.values(), workspace_path),
            )
            return True

    def get_analysis_status(self, workspace_path: str) -> Dict[str, Any]:
        data = self.get_workspace_analysis(workspace_path)
        if not data:
            return {
                "workspace_path": workspace_path,
                "status": ANALYSIS_STATUS_IDLE,
                "locked": False,
                "blocks_fine_count": 0,
                "blocks_modules_count": 0,
                "has_report": False,
            }
        locked = data.get("status") in ACTIVE_LOCK_STATUSES
        if locked and data.get("locked_at") and self._lock_stale(data["locked_at"]):
            locked = False
        last_extracted = int(data.get("last_extracted_message_count") or 0)
        user_count = int(data.get("user_message_count") or 0)
        extract_complete = bool(
            last_extracted > 0
            and user_count > 0
            and last_extracted >= user_count
        )
        return {
            "workspace_path": workspace_path,
            "status": data.get("status") or ANALYSIS_STATUS_IDLE,
            "stage": data.get("stage") or "",
            "stage_detail": data.get("stage_detail") or "",
            "progress_current": data.get("progress_current") or 0,
            "progress_total": data.get("progress_total") or 0,
            "locked": locked,
            "error_message": data.get("error_message") or "",
            "blocks_fine_count": len(data.get("blocks_fine") or []),
            "blocks_modules_count": len(data.get("blocks_modules") or []),
            "extract_total_batches": data.get("extract_total_batches") or 0,
            "extract_completed_batches": data.get("extract_completed_batches") or 0,
            "last_extracted_message_count": last_extracted,
            "user_message_count": user_count,
            "extract_complete": extract_complete,
            "has_report": bool(data.get("report_md")),
            "message_fingerprint": data.get("message_fingerprint"),
            "current_message_fingerprint": data.get("current_message_fingerprint") or "",
        }

    def save_workspace_report(self, workspace_path: str, payload: Dict[str, Any]) -> None:
        existing = self.get_workspace_analysis(workspace_path) or {}
        fingerprint = payload.get("message_fingerprint", existing.get("message_fingerprint", ""))
        if "blocks" in payload:
            self.save_blocks_modules(
                workspace_path,
                fingerprint,
                payload["blocks"],
                clear_report=bool(payload.get("clear_report_md")),
            )
        if "blocks_fine" in payload:
            self.save_blocks_fine(
                workspace_path,
                fingerprint,
                payload["blocks_fine"],
                int(payload.get("user_message_count") or existing.get("user_message_count") or 0),
            )
        if "report_md" in payload:
            self.save_analysis_report(
                workspace_path,
                fingerprint,
                payload["report_md"],
                int(payload.get("user_message_count") or existing.get("user_message_count") or 0),
            )

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_sync_state(self, source_path: str) -> Optional[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM sync_state WHERE source_path = ?", (source_path,)
            ).fetchone()

    def upsert_sync_state(
        self,
        source_path: str,
        conversation_id: str,
        source_type: str,
        file_mtime: float,
        file_size: int,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO sync_state (source_path, conversation_id, source_type, file_mtime, file_size, synced_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_path) DO UPDATE SET
                    conversation_id = excluded.conversation_id,
                    source_type = excluded.source_type,
                    file_mtime = excluded.file_mtime,
                    file_size = excluded.file_size,
                    synced_at = excluded.synced_at
                """,
                (source_path, conversation_id, source_type, file_mtime, file_size, utc_now()),
            )

    def save_conversation(self, conv: ParsedConversation) -> None:
        user_message_count = sum(1 for m in conv.messages if m.role == "user")
        with self.connect() as conn:
            conn.execute("DELETE FROM messages WHERE conversation_id = ?", (conv.conversation_id,))
            conn.execute(
                """
                INSERT INTO conversations (id, title, workspace_path, created_at, updated_at, parse_status, source_types, message_count, user_message_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    title = excluded.title,
                    workspace_path = excluded.workspace_path,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at,
                    parse_status = excluded.parse_status,
                    source_types = excluded.source_types,
                    message_count = excluded.message_count,
                    user_message_count = excluded.user_message_count
                """,
                (
                    conv.conversation_id,
                    conv.title,
                    conv.workspace_path,
                    conv.created_at,
                    conv.updated_at,
                    conv.parse_status,
                    json.dumps(conv.source_types, ensure_ascii=False),
                    len(conv.messages),
                    user_message_count,
                ),
            )
            for msg in conv.messages:
                images_json = None
                if msg.images:
                    images_json = json.dumps(msg.images, ensure_ascii=False)
                conn.execute(
                    """
                    INSERT INTO messages (
                        conversation_id, step_index, role, message_type, content,
                        thinking, tool_name, tool_args, created_at, source, is_truncated, images
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        conv.conversation_id,
                        msg.step_index,
                        msg.role,
                        msg.message_type,
                        msg.content,
                        msg.thinking,
                        msg.tool_name,
                        msg.tool_args,
                        msg.created_at,
                        msg.source,
                        1 if msg.is_truncated else 0,
                        images_json,
                    ),
                )

    def list_conversations(
        self,
        q: str = "",
        workspace: str = "",
        status: str = "",
        limit: int = 200,
        offset: int = 0,
        workspace_exact: bool = False,
    ) -> List[Dict[str, Any]]:
        sql = "SELECT * FROM conversations WHERE 1=1"
        params: List[Any] = []
        if q:
            sql += " AND (title LIKE ? OR id LIKE ? OR workspace_path LIKE ?)"
            like = f"%{q}%"
            params.extend([like, like, like])
        if workspace is not None and workspace != "":
            if workspace_exact:
                sql += " AND workspace_path = ?"
                params.append(workspace)
            else:
                sql += " AND workspace_path LIKE ?"
                params.append(f"%{workspace}%")
        elif workspace == "" and workspace_exact:
            sql += " AND (workspace_path = '' OR workspace_path IS NULL)"
        if status:
            sql += " AND parse_status = ?"
            params.append(status)
        sql += " ORDER BY COALESCE(updated_at, created_at, id) DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def list_conversations_by_workspace(
        self, workspace_path: str, q: str = "", status: str = ""
    ) -> List[Dict[str, Any]]:
        return self.list_conversations(
            q=q,
            workspace=workspace_path,
            status=status,
            limit=500,
            workspace_exact=True,
        )

    def count_conversations(self, status: str = "") -> int:
        with self.connect() as conn:
            if status:
                return conn.execute(
                    "SELECT COUNT(*) FROM conversations WHERE parse_status = ?", (status,)
                ).fetchone()[0]
            return conn.execute("SELECT COUNT(*) FROM conversations").fetchone()[0]

    def get_conversation(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM conversations WHERE id = ?", (conversation_id,)
            ).fetchone()
        return dict(row) if row else None

    def list_messages(self, conversation_id: str) -> List[Dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM messages WHERE conversation_id = ?
                ORDER BY step_index, id
                """,
                (conversation_id,),
            ).fetchall()
        return [self._row_to_message(dict(r)) for r in rows]

    def _row_to_message(self, row: Dict[str, Any]) -> Dict[str, Any]:
        images = row.get("images")
        if isinstance(images, str) and images:
            try:
                row["images"] = json.loads(images)
            except json.JSONDecodeError:
                row["images"] = None
        elif not images:
            row["images"] = None
        return row

    def list_workspaces(self, q: str = "", limit: int = 100) -> List[Dict[str, Any]]:
        base = """
            SELECT
                COALESCE(NULLIF(workspace_path, ''), '') AS workspace_path,
                COUNT(*) AS cnt,
                SUM(message_count) AS message_count,
                SUM(user_message_count) AS user_message_count,
                SUM(CASE WHEN parse_status = 'ok' THEN 1 ELSE 0 END) AS ok_count,
                MAX(updated_at) AS last_updated
            FROM conversations
            GROUP BY COALESCE(NULLIF(workspace_path, ''), '')
        """
        order = " ORDER BY (last_updated IS NULL), last_updated DESC, cnt DESC LIMIT ?"
        params: List[Any] = []
        if q:
            sql = f"SELECT * FROM ({base}) AS grouped WHERE workspace_path LIKE ?{order}"
            params.append(f"%{q}%")
        else:
            sql = base + order
        params.append(limit)
        with self.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

    def record_sync_run(
        self,
        mode: str,
        new_count: int,
        updated_count: int,
        skipped_count: int,
        error_count: int,
        message: str,
        run_id: Optional[int] = None,
    ) -> int:
        with self.connect() as conn:
            if run_id is None:
                cur = conn.execute(
                    """
                    INSERT INTO sync_runs (started_at, mode, new_count, updated_count, skipped_count, error_count, message)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (utc_now(), mode, new_count, updated_count, skipped_count, error_count, message),
                )
                return cur.lastrowid
            conn.execute(
                """
                UPDATE sync_runs SET finished_at = ?, new_count = ?, updated_count = ?,
                    skipped_count = ?, error_count = ?, message = ?
                WHERE id = ?
                """,
                (utc_now(), new_count, updated_count, skipped_count, error_count, message, run_id),
            )
            return run_id

    def last_sync_run(self) -> Optional[Dict[str, Any]]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM sync_runs ORDER BY id DESC LIMIT 1"
            ).fetchone()
        return dict(row) if row else None

    def stats(self) -> Dict[str, Any]:
        with self.connect() as conn:
            total = conn.execute("SELECT COUNT(*) FROM conversations").fetchone()[0]
            ok = conn.execute(
                "SELECT COUNT(*) FROM conversations WHERE parse_status = 'ok'"
            ).fetchone()[0]
            skipped = conn.execute(
                "SELECT COUNT(*) FROM conversations WHERE parse_status = 'encrypted_only'"
            ).fetchone()[0]
            msgs = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
            user_msgs = conn.execute(
                "SELECT COUNT(*) FROM messages WHERE role = 'user'"
            ).fetchone()[0]
        return {
            "total": total,
            "ok": ok,
            "encrypted_only": skipped,
            "messages": msgs,
            "user_messages": user_msgs,
        }
