"""只读直连 Cursor globalStorage/state.vscdb，按需解析会话与消息。"""

from __future__ import annotations

import base64
import json
import re
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import unquote

from config import CURSOR_DB_PATH, CURSOR_PROJECTS_DIR, CURSOR_WS_STORAGE_DIR
from parser import infer_workspace_from_text, project_root_from_path

CURSOR_ID_PREFIX = "cursor:"
UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)
IMAGE_FILE_RE = re.compile(
    r"^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
    r"-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.(?:png|jpe?g|webp)$",
    re.IGNORECASE,
)


def is_cursor_conversation_id(conversation_id: str) -> bool:
    return (conversation_id or "").startswith(CURSOR_ID_PREFIX)


def strip_cursor_prefix(conversation_id: str) -> str:
    if is_cursor_conversation_id(conversation_id):
        return conversation_id[len(CURSOR_ID_PREFIX) :]
    return conversation_id


def make_cursor_conversation_id(composer_id: str) -> str:
    return f"{CURSOR_ID_PREFIX}{composer_id}"


def _normalize_timestamp(value: Any) -> Optional[str]:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            datetime.fromisoformat(text.replace("Z", "+00:00"))
            return text
        except ValueError:
            pass
        try:
            ms = int(text)
        except ValueError:
            return None
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def _bytes_from_indexed_dict(data: Dict[str, Any]) -> bytes:
    keys = sorted(int(k) for k in data.keys())
    return bytes(data[str(k)] if str(k) in data else data[k] for k in keys)


def _mime_from_bytes(raw: bytes) -> str:
    if raw[:3] == b"\xff\xd8\xff":
        return "image/jpeg"
    if raw[:8] == b"\x89PNG\r\n\x1a\n":
        return "image/png"
    if raw[:6] in (b"GIF87a", b"GIF89a"):
        return "image/gif"
    if len(raw) >= 12 and raw[:4] == b"RIFF" and raw[8:12] == b"WEBP":
        return "image/webp"
    return "application/octet-stream"


def _decode_folder_uri(uri: str) -> str:
    if not uri:
        return ""
    if uri.startswith("file://"):
        return unquote(uri[7:])
    return uri


def _workspace_from_identifier(
    data: Dict[str, Any], hash_to_folder: Dict[str, str]
) -> str:
    workspace_id = data.get("workspaceIdentifier") or {}
    wid = workspace_id.get("id")
    if wid and wid in hash_to_folder:
        return hash_to_folder[wid]

    config_path = workspace_id.get("configPath") or {}
    for key in ("fsPath", "path", "external"):
        raw = config_path.get(key) or ""
        if not raw:
            continue
        if raw.startswith("file://"):
            raw = unquote(raw[7:])
        root = project_root_from_path(raw) or raw
        if root:
            return root
    return ""


def _bubble_path_fields(bubble: Dict[str, Any]) -> List[str]:
    chunks = [bubble.get("text") or ""]
    for key in (
        "relevantFiles",
        "attachedFolders",
        "recentlyViewedFiles",
        "codebaseContextChunks",
        "attachedFoldersNew",
        "docsReferences",
    ):
        value = bubble.get(key)
        if value:
            chunks.append(json.dumps(value, ensure_ascii=False))
    tool_results = bubble.get("toolResults")
    if tool_results:
        chunks.append(json.dumps(tool_results, ensure_ascii=False))
    return chunks


class CursorReader:
    def __init__(
        self,
        db_path: Path = CURSOR_DB_PATH,
        ws_storage_dir: Path = CURSOR_WS_STORAGE_DIR,
        projects_dir: Path = CURSOR_PROJECTS_DIR,
        cache_ttl: int = 600,
    ) -> None:
        self.db_path = db_path
        self.ws_storage_dir = ws_storage_dir
        self.projects_dir = projects_dir
        self.cache_ttl = cache_ttl
        self._index_cache: Optional[Tuple[float, List[Dict[str, Any]]]] = None
        self._hash_to_folder: Optional[Dict[str, str]] = None
        self._uuid_to_folder: Optional[Dict[str, str]] = None
        self._slug_to_folder: Optional[Dict[str, str]] = None
        self._image_path_index: Optional[Dict[str, Path]] = None

    def available(self) -> bool:
        return self.db_path.exists()

    def connect(self) -> sqlite3.Connection:
        uri = f"file:{self.db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True)
        conn.row_factory = sqlite3.Row
        return conn

    def _load_hash_to_folder(self) -> Dict[str, str]:
        if self._hash_to_folder is not None:
            return self._hash_to_folder

        mapping: Dict[str, str] = {}
        if not self.ws_storage_dir.exists():
            self._hash_to_folder = mapping
            return mapping

        for dpath in self.ws_storage_dir.iterdir():
            if not dpath.is_dir():
                continue
            wj = dpath / "workspace.json"
            if not wj.exists():
                continue
            try:
                data = json.loads(wj.read_text(encoding="utf-8"))
                folder = _decode_folder_uri(data.get("folder") or data.get("workspace") or "")
                root = project_root_from_path(folder) or folder
                if root:
                    mapping[dpath.name] = root
            except (json.JSONDecodeError, OSError):
                continue

        self._hash_to_folder = mapping
        return mapping

    def _load_uuid_to_folder(self) -> Dict[str, str]:
        if self._uuid_to_folder is not None:
            return self._uuid_to_folder

        mapping: Dict[str, str] = {}
        hash_map = self._load_hash_to_folder()
        if not self.ws_storage_dir.exists():
            self._uuid_to_folder = mapping
            return mapping

        for dpath in self.ws_storage_dir.iterdir():
            root = hash_map.get(dpath.name, "")
            if not root:
                continue
            db = dpath / "state.vscdb"
            if not db.exists():
                continue
            try:
                conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
                rows = conn.execute(
                    """
                    SELECT value FROM ItemTable
                    WHERE value IS NOT NULL AND length(value) BETWEEN 10 AND 500000
                    """
                ).fetchall()
                conn.close()
                for row in rows:
                    if not row[0]:
                        continue
                    for uid in UUID_RE.findall(row[0]):
                        mapping[uid] = root
            except (sqlite3.Error, OSError):
                continue

        self._uuid_to_folder = mapping
        return mapping

    def _load_slug_to_folder(self) -> Dict[str, str]:
        if self._slug_to_folder is not None:
            return self._slug_to_folder

        mapping: Dict[str, str] = {}
        projects_dir = CURSOR_PROJECTS_DIR
        if projects_dir.exists():
            for proj in projects_dir.iterdir():
                if not proj.is_dir() or not proj.name.startswith("Users-"):
                    continue
                slug_path = "/" + proj.name.replace("-", "/")
                root = project_root_from_path(slug_path) or ""
                for jsonl in proj.glob("agent-transcripts/*/*.jsonl"):
                    if "subagents" in jsonl.parts:
                        continue
                    mapping[jsonl.stem] = root

        self._slug_to_folder = mapping
        return mapping

    def _workspace_from_identifier(self, data: Dict[str, Any]) -> str:
        wi = data.get("workspaceIdentifier") or {}
        wid = wi.get("id") or ""
        if wid:
            hit = self._load_hash_to_folder().get(wid, "")
            if hit:
                return hit

        config = wi.get("configPath") or {}
        fs_path = config.get("fsPath") or config.get("path") or ""
        if fs_path:
            return project_root_from_path(fs_path) or fs_path
        return ""

    def _infer_from_bubbles(self, conn: sqlite3.Connection, composer_id: str) -> str:
        scores: Dict[str, int] = {}
        rows = conn.execute(
            "SELECT value FROM cursorDiskKV WHERE key LIKE ? LIMIT 200",
            (f"bubbleId:{composer_id}:%",),
        ).fetchall()
        for row in rows:
            if not row[0]:
                continue
            try:
                bubble = json.loads(row[0])
            except json.JSONDecodeError:
                continue

            chunks: List[str] = [bubble.get("text") or ""]
            for key in (
                "relevantFiles",
                "attachedFolders",
                "recentlyViewedFiles",
                "codebaseContextChunks",
                "toolResults",
            ):
                value = bubble.get(key)
                if value:
                    try:
                        chunks.append(json.dumps(value, ensure_ascii=False))
                    except (TypeError, ValueError):
                        chunks.append(str(value))

            for chunk in chunks:
                workspace = infer_workspace_from_text(chunk)
                if workspace:
                    scores[workspace] = scores.get(workspace, 0) + 1

        if not scores:
            return ""
        return max(scores.items(), key=lambda item: item[1])[0]

    def _infer_workspace(self, conn: sqlite3.Connection, composer_id: str, data: Dict[str, Any]) -> str:
        for resolver in (
            lambda: self._load_uuid_to_folder().get(composer_id, ""),
            lambda: self._load_slug_to_folder().get(composer_id, ""),
            lambda: self._workspace_from_identifier(data),
            lambda: self._infer_from_bubbles(conn, composer_id),
        ):
            workspace = resolver()
            if workspace:
                return workspace
        return ""

    def _build_index(self) -> List[Dict[str, Any]]:
        now = time.time()
        if self._index_cache and now - self._index_cache[0] < self.cache_ttl:
            return self._index_cache[1]

        if not self.available():
            self._index_cache = (now, [])
            return []

        items: List[Dict[str, Any]] = []
        try:
            conn = self.connect()
            rows = conn.execute(
                "SELECT key, value FROM cursorDiskKV WHERE key LIKE 'composerData:%'"
            ).fetchall()
            for row in rows:
                if not row["value"]:
                    continue
                try:
                    data = json.loads(row["value"])
                except json.JSONDecodeError:
                    continue
                headers = data.get("fullConversationHeadersOnly") or []
                if not headers:
                    continue

                composer_id = row["key"].split(":", 1)[1]
                user_count = sum(1 for h in headers if h.get("type") == 1)
                workspace = self._infer_workspace(conn, composer_id, data)
                title = (data.get("name") or "").strip() or composer_id
                created_at = _normalize_timestamp(data.get("createdAt"))
                updated_at = _normalize_timestamp(data.get("lastUpdatedAt") or data.get("conversationCheckpointLastUpdatedAt"))

                items.append(
                    {
                        "id": make_cursor_conversation_id(composer_id),
                        "composer_id": composer_id,
                        "title": title,
                        "workspace_path": workspace,
                        "created_at": created_at,
                        "updated_at": updated_at,
                        "parse_status": "ok",
                        "source_app": "cursor",
                        "message_count": len(headers),
                        "user_message_count": user_count,
                    }
                )
            conn.close()
        except sqlite3.Error:
            items = []

        items.sort(key=lambda x: x.get("updated_at") or x.get("created_at") or "", reverse=True)
        self._index_cache = (now, items)
        return items

    def invalidate_cache(self) -> None:
        self._index_cache = None
        self._hash_to_folder = None
        self._uuid_to_folder = None
        self._slug_to_folder = None
        self._image_path_index = None

    def _load_image_path_index(self) -> Dict[str, Path]:
        if self._image_path_index is not None:
            return self._image_path_index

        index: Dict[str, Path] = {}
        if self.ws_storage_dir.exists():
            for path in self.ws_storage_dir.glob("*/images/*"):
                if not path.is_file():
                    continue
                match = IMAGE_FILE_RE.match(path.name)
                if match:
                    index[match.group(1).lower()] = path

        self._image_path_index = index
        return index

    def get_image_path(self, image_uuid: str) -> Optional[Path]:
        if not image_uuid or not UUID_RE.fullmatch(image_uuid):
            return None
        path = self._load_image_path_index().get(image_uuid.lower())
        if path and path.is_file():
            return path
        return None

    def _resolve_bubble_image(self, image: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        dimension = image.get("dimension") or {}
        width = dimension.get("width")
        height = dimension.get("height")
        image_uuid = (image.get("uuid") or "").lower()

        data = image.get("data")
        if isinstance(data, dict) and data:
            raw = _bytes_from_indexed_dict(data)
            if raw:
                mime = _mime_from_bytes(raw)
                encoded = base64.b64encode(raw).decode("ascii")
                return {
                    "src": f"data:{mime};base64,{encoded}",
                    "width": width,
                    "height": height,
                }

        if image_uuid:
            if self.get_image_path(image_uuid):
                return {
                    "src": f"/cursor-image/{image_uuid}",
                    "width": width,
                    "height": height,
                }

        return None

    def _extract_bubble_images(self, bubble: Dict[str, Any]) -> List[Dict[str, Any]]:
        images: List[Dict[str, Any]] = []
        for image in bubble.get("images") or []:
            if not isinstance(image, dict):
                continue
            resolved = self._resolve_bubble_image(image)
            if resolved:
                images.append(resolved)
        return images

    def stats(self) -> Dict[str, int]:
        index = self._build_index()
        return {
            "total": len(index),
            "ok": len(index),
            "messages": sum(x["message_count"] for x in index),
            "user_messages": sum(x["user_message_count"] for x in index),
        }

    def list_workspaces(self, q: str = "") -> List[Dict[str, Any]]:
        buckets: Dict[str, Dict[str, Any]] = {}
        for item in self._build_index():
            path = item.get("workspace_path") or ""
            if path not in buckets:
                buckets[path] = {
                    "workspace_path": path,
                    "cnt": 0,
                    "message_count": 0,
                    "user_message_count": 0,
                    "ok_count": 0,
                    "last_updated": None,
                }
            bucket = buckets[path]
            bucket["cnt"] += 1
            bucket["message_count"] += item["message_count"]
            bucket["user_message_count"] += item["user_message_count"]
            bucket["ok_count"] += 1
            ts = item.get("updated_at")
            if ts and (not bucket["last_updated"] or ts > bucket["last_updated"]):
                bucket["last_updated"] = ts

        rows = list(buckets.values())
        if q:
            q_lower = q.lower()
            rows = [r for r in rows if q_lower in (r["workspace_path"] or "").lower()]
        rows.sort(key=lambda x: (x.get("last_updated") is None, x.get("last_updated") or ""), reverse=True)
        return rows

    def list_conversations(self, workspace_path: str = "", q: str = "") -> List[Dict[str, Any]]:
        rows = self._build_index()
        if workspace_path is not None:
            rows = [r for r in rows if (r.get("workspace_path") or "") == workspace_path]
        if q:
            like = q.lower()
            rows = [
                r
                for r in rows
                if like in (r.get("title") or "").lower()
                or like in r["id"].lower()
                or like in (r.get("workspace_path") or "").lower()
            ]
        return rows

    def get_conversation(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        cid = strip_cursor_prefix(conversation_id)
        for item in self._build_index():
            if item["composer_id"] == cid or item["id"] == conversation_id:
                return dict(item)
        return None

    def _bubble_role(self, bubble: Dict[str, Any], header_type: Optional[int]) -> str:
        bubble_type = bubble.get("type")
        if bubble_type == 1 or header_type == 1:
            return "user"
        tool_results = bubble.get("toolResults") or []
        if tool_results:
            return "assistant_tool"
        if bubble.get("text"):
            return "assistant"
        return "other"

    def list_messages(self, conversation_id: str) -> List[Dict[str, Any]]:
        composer_id = strip_cursor_prefix(conversation_id)
        if not self.available():
            return []

        messages: List[Dict[str, Any]] = []
        try:
            conn = self.connect()
            row = conn.execute(
                "SELECT value FROM cursorDiskKV WHERE key = ?",
                (f"composerData:{composer_id}",),
            ).fetchone()
            if not row or not row[0]:
                conn.close()
                return []

            data = json.loads(row[0])
            headers = data.get("fullConversationHeadersOnly") or []
            bubble_rows = conn.execute(
                "SELECT key, value FROM cursorDiskKV WHERE key LIKE ?",
                (f"bubbleId:{composer_id}:%",),
            ).fetchall()
            conn.close()
            bubble_map = {
                r["key"].split(":", 2)[2]: json.loads(r["value"])
                for r in bubble_rows
                if r["value"]
            }
        except (sqlite3.Error, json.JSONDecodeError):
            return []

        for step_index, header in enumerate(headers):
            bubble_id = header.get("bubbleId")
            if not bubble_id or bubble_id not in bubble_map:
                continue
            bubble = bubble_map[bubble_id]
            text = (bubble.get("text") or "").strip()
            thinking_blocks = bubble.get("allThinkingBlocks") or []
            thinking = ""
            if thinking_blocks:
                thinking = "\n\n".join(
                    block.get("text", "") if isinstance(block, dict) else str(block)
                    for block in thinking_blocks
                    if block
                ).strip()

            tool_name = None
            tool_args = None
            tool_results = bubble.get("toolResults") or []
            if tool_results:
                tool_name = "tool"
                try:
                    tool_args = json.dumps(tool_results, ensure_ascii=False, indent=2)
                except (TypeError, ValueError):
                    tool_args = str(tool_results)

            role = self._bubble_role(bubble, header.get("type"))
            images = self._extract_bubble_images(bubble) if role == "user" else []
            if role == "other" and not text and not thinking and not tool_args:
                continue

            content = text
            if not content and tool_args and role == "assistant_tool":
                content = tool_args

            if role == "user" and not content and not images:
                continue

            messages.append(
                {
                    "step_index": step_index,
                    "role": role,
                    "message_type": f"cursor_bubble_{bubble.get('type', '')}",
                    "content": content,
                    "thinking": thinking or None,
                    "tool_name": tool_name,
                    "tool_args": tool_args,
                    "created_at": _normalize_timestamp(bubble.get("createdAt")),
                    "source": "cursor_vscdb",
                    "is_truncated": False,
                    "images": images or None,
                }
            )

        return messages
