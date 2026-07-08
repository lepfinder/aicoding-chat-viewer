"""从 Antigravity 本地文件解析会话与消息。"""

from __future__ import annotations

import json
import re
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import quote

from protobuf_util import extract_strings, parse_protobuf, protobuf_timestamp_to_iso

USER_REQUEST_RE = re.compile(r"<USER_REQUEST>\s*(.*?)\s*</USER_REQUEST>", re.DOTALL)
ACTIVE_DOC_RE = re.compile(r"Active Document:\s*([^\s(]+)")
PATH_IN_JSON_RE = re.compile(r'"(?:DirectoryPath|AbsolutePath|Cwd|SearchPath)"\s*:\s*"?(/[^"\\]+)"?')
FILE_URI_RE = re.compile(r"file://(/[^\s\"')]+)")
AT_PATH_RE = re.compile(r"@\[?(/[^\s\]\)]+)")
AT_IMAGE_RE = re.compile(
    r"@\[(/[^\]]+\.(?:png|jpe?g|gif|webp|svg))\]|@(/[^\s\]\)@]+\.(?:png|jpe?g|gif|webp|svg))",
    re.IGNORECASE,
)
BRAIN_MEDIA_RE = re.compile(
    r"(/Users[^\x00-\x1f]*?media__\d+\.(?:png|jpe?g|webp))",
    re.IGNORECASE,
)
IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg"}
PATH_TAIL_CHARS = ").,;]'\""


@dataclass
class ParsedMessage:
    step_index: int
    role: str
    message_type: str
    content: str
    created_at: Optional[str] = None
    thinking: Optional[str] = None
    tool_name: Optional[str] = None
    tool_args: Optional[str] = None
    source: str = ""
    is_truncated: bool = False
    images: Optional[List[Dict[str, Any]]] = None


@dataclass
class ParsedConversation:
    conversation_id: str
    title: str = ""
    workspace_path: str = ""
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    parse_status: str = "ok"
    source_types: List[str] = field(default_factory=list)
    messages: List[ParsedMessage] = field(default_factory=list)


def extract_user_request(content: str) -> str:
    match = USER_REQUEST_RE.search(content or "")
    if match:
        return match.group(1).strip()
    return (content or "").strip()


def image_src_for_path(path: str) -> str:
    return f"/ag-image?path={quote(path, safe='')}"


def resolve_ag_image_path(raw: str) -> Optional[Path]:
    if not raw:
        return None
    try:
        path = Path(raw).expanduser()
        if not path.is_absolute():
            return None
        resolved = path.resolve()
        if resolved.suffix.lower() not in IMAGE_SUFFIXES:
            return None
        if not resolved.is_file():
            return None
        home = Path.home().resolve()
        if not str(resolved).startswith(str(home)):
            return None
        return resolved
    except (OSError, ValueError):
        return None


def _is_image_path(path: str) -> bool:
    return Path(path).suffix.lower() in IMAGE_SUFFIXES


def extract_image_paths_from_user_text(text: str) -> List[str]:
    if not text:
        return []
    paths: List[str] = []
    seen: Set[str] = set()
    for groups in AT_IMAGE_RE.findall(text):
        for raw in groups:
            path = _normalize_abs_path(raw)
            if path and _is_image_path(path) and path not in seen:
                seen.add(path)
                paths.append(path)
    return paths


def extract_media_paths_from_blob(blob: bytes) -> List[str]:
    if not blob:
        return []
    text = blob.decode("utf-8", errors="ignore")
    paths: List[str] = []
    seen: Set[str] = set()
    for match in BRAIN_MEDIA_RE.finditer(text):
        path = match.group(1)
        if path not in seen:
            seen.add(path)
            paths.append(path)
    return paths


def load_db_user_media_by_step(conversation_id: str, ide_dir: Path) -> Dict[int, List[str]]:
    db_path = ide_dir / "conversations" / f"{conversation_id}.db"
    if not db_path.exists():
        return {}

    mapping: Dict[int, List[str]] = {}
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT idx, step_payload FROM steps WHERE step_payload IS NOT NULL ORDER BY idx"
        ).fetchall()
    finally:
        conn.close()

    for idx, payload in rows:
        if not payload:
            continue
        paths = extract_media_paths_from_blob(bytes(payload))
        if not paths:
            continue
        bucket = mapping.setdefault(int(idx), [])
        for path in paths:
            if path not in bucket:
                bucket.append(path)
    return mapping


def _resolve_image_entries(paths: List[str]) -> List[Dict[str, Any]]:
    images: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for path in paths:
        if path in seen:
            continue
        seen.add(path)
        file_path = Path(path)
        if not file_path.is_file():
            continue
        images.append(
            {
                "src": image_src_for_path(path),
                "width": None,
                "height": None,
            }
        )
    return images


def enrich_messages_images(
    messages: List[Dict[str, Any]],
    conversation_id: str,
    ide_dir: Path,
) -> None:
    """为已落库但缺少 images 字段的历史消息补全附图。"""
    db_media = load_db_user_media_by_step(conversation_id, ide_dir)
    for msg in messages:
        if msg.get("role") != "user" or msg.get("images"):
            continue
        paths = extract_image_paths_from_user_text(msg.get("content") or "")
        for path in db_media.get(int(msg.get("step_index", -1)), []):
            if path not in paths:
                paths.append(path)
        if paths:
            entries = _resolve_image_entries(paths)
            if entries:
                msg["images"] = entries


def attach_user_images(
    messages: List[ParsedMessage],
    conversation_id: str,
    ide_dir: Path,
) -> None:
    db_media = load_db_user_media_by_step(conversation_id, ide_dir)
    for msg in messages:
        if msg.role != "user":
            continue
        paths = extract_image_paths_from_user_text(msg.content)
        for path in db_media.get(msg.step_index, []):
            if path not in paths:
                paths.append(path)
        if paths:
            msg.images = _resolve_image_entries(paths)


def _normalize_abs_path(path: str) -> str:
    path = (path or "").strip().strip('"').strip(PATH_TAIL_CHARS)
    if path.startswith("file://"):
        path = path[7:].strip(PATH_TAIL_CHARS)
    return path


def project_root_from_path(path: str) -> Optional[str]:
    """将文件/目录路径归一到项目根：workspace 下取 bucket/project 两级。"""
    path = _normalize_abs_path(path)
    if not path.startswith("/"):
        return None

    parts = [p for p in path.split("/") if p != ""]
    # /Users/xiyangxie/workspace/<bucket>/<project>/...
    for i, part in enumerate(parts):
        if part != "workspace":
            continue
        prefix = "/" + "/".join(parts[:i])
        if i + 2 < len(parts):
            return f"{prefix}/{'/'.join(parts[i : i + 3])}"
        if i + 1 < len(parts):
            return f"{prefix}/{'/'.join(parts[i : i + 2])}"
        return f"{prefix}/workspace"

    # ~/.gemini/antigravity-ide 等
    for i, part in enumerate(parts):
        if part == ".gemini" and i + 1 < len(parts):
            return "/" + "/".join(parts[: i + 2])

    if len(parts) >= 3:
        return "/" + "/".join(parts[:3])
    return path


def _collect_path_hints(text: str) -> List[Tuple[str, int]]:
    hints: List[Tuple[str, int]] = []
    if not text:
        return hints
    for match in ACTIVE_DOC_RE.finditer(text):
        hints.append((_normalize_abs_path(match.group(1)), 10))
    for match in AT_PATH_RE.finditer(text):
        hints.append((_normalize_abs_path(match.group(1)), 9))
    for match in PATH_IN_JSON_RE.finditer(text):
        hints.append((_normalize_abs_path(match.group(1)), 8))
    for match in FILE_URI_RE.finditer(text):
        hints.append((_normalize_abs_path(match.group(1)), 3))
    return hints


def infer_workspace_from_text(text: str) -> Optional[str]:
    for path, _weight in _collect_path_hints(text):
        root = project_root_from_path(path)
        if root:
            return root
    return None


def infer_workspace_from_messages(messages: List[ParsedMessage]) -> str:
    scores: Dict[str, float] = {}
    seen_first_user = False

    for msg in messages:
        if msg.role == "user" and not seen_first_user:
            seen_first_user = True
            role_mul = 2.0
        elif msg.role == "user":
            role_mul = 1.5
        elif msg.role in ("assistant_tool",):
            role_mul = 1.2
        else:
            role_mul = 1.0

        for text, text_mul in ((msg.content, 1.0), (msg.tool_args, 1.1)):
            if not text:
                continue
            for path, hint_weight in _collect_path_hints(text):
                root = project_root_from_path(path)
                if root:
                    scores[root] = scores.get(root, 0.0) + hint_weight * role_mul * text_mul

    if not scores:
        return ""
    return max(scores.items(), key=lambda item: item[1])[0]


def load_summaries(legacy_dir: Path) -> Dict[str, Dict[str, Any]]:
    path = legacy_dir / "agyhub_summaries_proto.pb"
    if not path.exists():
        return {}

    summaries: Dict[str, Dict[str, Any]] = {}
    for item in parse_protobuf(path.read_bytes()):
        if item.get("field") != 1 or "message" not in item:
            continue
        summary = _parse_summary(item["message"])
        if summary.get("conversation_id"):
            summaries[summary["conversation_id"]] = summary
    return summaries


def _parse_summary(fields: List[Dict[str, Any]]) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "conversation_id": "",
        "title": "",
        "created_at": None,
        "updated_at": None,
        "workspace_paths": [],
    }
    workspaces: List[str] = []
    for item in fields:
        fnum = item.get("field")
        if fnum == 1 and "string" in item:
            result["conversation_id"] = item["string"]
        elif fnum == 2 and "message" in item:
            for sub in item["message"]:
                sf = sub.get("field")
                if sf == 1 and "string" in sub:
                    result["title"] = sub["string"]
                elif sf == 3 and "message" in sub:
                    ts = protobuf_timestamp_to_iso(sub["message"])
                    if ts:
                        result["created_at"] = ts
                elif sf == 7 and "message" in sub:
                    ts = protobuf_timestamp_to_iso(sub["message"])
                    if ts:
                        result["updated_at"] = ts
                elif sf == 9 and "message" in sub:
                    for ws_field in sub["message"]:
                        if ws_field.get("field") == 1 and "string" in ws_field:
                            p = ws_field["string"]
                            if p.startswith("file://"):
                                p = p[7:]
                            workspaces.append(p)
                elif sf == 10 and "message" in sub:
                    ts = protobuf_timestamp_to_iso(sub["message"])
                    if ts and not result["updated_at"]:
                        result["updated_at"] = ts
    result["workspace_paths"] = sorted(set(workspaces))
    return result


def parse_jsonl_file(path: Path, source: str) -> List[ParsedMessage]:
    messages: List[ParsedMessage] = []
    if not path.exists():
        return messages

    for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        messages.extend(_json_step_to_messages(obj, source))
    return messages


def _json_step_to_messages(obj: Dict[str, Any], source: str) -> List[ParsedMessage]:
    step_index = int(obj.get("step_index", 0))
    step_type = str(obj.get("type", ""))
    created_at = obj.get("created_at")
    truncated = "<truncated" in json.dumps(obj, ensure_ascii=False).lower()
    out: List[ParsedMessage] = []

    if step_type == "USER_INPUT" and obj.get("content"):
        out.append(
            ParsedMessage(
                step_index=step_index,
                role="user",
                message_type=step_type,
                content=extract_user_request(obj["content"]),
                created_at=created_at,
                source=source,
                is_truncated=truncated,
            )
        )
    elif step_type == "PLANNER_RESPONSE":
        if obj.get("tool_calls"):
            for call in obj["tool_calls"]:
                args = call.get("args", {})
                out.append(
                    ParsedMessage(
                        step_index=step_index,
                        role="assistant_tool",
                        message_type=step_type,
                        content=json.dumps(args, ensure_ascii=False, indent=2),
                        created_at=created_at,
                        tool_name=str(call.get("name", "")),
                        tool_args=json.dumps(args, ensure_ascii=False),
                        source=source,
                        is_truncated=truncated,
                    )
                )
        text = obj.get("content") or obj.get("text")
        thinking = obj.get("thinking")
        if text:
            out.append(
                ParsedMessage(
                    step_index=step_index,
                    role="assistant",
                    message_type=step_type,
                    content=text,
                    created_at=created_at,
                    thinking=thinking,
                    source=source,
                    is_truncated=truncated,
                )
            )
        elif thinking:
            out.append(
                ParsedMessage(
                    step_index=step_index,
                    role="assistant",
                    message_type="THINKING",
                    content="",
                    created_at=created_at,
                    thinking=thinking,
                    source=source,
                )
            )
    elif obj.get("content") and step_type not in ("CONVERSATION_HISTORY", "KNOWLEDGE_ARTIFACTS"):
        role = "system" if obj.get("source") == "SYSTEM" else "other"
        if step_type in ("VIEW_FILE", "RUN_COMMAND", "CODE_ACTION"):
            role = "tool_event"
        out.append(
            ParsedMessage(
                step_index=step_index,
                role=role,
                message_type=step_type,
                content=obj["content"],
                created_at=created_at,
                source=source,
                is_truncated=truncated,
            )
        )
    return out


def parse_conversation_db(path: Path) -> List[ParsedMessage]:
    """从 Antigravity SQLite 会话库粗粒度提取消息（仅 db 无 jsonl 时使用）。"""
    messages: List[ParsedMessage] = []
    if not path.exists():
        return messages

    conn = sqlite3.connect(path)
    try:
        rows = conn.execute(
            "SELECT idx, step_type, step_payload FROM steps ORDER BY idx"
        ).fetchall()
    finally:
        conn.close()

    for idx, step_type, payload in rows:
        if not payload:
            continue
        blob = bytes(payload)
        text_blob = blob.decode("utf-8", errors="ignore")
        if "USER_REQUEST" in text_blob:
            match = USER_REQUEST_RE.search(text_blob)
            if match:
                content = match.group(1).strip()
                paths = extract_image_paths_from_user_text(content)
                paths.extend(extract_media_paths_from_blob(blob))
                images = _resolve_image_entries(paths) or None
                messages.append(
                    ParsedMessage(
                        step_index=idx,
                        role="user",
                        message_type="USER_INPUT",
                        content=content,
                        source="sqlite_db",
                        images=images,
                    )
                )
                continue
        strings = extract_strings(parse_protobuf(blob), min_len=6)
        tool_name = None
        content_parts: List[str] = []
        for s in strings:
            if s in ("write_to_file", "run_command", "view_file", "list_dir", "grep_search", "replace_file_content"):
                tool_name = s
                continue
            if s.startswith("{") and ("toolAction" in s or "CommandLine" in s):
                content_parts.append(s)
            elif len(s) > 40 and not s.startswith("$") and "@" not in s[:5]:
                content_parts.append(s)
        if tool_name:
            messages.append(
                ParsedMessage(
                    step_index=idx,
                    role="assistant_tool",
                    message_type=f"DB_STEP_{step_type}",
                    content="\n".join(content_parts) or text_blob[:2000],
                    tool_name=tool_name,
                    source="sqlite_db",
                )
            )
        elif content_parts:
            messages.append(
                ParsedMessage(
                    step_index=idx,
                    role="assistant",
                    message_type=f"DB_STEP_{step_type}",
                    content="\n".join(content_parts)[:8000],
                    source="sqlite_db",
                )
            )
    return messages


def merge_messages(sources: List[Tuple[str, List[ParsedMessage]]]) -> List[ParsedMessage]:
    """多源合并：transcript > overview > sqlite_db。"""
    priority = {"transcript": 3, "overview": 2, "sqlite_db": 1}
    by_key: Dict[Tuple[int, str, str, str], ParsedMessage] = {}

    for source_name, msgs in sources:
        pri = priority.get(source_name, 0)
        for msg in msgs:
            key = (msg.step_index, msg.role, msg.message_type, msg.tool_name or "")
            existing = by_key.get(key)
            if existing is None or pri > priority.get(existing.source, 0):
                by_key[key] = msg
            elif pri == priority.get(existing.source, 0):
                if msg.role == "assistant" and msg.content and not existing.content:
                    by_key[key] = msg

    return sorted(by_key.values(), key=lambda m: (m.step_index, m.role))


def discover_conversation_ids(ide_dir: Path) -> Set[str]:
    ids: Set[str] = set()
    conv = ide_dir / "conversations"
    if conv.exists():
        for p in conv.glob("*.pb"):
            ids.add(p.stem)
        for p in conv.glob("*.db"):
            ids.add(p.stem)
    brain = ide_dir / "brain"
    if brain.exists():
        for child in brain.iterdir():
            if child.is_dir():
                ids.add(child.name)
    return ids


def has_readable_sources(conversation_id: str, ide_dir: Path) -> List[str]:
    sources: List[str] = []
    base = ide_dir / "brain" / conversation_id / ".system_generated" / "logs"
    if (base / "overview.txt").exists():
        sources.append("overview")
    if (base / "transcript.jsonl").exists():
        sources.append("transcript")
    if (ide_dir / "conversations" / f"{conversation_id}.db").exists():
        sources.append("sqlite_db")
    return sources


def parse_conversation(
    conversation_id: str,
    ide_dir: Path,
    summaries: Dict[str, Dict[str, Any]],
) -> ParsedConversation:
    source_types = has_readable_sources(conversation_id, ide_dir)
    pb_exists = (ide_dir / "conversations" / f"{conversation_id}.pb").exists()

    if not source_types:
        status = "encrypted_only" if pb_exists else "empty"
        meta = summaries.get(conversation_id, {})
        return ParsedConversation(
            conversation_id=conversation_id,
            title=meta.get("title", ""),
            workspace_path=_workspace_from_summary(meta),
            created_at=meta.get("created_at"),
            updated_at=meta.get("updated_at"),
            parse_status=status,
            source_types=[],
        )

    base = ide_dir / "brain" / conversation_id / ".system_generated" / "logs"
    source_msgs: List[Tuple[str, List[ParsedMessage]]] = []

    if "transcript" in source_types:
        source_msgs.append(("transcript", parse_jsonl_file(base / "transcript.jsonl", "transcript")))
    if "overview" in source_types:
        source_msgs.append(("overview", parse_jsonl_file(base / "overview.txt", "overview")))
    if "sqlite_db" in source_types and "transcript" not in source_types and "overview" not in source_types:
        db_path = ide_dir / "conversations" / f"{conversation_id}.db"
        source_msgs.append(("sqlite_db", parse_conversation_db(db_path)))

    messages = merge_messages(source_msgs)
    attach_user_images(messages, conversation_id, ide_dir)
    meta = summaries.get(conversation_id, {})

    workspace = _workspace_from_summary(meta)
    if not workspace:
        workspace = infer_workspace_from_messages(messages)

    created_at = meta.get("created_at")
    updated_at = meta.get("updated_at")
    for msg in messages:
        if msg.created_at:
            if not created_at or msg.created_at < created_at:
                created_at = msg.created_at
            if not updated_at or msg.created_at > updated_at:
                updated_at = msg.created_at

    title = meta.get("title", "")
    if not title:
        for msg in messages:
            if msg.role == "user" and msg.content:
                title = msg.content.replace("\n", " ")[:80]
                break
    if not title:
        title = conversation_id

    return ParsedConversation(
        conversation_id=conversation_id,
        title=title,
        workspace_path=workspace or "",
        created_at=created_at,
        updated_at=updated_at,
        parse_status="ok",
        source_types=source_types,
        messages=messages,
    )


def _workspace_from_summary(meta: Dict[str, Any]) -> str:
    paths = meta.get("workspace_paths") or []
    if not paths:
        return ""
    p = paths[0]
    if p.startswith("file://"):
        p = p[7:]
    return project_root_from_path(p) or p


def collect_source_files(conversation_id: str, ide_dir: Path) -> List[Path]:
    files: List[Path] = []
    base = ide_dir / "brain" / conversation_id / ".system_generated" / "logs"
    for name in ("overview.txt", "transcript.jsonl"):
        p = base / name
        if p.exists():
            files.append(p)
    db = ide_dir / "conversations" / f"{conversation_id}.db"
    if db.exists():
        files.append(db)
    return files
