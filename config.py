import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

APP_DIR = Path(__file__).resolve().parent

_DEFAULT_IDE_DATA_DIR = Path.home() / ".gemini" / "antigravity-ide"
_DEFAULT_LEGACY_DATA_DIR = Path.home() / ".gemini" / "antigravity"
_DEFAULT_DB_PATH = APP_DIR / "data" / "antigravity_chats.db"
_DEFAULT_CURSOR_USER_DIR = Path.home() / "Library/Application Support/Cursor/User"
_DEFAULT_CURSOR_PROJECTS_DIR = Path.home() / ".cursor" / "projects"


def _resolve_path(env_key: str, default: Path) -> Path:
    """从环境变量读取路径；支持 ~ 与相对路径（相对项目根目录）。"""
    raw = os.environ.get(env_key, "").strip()
    if not raw:
        return default
    path = Path(raw).expanduser()
    if path.is_absolute():
        return path
    return (APP_DIR / path).resolve()


IDE_DATA_DIR = _resolve_path("ANTIGRAVITY_DATA_DIR", _DEFAULT_IDE_DATA_DIR)
LEGACY_DATA_DIR = _resolve_path("ANTIGRAVITY_LEGACY_DATA_DIR", _DEFAULT_LEGACY_DATA_DIR)
DB_PATH = _resolve_path("CHAT_VIEWER_DB_PATH", _DEFAULT_DB_PATH)

CURSOR_USER_DIR = _resolve_path("CURSOR_USER_DIR", _DEFAULT_CURSOR_USER_DIR)
CURSOR_DB_PATH = _resolve_path(
    "CURSOR_DB_PATH",
    CURSOR_USER_DIR / "globalStorage" / "state.vscdb",
)
CURSOR_WS_STORAGE_DIR = _resolve_path(
    "CURSOR_WS_STORAGE_DIR",
    CURSOR_USER_DIR / "workspaceStorage",
)
CURSOR_PROJECTS_DIR = _resolve_path("CURSOR_PROJECTS_DIR", _DEFAULT_CURSOR_PROJECTS_DIR)

HOST = os.environ.get("CHAT_VIEWER_HOST", "127.0.0.1").strip() or "127.0.0.1"
PORT = int(os.environ.get("CHAT_VIEWER_PORT", "8788"))

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")
OPENAI_BASE_URL = os.environ.get(
    "OPENAI_BASE_URL", "https://ark.cn-beijing.volces.com/api/coding/v3"
)
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "doubao-seed-1-8-251228")
# 默认保留 thinking；仅当显式设为 true 时全程关闭思考
OPENAI_DISABLE_THINKING = os.environ.get("OPENAI_DISABLE_THINKING", "false").lower() in (
    "1",
    "true",
    "yes",
)
# content 为空或截断时，是否在最后尝试 enable_thinking=false
OPENAI_THINKING_FALLBACK = os.environ.get("OPENAI_THINKING_FALLBACK", "true").lower() in (
    "1",
    "true",
    "yes",
)


def _optional_int(env_key: str, default: int) -> int:
    raw = os.environ.get(env_key, "").strip()
    if not raw:
        return default
    return int(raw)


def _optional_max_tokens(env_key: str = "OPENAI_MAX_TOKENS") -> int | None:
    """未设置或为 0/none/unlimited 时不限制输出长度（不传 max_tokens）。"""
    raw = os.environ.get(env_key, "").strip()
    if not raw or raw.lower() in ("0", "none", "unlimited", "max"):
        return None
    return int(raw)


OPENAI_MAX_TOKENS = _optional_max_tokens()
OPENAI_MAX_TOKENS_JSON = _optional_max_tokens("OPENAI_MAX_TOKENS_JSON") or OPENAI_MAX_TOKENS
OPENAI_MAX_TOKENS_REPORT = _optional_max_tokens("OPENAI_MAX_TOKENS_REPORT") or OPENAI_MAX_TOKENS
# 截断 / thinking 占满预算时的重试上限
OPENAI_RETRY_MAX_TOKENS = _optional_int("OPENAI_RETRY_MAX_TOKENS", 32768)
