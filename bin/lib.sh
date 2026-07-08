#!/usr/bin/env bash
# shellcheck disable=SC2034
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PID_FILE="${ROOT_DIR}/data/chat-viewer.pid"
LOG_FILE="${ROOT_DIR}/data/chat-viewer.log"
HOST="${CHAT_VIEWER_HOST:-127.0.0.1}"
PORT="${CHAT_VIEWER_PORT:-8788}"
PYTHON="${ROOT_DIR}/.venv/bin/python"

ensure_venv() {
  if [[ ! -x "$PYTHON" ]]; then
    echo "未找到虚拟环境: ${ROOT_DIR}/.venv"
    echo "请先执行:"
    echo "  cd ${ROOT_DIR}"
    echo "  python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
    exit 1
  fi
}

pid_alive() {
  local pid="$1"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

port_pids() {
  lsof -ti ":${PORT}" 2>/dev/null || true
}

is_running() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if pid_alive "$pid"; then
      return 0
    fi
  fi
  [[ -n "$(port_pids)" ]]
}

running_pid() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if pid_alive "$pid"; then
      echo "$pid"
      return 0
    fi
  fi
  port_pids | head -n 1
}
