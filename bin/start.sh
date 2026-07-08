#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

if is_running; then
  pid="$(running_pid)"
  echo "chat-viewer 已在运行 (PID ${pid})"
  echo "访问: http://${HOST}:${PORT}"
  exit 0
fi

ensure_venv
mkdir -p "${ROOT_DIR}/data"

cd "${ROOT_DIR}"
nohup "${PYTHON}" app.py --host "${HOST}" --port "${PORT}" >>"${LOG_FILE}" 2>&1 &
echo $! >"${PID_FILE}"

sleep 1

if is_running; then
  echo "chat-viewer 已启动 (PID $(cat "${PID_FILE}"))"
  echo "访问: http://${HOST}:${PORT}"
  echo "日志: ${LOG_FILE}"
else
  echo "启动失败，请查看日志: ${LOG_FILE}"
  tail -n 30 "${LOG_FILE}" 2>/dev/null || true
  rm -f "${PID_FILE}"
  exit 1
fi
