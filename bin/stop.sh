#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=lib.sh
source "${SCRIPT_DIR}/lib.sh"

stopped=0

if [[ -f "${PID_FILE}" ]]; then
  pid="$(cat "${PID_FILE}" 2>/dev/null || true)"
  if pid_alive "$pid"; then
    kill "${pid}" 2>/dev/null || true
    for _ in $(seq 1 10); do
      if ! pid_alive "$pid"; then
        stopped=1
        break
      fi
      sleep 0.5
    done
    if [[ "${stopped}" -eq 0 ]]; then
      kill -9 "${pid}" 2>/dev/null || true
      stopped=1
    fi
    echo "已停止 chat-viewer (PID ${pid})"
  fi
  rm -f "${PID_FILE}"
fi

remaining="$(port_pids)"
if [[ -n "${remaining}" ]]; then
  # shellcheck disable=SC2086
  kill ${remaining} 2>/dev/null || true
  sleep 1
  remaining="$(port_pids)"
  if [[ -n "${remaining}" ]]; then
    # shellcheck disable=SC2086
    kill -9 ${remaining} 2>/dev/null || true
  fi
  echo "已释放端口 ${PORT}"
  stopped=1
fi

if [[ "${stopped}" -eq 0 ]]; then
  echo "chat-viewer 未在运行"
fi
