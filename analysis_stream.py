"""SSE 事件格式化与流式响应辅助。"""

from __future__ import annotations

import json
from typing import Any, Dict, Iterator


def format_sse(event: str, data: Dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


def sse_response(event_iter: Iterator[str]):
    from flask import Response

    return Response(
        event_iter,
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
