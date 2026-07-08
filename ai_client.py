"""OpenAI 兼容 API 客户端（火山方舟等）。"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from config import (
    OPENAI_API_KEY,
    OPENAI_BASE_URL,
    OPENAI_DISABLE_THINKING,
    OPENAI_MAX_TOKENS,
    OPENAI_MODEL,
    OPENAI_RETRY_MAX_TOKENS,
    OPENAI_THINKING_FALLBACK,
)

logger = logging.getLogger(__name__)

_client = None


def ai_available() -> bool:
    return bool(OPENAI_API_KEY and OPENAI_BASE_URL)


def _get_client():
    global _client
    if _client is not None:
        return _client
    if not ai_available():
        return None
    from openai import OpenAI

    _client = OpenAI(api_key=OPENAI_API_KEY, base_url=OPENAI_BASE_URL)
    return _client


@dataclass
class _ParsedMessage:
    content: str
    reasoning: str
    finish_reason: str
    model: str
    usage: Any
    reasoning_tokens: int = 0
    truncated: bool = False


def _usage_reasoning_tokens(usage: Any) -> int:
    if usage is None:
        return 0
    details = getattr(usage, "completion_tokens_details", None)
    if details is None and isinstance(usage, dict):
        details = usage.get("completion_tokens_details")
    if details is None:
        return 0
    tokens = getattr(details, "reasoning_tokens", None)
    if tokens is None and isinstance(details, dict):
        tokens = details.get("reasoning_tokens")
    try:
        return int(tokens or 0)
    except (TypeError, ValueError):
        return 0


def _normalize_text_content(raw: Any) -> str:
    if raw is None:
        return ""
    if isinstance(raw, str):
        return raw.strip()
    if isinstance(raw, list):
        parts: List[str] = []
        for item in raw:
            if isinstance(item, dict):
                if item.get("type") in ("text", "output_text", "message"):
                    parts.append(str(item.get("text") or item.get("content") or ""))
            else:
                text = getattr(item, "text", None) or getattr(item, "content", None)
                if text:
                    parts.append(str(text))
        return "\n".join(p for p in parts if p).strip()
    return str(raw).strip()


def _extract_json_text(text: str) -> str:
    text = (text or "").strip()
    if not text:
        return ""
    fence = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", text)
    if fence:
        return fence.group(1).strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        return text.strip()
    if text.startswith("{"):
        return text
    return _find_balanced_json(text) or ""


def _find_balanced_json(text: str) -> Optional[str]:
    start = text.find("{")
    while start != -1:
        depth = 0
        in_string = False
        escape = False
        for i in range(start, len(text)):
            ch = text[i]
            if in_string:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == '"':
                    in_string = False
                continue
            if ch == '"':
                in_string = True
            elif ch == "{":
                depth += 1
            elif ch == "}":
                depth -= 1
                if depth == 0:
                    candidate = text[start : i + 1]
                    try:
                        json.loads(candidate)
                        return candidate
                    except json.JSONDecodeError:
                        break
        start = text.find("{", start + 1)
    return None


def _is_truncated(finish_reason: Optional[str]) -> bool:
    return (finish_reason or "") in ("length", "max_tokens")


def _parse_completion(choice: Any, response: Any) -> _ParsedMessage:
    message = choice.message
    content = _normalize_text_content(getattr(message, "content", None))
    reasoning = _normalize_text_content(getattr(message, "reasoning_content", None))
    finish_reason = choice.finish_reason or ""
    usage = response.usage
    reasoning_tokens = _usage_reasoning_tokens(usage)
    return _ParsedMessage(
        content=content,
        reasoning=reasoning,
        finish_reason=finish_reason,
        model=response.model,
        usage=usage,
        reasoning_tokens=reasoning_tokens,
        truncated=_is_truncated(finish_reason),
    )


def _effective_max_tokens(base: Optional[int], bump_level: int) -> Optional[int]:
    if bump_level <= 0:
        return base
    if base is None:
        return OPENAI_RETRY_MAX_TOKENS
    return min(base * (2**bump_level), OPENAI_RETRY_MAX_TOKENS)


def _build_attempt_specs(want_json_mode: bool) -> List[Dict[str, Any]]:
    if OPENAI_DISABLE_THINKING:
        specs: List[Dict[str, Any]] = [
            {"json": want_json_mode, "thinking_off": True, "token_bump": 0},
            {"json": want_json_mode, "thinking_off": True, "token_bump": 1},
            {"json": False, "thinking_off": True, "token_bump": 0},
        ]
        return specs

    specs = [
        {"json": want_json_mode, "thinking_off": False, "token_bump": 0},
        {"json": want_json_mode, "thinking_off": False, "token_bump": 1},
        {"json": want_json_mode, "thinking_off": False, "token_bump": 2},
    ]
    if OPENAI_THINKING_FALLBACK:
        specs.extend(
            [
                {"json": want_json_mode, "thinking_off": True, "token_bump": 0},
                {"json": want_json_mode, "thinking_off": True, "token_bump": 1},
            ]
        )
    specs.extend(
        [
            {"json": False, "thinking_off": False, "token_bump": 0},
        ]
    )
    if OPENAI_THINKING_FALLBACK:
        specs.append({"json": False, "thinking_off": True, "token_bump": 0})
    return specs


def _should_retry_empty(parsed: _ParsedMessage) -> bool:
    if parsed.content:
        return parsed.truncated
    if parsed.truncated:
        return True
    if parsed.reasoning and not parsed.content:
        return True
    return False


def _create_completion(
    client: Any,
    *,
    messages: List[Dict[str, str]],
    temperature: float,
    max_tokens: Optional[int],
    use_json_mode: bool,
    thinking_off: bool,
) -> Tuple[_ParsedMessage, Any]:
    kwargs: Dict[str, Any] = {
        "model": OPENAI_MODEL,
        "messages": messages,
        "temperature": temperature,
    }
    if max_tokens is not None:
        kwargs["max_tokens"] = max_tokens
    if thinking_off:
        kwargs["extra_body"] = {"enable_thinking": False}
    if use_json_mode:
        kwargs["response_format"] = {"type": "json_object"}

    response = client.chat.completions.create(**kwargs)
    choice = response.choices[0]
    parsed = _parse_completion(choice, response)
    return parsed, response


def chat_text(
    messages: List[Dict[str, str]],
    *,
    temperature: float = 0.4,
    max_tokens: Optional[int] = None,
    label: str = "chat",
) -> str:
    client = _get_client()
    if client is None:
        raise RuntimeError("未配置 OPENAI_API_KEY，请在 .env 中设置")

    if max_tokens is None:
        max_tokens = OPENAI_MAX_TOKENS

    want_json_mode = "blocks" in label or label.endswith("json")
    specs = _build_attempt_specs(want_json_mode)

    last_parsed: Optional[_ParsedMessage] = None
    last_exc: Optional[Exception] = None

    for idx, spec in enumerate(specs):
        attempt_tokens = _effective_max_tokens(max_tokens, int(spec["token_bump"]))
        thinking_off = bool(spec["thinking_off"])
        use_json = bool(spec["json"])
        attempt_label = label if idx == 0 else f"{label}-a{idx + 1}"

        logger.info(
            "[%s] LLM 请求 model=%s messages=%d max_tokens=%s json=%s thinking=%s bump=%s",
            attempt_label,
            OPENAI_MODEL,
            len(messages),
            attempt_tokens if attempt_tokens is not None else "unlimited",
            use_json,
            "off" if thinking_off else "on",
            spec["token_bump"],
        )

        try:
            parsed, response = _create_completion(
                client,
                messages=messages,
                temperature=temperature,
                max_tokens=attempt_tokens,
                use_json_mode=use_json,
                thinking_off=thinking_off,
            )
        except Exception as exc:
            last_exc = exc
            logger.warning("[%s] 请求失败: %s", attempt_label, exc)
            continue

        last_parsed = parsed
        if parsed.reasoning:
            logger.info(
                "[%s] thinking 内容已分离 reasoning_chars=%d reasoning_tokens=%s",
                attempt_label,
                len(parsed.reasoning),
                parsed.reasoning_tokens or "—",
            )

        if parsed.content:
            if parsed.truncated:
                logger.warning(
                    "[%s] 输出可能被截断 finish=%s chars=%d，尝试提高预算重试",
                    attempt_label,
                    parsed.finish_reason,
                    len(parsed.content),
                )
                if idx + 1 < len(specs):
                    continue
            logger.info(
                "[%s] LLM 响应 model=%s chars=%d finish=%s usage=%s",
                attempt_label,
                parsed.model,
                len(parsed.content),
                parsed.finish_reason,
                parsed.usage,
            )
            return parsed.content

        logger.warning(
            "[%s] content 为空 finish=%s reasoning_chars=%d usage=%s",
            attempt_label,
            parsed.finish_reason,
            len(parsed.reasoning),
            parsed.usage,
        )
        if _should_retry_empty(parsed) and idx + 1 < len(specs):
            continue

    if last_parsed and last_parsed.content:
        return last_parsed.content

    detail = ""
    if last_parsed:
        detail = (
            f"finish_reason={last_parsed.finish_reason}, "
            f"reasoning_chars={len(last_parsed.reasoning)}, "
            f"reasoning_tokens={last_parsed.reasoning_tokens}, "
            f"usage={last_parsed.usage}"
        )
    if last_exc and not detail:
        raise last_exc
    raise RuntimeError(
        "LLM 未返回可用的最终内容（content 为空）。"
        f" {detail}"
        " 可尝试增大 OPENAI_MAX_TOKENS / OPENAI_RETRY_MAX_TOKENS，"
        "或设置 OPENAI_DISABLE_THINKING=true 强制关闭思考。"
    )


def chat_json(
    messages: List[Dict[str, str]],
    *,
    temperature: float = 0.3,
    max_tokens: Optional[int] = None,
    label: str = "chat_json",
    retries: int = 2,
) -> Dict[str, Any]:
    msgs = list(messages)
    last_error: Optional[Exception] = None
    for attempt in range(retries):
        attempt_label = label if attempt == 0 else f"{label}-retry{attempt}"
        try:
            text = chat_text(
                msgs,
                temperature=temperature,
                max_tokens=max_tokens,
                label=attempt_label,
            )
            raw = _extract_json_text(text)
            if not raw:
                raise ValueError("LLM 返回空 JSON 文本")
            parsed = json.loads(raw)
            if not isinstance(parsed, dict):
                raise ValueError("LLM 未返回 JSON 对象")
            return parsed
        except (ValueError, json.JSONDecodeError, RuntimeError) as exc:
            last_error = exc
            logger.error(
                "[%s] JSON 解析失败 attempt=%d/%d: %s",
                attempt_label,
                attempt + 1,
                retries,
                exc,
            )
            if attempt + 1 < retries:
                msgs = msgs + [
                    {
                        "role": "user",
                        "content": (
                            "上次输出不是合法 JSON。请仅输出一个 JSON 对象（含 blocks 数组），"
                            "思考过程不要写入最终答案。"
                            "不要 markdown 列表、不要代码块标记外的任何文字。"
                            "每个 block 的 summary 控制在 80 字以内。"
                        ),
                    }
                ]
    if last_error:
        raise ValueError(str(last_error)) from last_error
    raise ValueError("LLM 未返回有效 JSON")
