"""通用 Protobuf wire format 工具（无 .proto schema）。"""

from __future__ import annotations

import struct
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple


def decode_varint(data: bytes, pos: int) -> Tuple[int, int]:
    result = 0
    shift = 0
    while pos < len(data):
        byte = data[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            return result, pos
        shift += 7
        if shift >= 64:
            raise ValueError("varint too long")
    raise ValueError("unexpected end of data")


def read_field(data: bytes, pos: int, wire_type: int) -> Tuple[Any, int]:
    if wire_type == 0:
        return decode_varint(data, pos)
    if wire_type == 1:
        if pos + 8 > len(data):
            raise ValueError("truncated fixed64")
        return struct.unpack("<Q", data[pos : pos + 8])[0], pos + 8
    if wire_type == 2:
        length, pos = decode_varint(data, pos)
        if pos + length > len(data):
            raise ValueError("truncated length-delimited field")
        return data[pos : pos + length], pos + length
    if wire_type == 5:
        if pos + 4 > len(data):
            raise ValueError("truncated fixed32")
        return struct.unpack("<I", data[pos : pos + 4])[0], pos + 4
    raise ValueError(f"unsupported wire type: {wire_type}")


def try_decode_text(raw: bytes) -> Optional[str]:
    if not raw:
        return None
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None
    if not text:
        return None
    printable = sum(1 for c in text if c.isprintable() or c in "\n\r\t")
    if printable / len(text) < 0.85:
        return None
    return text


def parse_protobuf(data: bytes, max_depth: int = 12) -> List[Dict[str, Any]]:
    fields: List[Dict[str, Any]] = []

    def walk(blob: bytes, depth: int) -> None:
        if depth > max_depth:
            return
        pos = 0
        while pos < len(blob):
            try:
                tag, pos = decode_varint(blob, pos)
                field_number = tag >> 3
                wire_type = tag & 0x7
                value, pos = read_field(blob, pos, wire_type)
                entry: Dict[str, Any] = {"field": field_number, "wire_type": wire_type}
                if wire_type == 0:
                    entry["varint"] = value
                elif wire_type == 2:
                    raw: bytes = value
                    text = try_decode_text(raw)
                    if text is not None:
                        entry["string"] = text
                    else:
                        nested = parse_protobuf(raw, max_depth)
                        if nested:
                            entry["message"] = nested
                        else:
                            entry["bytes_len"] = len(raw)
                else:
                    entry["value"] = value
                fields.append(entry)
            except Exception:
                break

    walk(data, 0)
    return fields


def extract_strings(fields: List[Dict[str, Any]], min_len: int = 8) -> List[str]:
    out: List[str] = []

    def visit(items: List[Dict[str, Any]]) -> None:
        for item in items:
            if "string" in item and len(item["string"]) >= min_len:
                out.append(item["string"])
            if "message" in item:
                visit(item["message"])

    visit(fields)
    return out


def protobuf_timestamp_to_iso(fields: List[Dict[str, Any]]) -> Optional[str]:
    seconds = None
    nanos = None
    for item in fields:
        if item.get("field") == 1 and "varint" in item:
            seconds = item["varint"]
        if item.get("field") == 2 and "varint" in item:
            nanos = item["varint"]
    if seconds is None:
        return None
    dt = datetime.fromtimestamp(seconds + (nanos or 0) / 1_000_000_000, tz=timezone.utc)
    return dt.isoformat()
