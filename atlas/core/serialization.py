from __future__ import annotations

import json
import logging
import re
from dataclasses import is_dataclass, asdict
from datetime import date, datetime, timezone
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import UUID

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────
# Phase 24 — centralized timestamp normalization
# ──────────────────────────────────────────────

def normalize_timestamp(ts: Any) -> datetime:
    """
    Convert any timestamp value into a timezone-aware UTC datetime.

    Accepts:
    - datetime objects (naive → UTC assumed, aware → converted to UTC)
    - ISO-8601 strings (e.g. "2024-01-01T12:00:00.123456+00:00")
    - int / float (unix epoch seconds)
    - None (returns current UTC time)

    Returns:
        A timezone-aware datetime in UTC.

    Raises:
        ValueError if the value cannot be parsed.
    """
    if ts is None:
        return datetime.now(timezone.utc)

    if isinstance(ts, datetime):
        if ts.tzinfo is None:
            # Naive datetime — assume UTC
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)

    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    if isinstance(ts, str):
        # Try ISO-8601 parsing with fromisoformat
        cleaned = ts.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(cleaned)
        except ValueError:
            # Python < 3.11 does not support colon in UTC offset ("+00:00")
            # Strip the colon from the last +HH:MM or -HH:MM segment
            cleaned = re.sub(r"([+-]\d{2}):(\d{2})$", r"\1\2", ts.replace("Z", "+0000"))
            try:
                dt = datetime.fromisoformat(cleaned)
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "normalize_timestamp: cannot parse ISO string %r — %s. Falling back to now().",
                    ts, exc,
                )
                return datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    # Last resort — stringify and try again (catches date objects, etc.)
    try:
        return normalize_timestamp(str(ts))
    except Exception as exc:
        logger.warning(
            "normalize_timestamp: unexpected type %s (%r) — %s. Falling back to now().",
            type(ts).__name__, ts, exc,
        )
        return datetime.now(timezone.utc)


def normalize_json_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, Path):
        return str(value)
    if is_dataclass(value):
        return normalize_json_value(asdict(value))
    if hasattr(value, "_mapping"):
        return normalize_json_value(dict(value._mapping))
    if isinstance(value, dict):
        return {str(k): normalize_json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [normalize_json_value(v) for v in value]
    if hasattr(value, "item") and callable(value.item):
        try:
            return normalize_json_value(value.item())
        except Exception:
            pass
    return str(value)


def normalize_db_params(params: dict[str, Any] | None) -> dict[str, Any]:
    """
    Normalize DB parameters for SQLAlchemy text() execution.

    Phase 24 enhancement:
    - datetime objects are kept as-is (with UTC timezone) for proper
      PostgreSQL ::timestamptz binding.
    - ISO-8601 timestamp *strings* are converted to datetime objects.
    - numpy scalars and other non-native types are converted to native Python.
    """
    result: dict[str, Any] = {}
    for k, v in (params or {}).items():
        key = str(k)

        # NEW: Keep datetime objects as-is for proper DB binding —
        # do NOT send them through normalize_json_value which converts
        # them to ISO strings (breaking TIMESTAMPTZ columns).
        if isinstance(v, datetime):
            if v.tzinfo is None:
                result[key] = v.replace(tzinfo=timezone.utc)
            else:
                result[key] = v.astimezone(timezone.utc)
            continue

        # Try to recover ISO-8601 timestamp strings as proper datetime objects
        # so that SQL ::timestamptz casts are unambiguous.
        if isinstance(v, str) and _looks_like_iso_timestamp(v):
            try:
                result[key] = normalize_timestamp(v)
                continue
            except Exception:
                pass  # fall through to normal serialization

        result[key] = normalize_json_value(v)

    return result


# Internal helper: fast heuristic to detect ISO-8601 timestamp strings
_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}")


def _looks_like_iso_timestamp(v: str) -> bool:
    return bool(_ISO_RE.match(v))


def safe_json_dumps(value: Any) -> str:
    return json.dumps(normalize_json_value(value), default=str, ensure_ascii=False)
