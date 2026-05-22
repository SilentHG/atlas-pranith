from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from atlas.core.serialization import normalize_json_value, normalize_timestamp


@dataclass
class ScoutValidationResult:
    valid: bool
    reasons: list[str]
    normalized_payload: dict[str, Any]


def _payload_size(payload: dict[str, Any]) -> int:
    return len(json.dumps(payload, default=str, ensure_ascii=False))


def validate_scout_payload(payload: dict[str, Any]) -> ScoutValidationResult:
    normalized = normalize_json_value(payload)
    reasons: list[str] = []

    if not isinstance(normalized, dict):
        return ScoutValidationResult(False, ["payload_not_object"], {"payload": normalized})

    source = str(normalized.get("source", "")).strip()
    if not source:
        reasons.append("missing_source")

    # Phase 24: Use centralized normalize_timestamp() for timestamp handling
    raw_ts = payload.get("timestamp")  # raw original (pre-normalize_json_value)
    try:
        # Keep timestamp as a timezone-aware datetime object for deterministic DB binding
        normalized["timestamp"] = normalize_timestamp(raw_ts)
    except Exception as e:
        reasons.append(f"invalid_timestamp: {e}")
        normalized["timestamp"] = datetime.now(timezone.utc)

    confidence = normalized.get("confidence")
    if confidence is None:
        confidence = normalized.get("confidence_score", normalized.get("hypothesis_score"))
        if confidence is not None:
            normalized["confidence"] = confidence

    try:
        confidence_value = float(confidence) if confidence is not None else None
    except Exception:
        confidence_value = None
        reasons.append("invalid_confidence")

    if confidence_value is not None and not (0.0 <= confidence_value <= 1.0):
        reasons.append("confidence_out_of_range")

    if _payload_size(normalized) > 16_384:
        reasons.append("payload_too_large")

    if normalized.get("details") is None and normalized.get("metadata") is None:
        reasons.append("missing_details")

    return ScoutValidationResult(valid=not reasons, reasons=reasons, normalized_payload=normalized)
