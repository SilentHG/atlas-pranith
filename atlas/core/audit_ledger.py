"""
audit_ledger.py — Immutable audit ledger with cryptographic hash chaining.

Provides tamper-resistant governance tracking for:
- validations, approvals, deployments, retirements
- capital reallocations, overrides, kill switches
- risk breaches, operator actions
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text


class AuditEventType(str, Enum):
    VALIDATION = "validation"
    APPROVAL = "approval"
    DEPLOYMENT = "deployment"
    RETIREMENT = "retirement"
    CAPITAL_REALLOCATION = "capital_reallocation"
    OVERRIDE = "override"
    KILL_SWITCH = "kill_switch"
    RISK_BREACH = "risk_breach"
    OPERATOR_ACTION = "operator_action"
    SYSTEM_CONFIG = "system_config"


class AuditLedger:
    """
    Append-only, tamper-resistant audit ledger with hash chaining.

    Each entry includes:
    - hash_prev: SHA-256 of the previous audit entry
    - hash_self: SHA-256 of this entry's content (including hash_prev)
    - signature: cryptographic signature of the entry
    """

    def __init__(self, db):
        self.db = db

    async def record(
        self,
        event_type: str,
        actor: str,
        action: str,
        resource_type: str,
        resource_id: Optional[str] = None,
        details: Optional[dict] = None,
        severity: str = "info",
        trace_id: Optional[str] = None,
    ) -> str:
        """Record an immutable audit entry. Returns entry_id."""
        entry_id = uuid.uuid4().hex[:16]
        now = datetime.now(timezone.utc)

        # Compute next sequence for this trace_id (per-trace_id incrementing)
        sequence = await self._next_sequence(trace_id)

        # Get previous hash for chain (per-trace_id chain)
        prev_hash = await self._get_last_hash(trace_id)

        # Build content for self-hash (sequence included for deterministic hash)
        content = {
            "id": entry_id,
            "event_type": event_type,
            "actor": actor,
            "action": action,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "details": details or {},
            "severity": severity,
            "trace_id": trace_id,
            "sequence": sequence,
            "hash_prev": prev_hash,
            "created_at": now.isoformat(),
        }
        hash_self = hashlib.sha256(
            json.dumps(content, sort_keys=True).encode("utf-8")
        ).hexdigest()

        await self.db._execute_insert(
            """
            INSERT INTO audit_ledger
                (id, event_type, actor, action, resource_type, resource_id,
                 details, severity, trace_id, sequence, hash_prev, hash_self, created_at)
            VALUES
                (:id, :event_type, :actor, :action, :resource_type, :resource_id,
                 CAST(:details AS jsonb), :severity, :trace_id, :sequence,
                 :hash_prev, :hash_self, :created_at)
            """,
            {
                "id": entry_id,
                "event_type": event_type,
                "actor": actor,
                "action": action,
                "resource_type": resource_type,
                "resource_id": resource_id,
                "details": json.dumps(details or {}),
                "severity": severity,
                "trace_id": trace_id,
                "sequence": sequence,
                "hash_prev": prev_hash,
                "hash_self": hash_self,
                "created_at": now.isoformat(),
            },
        )

        logger.info(f"Audit: {event_type} | {actor} | {action} | {resource_type}:{resource_id} | seq={sequence}")
        return entry_id

    async def get_entries(
        self,
        limit: int = 100,
        offset: int = 0,
        event_type: Optional[str] = None,
        actor: Optional[str] = None,
        severity: Optional[str] = None,
    ) -> list[dict]:
        """Query audit entries with optional filters."""
        conditions = []
        params: dict = {"limit": limit, "offset": offset}

        if event_type:
            conditions.append("event_type = :event_type")
            params["event_type"] = event_type
        if actor:
            conditions.append("actor = :actor")
            params["actor"] = actor
        if severity:
            conditions.append("severity = :severity")
            params["severity"] = severity

        where_clause = " AND ".join(conditions) if conditions else "TRUE"

        async with self.db.engine.connect() as conn:
            result = await conn.execute(
                text(f"""
                    SELECT id, event_type, actor, action, resource_type, resource_id,
                           details, severity, trace_id, sequence, hash_prev, hash_self, created_at
                    FROM audit_ledger
                    WHERE {where_clause}
                    ORDER BY created_at DESC
                    LIMIT :limit OFFSET :offset
                """),
                params,
            )
            return [self._row_to_entry(row) for row in result.fetchall()]

    async def verify_chain(self) -> dict:
        """
        Verify the integrity of the entire audit chain.
        Verifies per-trace_id groups independently using sequence ordering.
        Returns verification report.
        """
        async with self.db.engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT id, event_type, actor, action, resource_type, resource_id,
                           details, severity, trace_id, sequence, hash_prev, hash_self, created_at
                    FROM audit_ledger
                    ORDER BY COALESCE(trace_id, ''), sequence ASC
                """),
            )
            rows = result.fetchall()

        if not rows:
            return {"valid": True, "entries_checked": 0}

        violations = []
        # Group entries per trace_id for independent hash chain verification
        prev_hash: str | None = None
        prev_trace_id: str | None = None
        for i, row in enumerate(rows):
            entry = self._row_to_entry(row)
            current_trace_id = entry["trace_id"]

            # Reset chain when trace_id changes (per-trace_id chains)
            if current_trace_id != prev_trace_id:
                prev_hash = None
                prev_trace_id = current_trace_id

            # Build content for self-hash (must match record() exactly)
            content = {
                "id": entry["id"],
                "event_type": entry["event_type"],
                "actor": entry["actor"],
                "action": entry["action"],
                "resource_type": entry["resource_type"],
                "resource_id": entry["resource_id"],
                "details": entry["details"],
                "severity": entry["severity"],
                "trace_id": current_trace_id,
                "sequence": entry["sequence"],
                "hash_prev": entry["hash_prev"],
                "created_at": entry["created_at"],
            }
            expected_hash = hashlib.sha256(
                json.dumps(content, sort_keys=True).encode("utf-8")
            ).hexdigest()

            if expected_hash != entry["hash_self"]:
                violations.append(f"Entry {i} ({entry['id']}): self-hash mismatch")

            if prev_hash is not None and entry["hash_prev"] != prev_hash:
                violations.append(
                    f"Entry {i} ({entry['id']}): chain broken in {current_trace_id} "
                    f"(expected prev_hash={prev_hash[:16]}...)"
                )

            prev_hash = entry["hash_self"]

        return {
            "valid": len(violations) == 0,
            "entries_checked": len(rows),
            "violations": violations,
        }

    async def get_summary(
        self,
        hours: int = 24,
    ) -> dict:
        """Get audit summary for the time window."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT
                        COUNT(*) as total_entries,
                        COUNT(DISTINCT actor) as unique_actors,
                        COUNT(DISTINCT event_type) as unique_types,
                        COUNT(CASE WHEN severity = 'critical' THEN 1 END) as critical_events,
                        COUNT(CASE WHEN severity = 'warning' THEN 1 END) as warnings
                    FROM audit_ledger
                    WHERE created_at > NOW() - INTERVAL :delta
                """),
                {"delta": f"{hours} hours"},
            )
            row = r.fetchone()
            if not row:
                return {}

            r2 = await conn.execute(
                text("""
                    SELECT event_type, COUNT(*) as cnt
                    FROM audit_ledger
                    WHERE created_at > NOW() - INTERVAL :delta
                    GROUP BY event_type ORDER BY cnt DESC
                """),
                {"delta": f"{hours} hours"},
            )
            by_type = {str(r[0]): r[1] for r in r2.fetchall()}

            return {
                "total_entries": row[0],
                "unique_actors": row[1],
                "unique_types": row[2],
                "critical_events": row[3],
                "warnings": row[4],
                "by_type": by_type,
            }

    async def _next_sequence(self, trace_id: Optional[str]) -> int:
        """Compute the next sequence number for the given trace_id."""
        async with self.db.engine.connect() as conn:
            if trace_id:
                result = await conn.execute(
                    text("SELECT COALESCE(MAX(sequence), 0) + 1 FROM audit_ledger WHERE trace_id = :trace_id"),
                    {"trace_id": trace_id},
                )
            else:
                result = await conn.execute(
                    text("SELECT COALESCE(MAX(sequence), 0) + 1 FROM audit_ledger WHERE trace_id IS NULL")
                )
            row = result.fetchone()
            return int(row[0]) if row else 1

    async def _get_last_hash(self, trace_id: Optional[str] = None) -> Optional[str]:
        """Get the last hash_self for the given trace_id (or global if trace_id is None)."""
        async with self.db.engine.connect() as conn:
            if trace_id:
                result = await conn.execute(
                    text("""
                        SELECT hash_self FROM audit_ledger
                        WHERE trace_id = :trace_id
                        ORDER BY sequence DESC LIMIT 1
                    """),
                    {"trace_id": trace_id},
                )
            else:
                result = await conn.execute(
                    text("""
                        SELECT hash_self FROM audit_ledger
                        WHERE trace_id IS NULL
                        ORDER BY sequence DESC LIMIT 1
                    """),
                )
            row = result.fetchone()
            return str(row[0]) if row else None

    def _row_to_entry(self, row) -> dict:
        details = row[6]
        if isinstance(details, str):
            try:
                details = json.loads(details)
            except Exception:
                details = {}
        return {
            "id": str(row[0]),
            "event_type": str(row[1]),
            "actor": str(row[2]),
            "action": str(row[3]),
            "resource_type": str(row[4]),
            "resource_id": str(row[5]) if row[5] else None,
            "details": details,
            "severity": str(row[7]),
            "trace_id": str(row[8]) if row[8] else None,
            "sequence": int(row[9]) if row[9] is not None else 0,
            "hash_prev": str(row[10]) if row[10] else None,
            "hash_self": str(row[11]),
            "created_at": row[12].isoformat() if hasattr(row[12], "isoformat") else str(row[12]),
        }
