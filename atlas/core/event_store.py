"""
event_store.py — Append-only immutable event log for deterministic reconstruction.

All ATLAS actions emit events:
- strategy generation, mutation, validation
- allocation, execution, fill
- drift, retirement, scout findings
- portfolio changes, deployment changes

Supports:
- Event versioning
- Deterministic replay
- Event snapshots
- Causal lineage reconstruction
- Parent-child event graphs
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from collections import OrderedDict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text


class EventVersion(str, Enum):
    V1 = "1.0"
    V2 = "2.0"


@dataclass
class StoredEvent:
    id: str
    event_type: str
    version: str
    trace_id: str
    parent_event_id: Optional[str]
    aggregate_id: Optional[str]
    aggregate_type: Optional[str]
    data: dict
    metadata: dict
    hash_prev: Optional[str]
    hash_self: str
    created_at: str
    sequence: int

    def to_dict(self) -> dict:
        return asdict(self)


class EventStore:
    """
    Append-only immutable event store.

    Each event carries:
    - hash_prev: SHA-256 of the previous event in the aggregate stream
    - hash_self: SHA-256 of this event's content (including hash_prev)
    - sequence: monotonically increasing per aggregate
    """

    def __init__(self, db):
        self.db = db
        self._cache: OrderedDict[str, list[StoredEvent]] = OrderedDict()
        self._snapshot_cache: OrderedDict[str, dict] = OrderedDict()
        self._max_cache_entries = 1024
        self._max_snapshot_entries = 256
        self._lock = asyncio.Lock()

    # ───────────────────────────────────────────────
    # Write path
    # ───────────────────────────────────────────────

    async def append_event(
        self,
        event_type: str,
        trace_id: str,
        data: dict,
        aggregate_id: Optional[str] = None,
        aggregate_type: Optional[str] = None,
        parent_event_id: Optional[str] = None,
        metadata: Optional[dict] = None,
        version: str = EventVersion.V1.value,
    ) -> str:
        """Append an immutable event. Returns event_id."""
        async with self._lock:
            event_id = uuid.uuid4().hex[:16]
            now = datetime.now(timezone.utc)

            # Get previous hash for aggregate stream
            prev_hash = None
            next_sequence = 1
            if aggregate_id:
                last_event = await self._get_last_event(aggregate_id)
                if last_event:
                    prev_hash = last_event.hash_self
                    next_sequence = last_event.sequence + 1

            # Build content for self-hash
            # IMPORTANT: Use isoformat() for JSON serialization — datetime objects
            # are NOT JSON-serializable by default.
            now_iso = now.isoformat()
            content = {
                "id": event_id,
                "event_type": event_type,
                "version": version,
                "trace_id": trace_id,
                "parent_event_id": parent_event_id,
                "aggregate_id": aggregate_id,
                "aggregate_type": aggregate_type,
                "data": data,
                "metadata": metadata or {},
                "hash_prev": prev_hash,
                "sequence": next_sequence,
                "created_at": now_iso,
            }
            hash_self = hashlib.sha256(
                json.dumps(content, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()

            await self.db._execute_insert(
                """
                INSERT INTO event_store
                    (id, event_type, version, trace_id, parent_event_id,
                     aggregate_id, aggregate_type, data, metadata,
                     hash_prev, hash_self, created_at, sequence)
                VALUES
                    (:id, :event_type, :version, :trace_id, :parent_event_id,
                     :aggregate_id, :aggregate_type, CAST(:data AS jsonb), CAST(:metadata AS jsonb),
                     :hash_prev, :hash_self, :created_at, :sequence)
                """,
                {
                    "id": event_id,
                    "event_type": event_type,
                    "version": version,
                    "trace_id": trace_id,
                    "parent_event_id": parent_event_id,
                    "aggregate_id": aggregate_id,
                    "aggregate_type": aggregate_type,
                    "data": json.dumps(data),
                    "metadata": json.dumps(metadata or {}),
                    "hash_prev": prev_hash,
                    "hash_self": hash_self,
                    "created_at": now,
                    "sequence": next_sequence,
                },
            )

            # Invalidate caches
            if aggregate_id:
                self._cache.pop(aggregate_id, None)
                self._snapshot_cache.pop(aggregate_id, None)

            logger.debug(f"Event appended: {event_type} [{event_id}] seq={next_sequence}")
            return event_id

    # ───────────────────────────────────────────────
    # Read path
    # ───────────────────────────────────────────────

    async def get_events(
        self,
        aggregate_id: str,
        from_sequence: int = 0,
        limit: int = 1000,
    ) -> list[StoredEvent]:
        """Get all events for an aggregate, ordered by sequence."""
        if aggregate_id in self._cache:
            events = self._cache[aggregate_id]
            self._cache.move_to_end(aggregate_id)
            if from_sequence > 0:
                events = [e for e in events if e.sequence >= from_sequence]
            return events[:limit]

        async with self.db.engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT id, event_type, version, trace_id, parent_event_id,
                           aggregate_id, aggregate_type, data, metadata,
                           hash_prev, hash_self, created_at, sequence
                    FROM event_store
                    WHERE aggregate_id = :aggregate_id
                    ORDER BY sequence ASC
                    LIMIT :limit
                """),
                {"aggregate_id": aggregate_id, "limit": limit},
            )
            events = self._rows_to_events(result.fetchall())

        self._store_cached_events(aggregate_id, events)
        if from_sequence > 0:
            events = [e for e in events if e.sequence >= from_sequence]
        return events

    async def get_events_by_trace(self, trace_id: str) -> list[StoredEvent]:
        """Get all events for a trace (across all aggregates)."""
        async with self.db.engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT id, event_type, version, trace_id, parent_event_id,
                           aggregate_id, aggregate_type, data, metadata,
                           hash_prev, hash_self, created_at, sequence
                    FROM event_store
                    WHERE trace_id = :trace_id
                    ORDER BY created_at ASC
                """),
                {"trace_id": trace_id},
            )
            return self._rows_to_events(result.fetchall())

    async def get_events_by_type(
        self,
        event_type: str,
        limit: int = 100,
        offset: int = 0,
    ) -> list[StoredEvent]:
        """Get events by type, ordered by recency."""
        async with self.db.engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT id, event_type, version, trace_id, parent_event_id,
                           aggregate_id, aggregate_type, data, metadata,
                           hash_prev, hash_self, created_at, sequence
                    FROM event_store
                    WHERE event_type = :event_type
                    ORDER BY created_at DESC
                    LIMIT :limit OFFSET :offset
                """),
                {"event_type": event_type, "limit": limit, "offset": offset},
            )
            return self._rows_to_events(result.fetchall())

    async def get_all_aggregates(self, limit: int = 100) -> list[dict]:
        """Get distinct aggregates with summary info."""
        async with self.db.engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT
                        aggregate_id,
                        aggregate_type,
                        COUNT(*) as event_count,
                        MAX(sequence) as last_sequence,
                        MIN(created_at) as first_event,
                        MAX(created_at) as last_event
                    FROM event_store
                    WHERE aggregate_id IS NOT NULL
                    GROUP BY aggregate_id, aggregate_type
                    ORDER BY last_event DESC
                    LIMIT :limit
                """),
                {"limit": limit},
            )
            return [
                {
                    "aggregate_id": str(row[0]),
                    "aggregate_type": str(row[1]) if row[1] else "",
                    "event_count": row[2],
                    "last_sequence": row[3],
                    "first_event": row[4].isoformat() if hasattr(row[4], "isoformat") else str(row[4]),
                    "last_event": row[5].isoformat() if hasattr(row[5], "isoformat") else str(row[5]),
                }
                for row in result.fetchall()
            ]

    # ───────────────────────────────────────────────
    # Snapshots
    # ───────────────────────────────────────────────

    async def create_snapshot(
        self,
        aggregate_id: str,
        state: dict,
        version: int,
    ) -> str:
        """Create a point-in-time snapshot for fast replay."""
        snapshot_id = uuid.uuid4().hex[:16]
        await self.db._execute_insert(
            """
            INSERT INTO event_snapshots
                (id, aggregate_id, state, version, created_at)
            VALUES
                (:id, :aggregate_id, CAST(:state AS jsonb), :version, NOW())
            """,
            {
                "id": snapshot_id,
                "aggregate_id": aggregate_id,
                "state": json.dumps(state),
                "version": version,
            },
        )
        self._store_cached_snapshot(aggregate_id, state)
        return snapshot_id

    async def get_snapshot(self, aggregate_id: str) -> Optional[dict]:
        """Get the latest snapshot for an aggregate."""
        if aggregate_id in self._snapshot_cache:
            self._snapshot_cache.move_to_end(aggregate_id)
            return self._snapshot_cache[aggregate_id]

        async with self.db.engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT state, version
                    FROM event_snapshots
                    WHERE aggregate_id = :aggregate_id
                    ORDER BY version DESC
                    LIMIT 1
                """),
                {"aggregate_id": aggregate_id},
            )
            row = result.fetchone()
            if row:
                state = row[0]
                if isinstance(state, str):
                    state = json.loads(state)
                self._store_cached_snapshot(aggregate_id, state)
                return state
        return None

    # ───────────────────────────────────────────────
    # Replay
    # ───────────────────────────────────────────────

    async def replay_aggregate(
        self,
        aggregate_id: str,
        apply_fn,
        use_snapshot: bool = True,
    ) -> Any:
        """
        Deterministic replay: load snapshot + apply events since snapshot.

        apply_fn(state, event) -> new_state
        """
        state = None
        start_sequence = 0

        if use_snapshot:
            snapshot = await self.get_snapshot(aggregate_id)
            if snapshot:
                state = snapshot.get("state")
                start_sequence = snapshot.get("version", 0)

        events = await self.get_events(aggregate_id, from_sequence=start_sequence + 1)
        for event in events:
            state = apply_fn(state, event.data)

        return state

    async def replay_trace(self, trace_id: str, apply_fn) -> list[Any]:
        """Replay all events in a trace through an apply function."""
        events = await self.get_events_by_trace(trace_id)
        results = []
        for event in events:
            result = apply_fn(event)
            results.append(result)
        return results

    async def verify_integrity(self, aggregate_id: str) -> dict:
        """
        Verify the hash chain for an aggregate.
        Returns integrity report.
        """
        events = await self.get_events(aggregate_id)
        if not events:
            return {"aggregate_id": aggregate_id, "valid": True, "events_checked": 0}

        violations = []
        prev_hash = None
        for i, event in enumerate(events):
            # Verify self-hash
            content = {
                "id": event.id,
                "event_type": event.event_type,
                "version": event.version,
                "trace_id": event.trace_id,
                "parent_event_id": event.parent_event_id,
                "aggregate_id": event.aggregate_id,
                "aggregate_type": event.aggregate_type,
                "data": event.data,
                "metadata": event.metadata,
                "hash_prev": event.hash_prev,
                "sequence": event.sequence,
                "created_at": event.created_at,
            }
            expected_hash = hashlib.sha256(
                json.dumps(content, sort_keys=True, default=str).encode("utf-8")
            ).hexdigest()

            if expected_hash != event.hash_self:
                violations.append(f"Event {i} (seq={event.sequence}): self-hash mismatch")

            # Verify prev-hash chain
            if i > 0 and event.hash_prev != prev_hash:
                violations.append(
                    f"Event {i} (seq={event.sequence}): prev-hash broken "
                    f"(expected {prev_hash[:12]}..., got {event.hash_prev[:12]}...)"
                )

            prev_hash = event.hash_self

        return {
            "aggregate_id": aggregate_id,
            "valid": len(violations) == 0,
            "events_checked": len(events),
            "violations": violations,
        }

    async def get_children(self, event_id: str) -> list[StoredEvent]:
        """Get all direct children of an event."""
        async with self.db.engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT id, event_type, version, trace_id, parent_event_id,
                           aggregate_id, aggregate_type, data, metadata,
                           hash_prev, hash_self, created_at, sequence
                    FROM event_store
                    WHERE parent_event_id = :event_id
                    ORDER BY created_at ASC
                """),
                {"event_id": event_id},
            )
            return self._rows_to_events(result.fetchall())

    async def get_lineage_graph(self, root_event_id: str, depth: int = 5) -> dict:
        """Build a parent-child event graph starting from a root event."""
        nodes = {}
        edges = []

        async def _traverse(event_id: str, current_depth: int):
            if current_depth > depth:
                return
            if event_id in nodes:
                return

            async with self.db.engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT id, event_type, version, trace_id, parent_event_id,
                               aggregate_id, aggregate_type, data, metadata,
                               hash_prev, hash_self, created_at, sequence
                        FROM event_store
                        WHERE id = :event_id
                    """),
                    {"event_id": event_id},
                )
                row = result.fetchone()
                if not row:
                    return

                event = self._rows_to_events([row])[0]
                nodes[event_id] = {
                    "event_type": event.event_type,
                    "aggregate_type": event.aggregate_type,
                    "sequence": event.sequence,
                    "created_at": event.created_at,
                }

                # Traverse children
                children = await self.get_children(event_id)
                for child in children:
                    edges.append({"from": event_id, "to": child.id})
                    await _traverse(child.id, current_depth + 1)

        await _traverse(root_event_id, 0)
        return {"nodes": nodes, "edges": edges}

    # ───────────────────────────────────────────────
    # Helpers
    # ───────────────────────────────────────────────

    async def _get_last_event(self, aggregate_id: str) -> Optional[StoredEvent]:
        async with self.db.engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT id, event_type, version, trace_id, parent_event_id,
                           aggregate_id, aggregate_type, data, metadata,
                           hash_prev, hash_self, created_at, sequence
                    FROM event_store
                    WHERE aggregate_id = :aggregate_id
                    ORDER BY sequence DESC
                    LIMIT 1
                """),
                {"aggregate_id": aggregate_id},
            )
            row = result.fetchone()
            if row:
                return self._rows_to_events([row])[0]
        return None

    def _rows_to_events(self, rows) -> list[StoredEvent]:
        out = []
        for row in rows:
            data_raw = row[7]
            if isinstance(data_raw, str):
                data_raw = json.loads(data_raw)
            meta_raw = row[8]
            if isinstance(meta_raw, str):
                meta_raw = json.loads(meta_raw)
            out.append(
                StoredEvent(
                    id=str(row[0]),
                    event_type=str(row[1]),
                    version=str(row[2]),
                    trace_id=str(row[3]),
                    parent_event_id=str(row[4]) if row[4] else None,
                    aggregate_id=str(row[5]) if row[5] else None,
                    aggregate_type=str(row[6]) if row[6] else None,
                    data=data_raw or {},
                    metadata=meta_raw or {},
                    hash_prev=str(row[9]) if row[9] else None,
                    hash_self=str(row[10]),
                    created_at=row[11].isoformat() if hasattr(row[11], "isoformat") else str(row[11]),
                    sequence=int(row[12]),
                )
            )
        return out

    def _store_cached_events(self, aggregate_id: str, events: list[StoredEvent]) -> None:
        self._cache[aggregate_id] = events
        self._cache.move_to_end(aggregate_id)
        while len(self._cache) > self._max_cache_entries:
            self._cache.popitem(last=False)

    def _store_cached_snapshot(self, aggregate_id: str, state: dict) -> None:
        self._snapshot_cache[aggregate_id] = state
        self._snapshot_cache.move_to_end(aggregate_id)
        while len(self._snapshot_cache) > self._max_snapshot_entries:
            self._snapshot_cache.popitem(last=False)
