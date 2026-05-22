"""
replay_engine.py — Deterministic replay of entire ATLAS sessions.

Capabilities:
- Replay entire sessions (executions, portfolios, scout states, fills, mutations, drift events)
- Generate replay_integrity_score
- Produce divergence_reports and state_mismatch_reports
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent
from atlas.core.event_store import EventStore


class ReplayEngine(BaseAgent):
    """
    L7 Meta Agent — Deterministic replay for integrity verification.

    Can replay any event-sourced aggregate and compare against persisted state.
    """

    name = "ReplayEngine"
    agent_type = "replay"
    layer = "L7"

    def __init__(self, redis_client, db_client):
        super().__init__(self.name, self.agent_type, self.layer, redis_client)
        self.db = db_client
        self.event_store = EventStore(db_client)
        self._run_interval = 3600  # Run every hour

    async def run(self):
        logger.info(f"{self.name}: Starting replay integrity sweeps")

        while self.status == "running":
            try:
                await self._sweep_replay_checks()
            except Exception as e:
                logger.error(f"{self.name}: Sweep error: {e}")

            for _ in range(self._run_interval // 10):
                await self._sleep(10)
                if self.status != "running":
                    return

    async def _sleep(self, seconds: int):
        import asyncio
        await asyncio.sleep(seconds)

    async def _sweep_replay_checks(self):
        """Perform periodic replay integrity checks."""
        # Get recent aggregates from event store
        aggregates = await self.event_store.get_all_aggregates(limit=20)
        if not aggregates:
            logger.info(f"{self.name}: No aggregates found for replay check")
            return

        results = []
        for agg in aggregates:
            integrity = await self.event_store.verify_integrity(agg["aggregate_id"])
            results.append(integrity)

            if not integrity["valid"]:
                logger.warning(
                    f"{self.name}: Integrity violation in {agg['aggregate_id']}: "
                    f"{integrity['violations']}"
                )

        # Compute aggregate replay integrity score
        total_events = sum(r["events_checked"] for r in results)
        valid_count = sum(1 for r in results if r["valid"])
        integrity_score = (valid_count / len(results) * 100) if results else 100.0

        # Persist replay check result
        await self.db._execute_insert(
            """
            INSERT INTO replay_integrity
                (id, checked_at, n_aggregates_checked, n_events_checked,
                 integrity_score, n_violations, details)
            VALUES
                (:id, NOW(), :n_aggregates, :n_events,
                 :integrity_score, :violations, CAST(:details AS jsonb))
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "n_aggregates": len(results),
                "n_events": total_events,
                "integrity_score": integrity_score,
                "violations": sum(len(r["violations"]) for r in results),
                "details": json.dumps({
                    "aggregates": [r["aggregate_id"] for r in results],
                    "violations_detail": [r["violations"] for r in results if r["violations"]],
                }),
            },
        )

        logger.info(
            f"{self.name}: Replay check complete — "
            f"{valid_count}/{len(results)} aggregates valid, "
            f"{total_events} events checked, score={integrity_score:.1f}%"
        )

    async def replay_strategy_lifecycle(self, trace_id: str) -> dict:
        """Replay the full lifecycle of a strategy from its trace."""
        events = await self.event_store.get_events_by_trace(trace_id)
        if not events:
            return {"trace_id": trace_id, "replayable": False, "events_found": 0}

        # Group events by aggregate
        stages = {}
        for event in events:
            stage = event.event_type
            if stage not in stages:
                stages[stage] = []
            stages[stage].append(event)

        return {
            "trace_id": trace_id,
            "replayable": True,
            "events_found": len(events),
            "stages": list(stages.keys()),
            "stage_counts": {k: len(v) for k, v in stages.items()},
            "time_span": {
                "first_event": events[0].created_at,
                "last_event": events[-1].created_at,
            },
        }

    async def replay_execution(self, strategy_id: str) -> dict:
        """Replay execution events for a strategy."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT id, trace_id, event_type, data, created_at
                    FROM event_store
                    WHERE aggregate_id = :strategy_id
                      AND event_type LIKE '%execution%'
                      OR event_type LIKE '%fill%'
                      OR event_type LIKE '%order%'
                    ORDER BY created_at ASC
                """),
                {"strategy_id": strategy_id},
            )
            rows = r.fetchall()

        events = []
        for row in rows:
            data_raw = row[3]
            if isinstance(data_raw, str):
                try:
                    data_raw = json.loads(data_raw)
                except Exception:
                    data_raw = {}
            events.append({
                "id": str(row[0]),
                "trace_id": str(row[1]),
                "event_type": str(row[2]),
                "data": data_raw,
                "created_at": row[4].isoformat() if hasattr(row[4], "isoformat") else str(row[4]),
            })

        return {
            "strategy_id": strategy_id,
            "execution_events": len(events),
            "events": events,
        }

    async def compare_replay_to_live(self, aggregate_id: str) -> dict:
        """
        Compare replay-derived state against current live DB state.
        Returns divergence report.
        """
        # Replay state from events
        replayed_state = await self.event_store.replay_aggregate(
            aggregate_id,
            lambda state, data: {**(state or {}), **data},
        )

        # Get live state from DB (strategy record)
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT id, name, status, parameters, code
                    FROM strategies WHERE id = :id
                """),
                {"id": aggregate_id},
            )
            live_row = r.fetchone()

        if not live_row:
            return {
                "aggregate_id": aggregate_id,
                "error": "No live state found",
                "replayable": replayed_state is not None,
            }

        live_state = {
            "id": str(live_row[0]),
            "name": str(live_row[1]),
            "status": str(live_row[2]),
        }

        divergences = []
        if replayed_state:
            for key in replayed_state:
                if key in live_state and str(replayed_state[key]) != str(live_state[key]):
                    divergences.append({
                        "key": key,
                        "replay_value": replayed_state[key],
                        "live_value": live_state[key],
                    })

        return {
            "aggregate_id": aggregate_id,
            "replayable": True,
            "has_live_state": True,
            "divergences": divergences,
            "n_divergences": len(divergences),
            "match": len(divergences) == 0,
        }
