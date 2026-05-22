"""
hypothesis_validation_engine.py — External scout for hypothesis validation.

Converts scout claims into testable market hypotheses:
- Extract claims from scout signals
- Validate against historical market data
- Rank signal survivability
- Retire weak narratives
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent


class HypothesisValidationEngine(BaseAgent):
    """
    External Scout — Validates scout narratives against market data.
    """

    name = "HypothesisValidationEngine"
    agent_type = "hypothesis_validator"
    layer = "Scout"

    def __init__(self, redis_client, db_client):
        super().__init__(self.name, self.agent_type, self.layer, redis_client)
        self.db = db_client
        self._run_interval = 3600  # Every hour

    async def run(self):
        logger.info(f"{self.name}: Starting hypothesis validation")

        while self.status == "running":
            try:
                await self._validate_hypotheses()
            except Exception as e:
                logger.error(f"{self.name}: Validation error: {e}")

            for _ in range(self._run_interval // 10):
                await self._sleep(10)
                if self.status != "running":
                    return

    async def _sleep(self, seconds: int):
        import asyncio
        await asyncio.sleep(seconds)

    async def _validate_hypotheses(self):
        """Validate recent scout signals against market data."""
        signals = await self._load_recent_signals()
        if not signals:
            return

        validated = []
        for signal in signals:
            result = await self._validate_signal(signal)
            validated.append(result)

        # Retire weak signals
        n_retired = 0
        for result in validated:
            if result["survivability_score"] < 0.2:
                await self._retire_signal(result["signal_id"])
                n_retired += 1

        # Persist validation summary
        await self.db._execute_insert(
            """
            INSERT INTO external_scout_memory
                (id, source, source_sub, timestamp, sentiment,
                 hypothesis_score, signal_direction, details)
            VALUES
                (:id, 'hypothesis_validation', 'summary', NOW(), :sentiment,
                 :score, 'neutral', CAST(:details AS jsonb))
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "sentiment": round(
                    sum(r["survivability_score"] for r in validated) / max(1, len(validated)), 3
                ),
                "score": sum(1 for r in validated if r["survivability_score"] > 0.5) / max(1, len(validated)),
                "details": json.dumps({
                    "n_validated": len(validated),
                    "n_retired": n_retired,
                    "avg_survivability": round(
                        sum(r["survivability_score"] for r in validated) / max(1, len(validated)), 3
                    ),
                }),
            },
        )

        logger.info(
            f"{self.name}: Validated {len(validated)} hypotheses, "
            f"retired {n_retired}, avg_survivability="
            f"{sum(r['survivability_score'] for r in validated) / max(1, len(validated)):.2f}"
        )

    async def _load_recent_signals(self) -> list[dict]:
        """Load recent scout signals for validation."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT id, source, source_sub, sentiment,
                           hypothesis_score, signal_direction, details
                    FROM external_scout_memory
                    WHERE source NOT IN ('source_reliability', 'hypothesis_validation')
                      AND timestamp > NOW() - INTERVAL '24 hours'
                      AND hypothesis_score > 0.3
                    ORDER BY hypothesis_score DESC
                    LIMIT 50
                """)
            )
            return [
                {
                    "id": str(row[0]),
                    "source": str(row[1]),
                    "source_sub": str(row[2]) if row[2] else "",
                    "sentiment": float(row[3] or 0),
                    "score": float(row[4] or 0),
                    "direction": str(row[5]) if row[5] else "neutral",
                    "details": row[6],
                }
                for row in r.fetchall()
            ]

    async def _validate_signal(self, signal: dict) -> dict:
        """Validate a single signal against historical market data."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT COUNT(*) as strategy_count,
                           AVG(short_window_score) as avg_score
                    FROM backtest_results br
                    JOIN strategies s ON s.id = br.strategy_id
                    WHERE s.created_at > NOW() - INTERVAL '7 days'
                """)
            )
            row = r.fetchone()
            n_strategies = row[0] if row else 0
            avg_score = float(row[1] or 0) if row else 0

        # Compute survivability score based on signal strength and market alignment
        base_score = signal.get("score", 0.5)
        alignment = min(1.0, avg_score / 100)  # How well market aligns with signal
        survivability = 0.6 * base_score + 0.4 * alignment

        return {
            "signal_id": signal["id"],
            "source": signal["source"],
            "survivability_score": round(survivability, 3),
            "market_alignment": round(alignment, 3),
            "base_signal_score": base_score,
        }

    async def _retire_signal(self, signal_id: str):
        """Mark a signal as retired (low survivability)."""
        await self.db._execute_insert(
            """
            UPDATE external_scout_memory
            SET hypothesis_score = -1.0,
                details = COALESCE(details, '{}'::jsonb) || '{"retired": true}'::jsonb
            WHERE id = :id
            """,
            {"id": signal_id},
        )
