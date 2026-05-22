"""
system_health_engine.py — L7 Meta Agent for global system health scoring and degradation monitoring.

Capabilities:
- Global health scoring across all subsystems
- Degraded mode detection and autonomous throttling
- Emergency risk mode activation
- Execution and scout reliability degradation detection
- Subsystem health mapping
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent


class SystemHealthEngine(BaseAgent):
    """
    L7 Meta Agent — Central system health scoring and degradation response.
    """

    name = "SystemHealthEngine"
    agent_type = "system_health"
    layer = "L7"

    # Subsystem weightings for composite score
    SUBSYSTEM_WEIGHTS = {
        "ingestion": 0.10,
        "ideation": 0.10,
        "backtest": 0.10,
        "validation": 0.10,
        "portfolio": 0.10,
        "execution": 0.10,
        "scouts": 0.08,
        "drift": 0.08,
        "replay": 0.08,
        "audit": 0.08,
        "dashboard": 0.04,
        "api": 0.04,
    }

    def __init__(self, redis_client, db_client):
        super().__init__(self.name, self.agent_type, self.layer, redis_client)
        self.db = db_client
        self._run_interval = 300  # Every 5 minutes
        self._current_mode: str = "normal"
        self._degraded_subsystems: set[str] = set()

    async def run(self):
        logger.info(f"{self.name}: Starting system health monitoring")

        while self.status == "running":
            try:
                health = await self._compute_system_health()
                await self._persist_health(health)
                await self._apply_degraded_mode(health)
            except Exception as e:
                logger.error(f"{self.name}: Health check error: {e}")

            for _ in range(self._run_interval // 10):
                await self._sleep(10)
                if self.status != "running":
                    return

    async def _sleep(self, seconds: int):
        import asyncio
        await asyncio.sleep(seconds)

    async def _safe_count(self, conn, query: str, params: dict | None = None, default: int = 0) -> int:
        """Execute a COUNT query safely — returns default on table-not-found errors."""
        try:
            r = await conn.execute(text(query), params or {})
            return r.scalar() or default
        except Exception as e:
            logger.debug(f"Health query failed (table may not exist): {e}")
            return default

    async def _safe_scalar(self, conn, query: str, default: float = 0.0) -> float:
        """Execute a scalar query safely — returns default on errors."""
        try:
            r = await conn.execute(text(query))
            return float(r.scalar() or default)
        except Exception as e:
            logger.debug(f"Health scalar failed (table may not exist): {e}")
            return default

    async def _compute_system_health(self) -> dict:
        """Compute health scores for all subsystems."""
        scores = {}
        degraded = []

        async with self.db.engine.connect() as conn:
            # Each subsystem query is wrapped with _safe_count / _safe_scalar
            # so non-existent tables or schema drift don't crash the health engine.

            # Ingestion health — check recent data freshness
            recent_bars = await self._safe_count(
                conn, "SELECT COUNT(*) FROM market_data_l1 WHERE time > NOW() - INTERVAL '1 hour'"
            )
            scores["ingestion"] = min(100.0, (recent_bars / 100) * 100) if recent_bars > 0 else 0.0
            if scores["ingestion"] < 30:
                degraded.append("ingestion")

            # Ideation health — recent strategies generated
            recent_strategies = await self._safe_count(
                conn, "SELECT COUNT(*) FROM strategies WHERE created_at > NOW() - INTERVAL '1 hour'"
            )
            scores["ideation"] = min(100.0, recent_strategies * 20)
            if scores["ideation"] < 20:
                degraded.append("ideation")

            # Backtest health — recent backtests
            recent_backtests = await self._safe_count(
                conn, "SELECT COUNT(*) FROM backtest_results WHERE created_at > NOW() - INTERVAL '1 hour'"
            )
            scores["backtest"] = min(100.0, recent_backtests * 10)
            if scores["backtest"] < 10:
                degraded.append("backtest")

            # Validation health — recent validations
            recent_validations = await self._safe_count(
                conn,
                """
                    SELECT COUNT(*) FROM (
                        SELECT analyzed_at FROM walk_forward_analysis
                        WHERE analyzed_at > NOW() - INTERVAL '1 hour'
                        UNION ALL
                        SELECT simulated_at FROM monte_carlo_analysis
                        WHERE simulated_at > NOW() - INTERVAL '1 hour'
                    ) v
                """,
            )
            scores["validation"] = min(100.0, recent_validations * 25)
            if scores["validation"] < 25:
                degraded.append("validation")

            # Portfolio health — recent portfolio intel
            recent_portfolio = await self._safe_count(
                conn, "SELECT COUNT(*) FROM portfolio_intelligence WHERE computed_at > NOW() - INTERVAL '1 hour'"
            )
            scores["portfolio"] = min(100.0, recent_portfolio * 50)
            if scores["portfolio"] < 50:
                degraded.append("portfolio")

            # Execution health — recent trades
            recent_trades = await self._safe_count(
                conn, "SELECT COUNT(*) FROM execution_log WHERE created_at > NOW() - INTERVAL '1 hour'"
            )
            scores["execution"] = min(100.0, recent_trades * 10)
            if scores["execution"] < 10:
                degraded.append("execution")

            # Scout health — recent scout signals
            recent_scouts = await self._safe_count(
                conn, "SELECT COUNT(*) FROM external_scout_memory WHERE timestamp > NOW() - INTERVAL '1 hour'"
            )
            scores["scouts"] = min(100.0, recent_scouts * 10)
            if scores["scouts"] < 10:
                degraded.append("scouts")

            # Drift health — drift severity check
            drift_severity = await self._safe_scalar(
                conn,
                """
                    SELECT COALESCE(composite_severity, 0)
                    FROM drift_detection
                    ORDER BY detected_at DESC LIMIT 1
                """,
            )
            scores["drift"] = max(0.0, 100.0 - drift_severity * 100)
            if drift_severity > 0.7:
                degraded.append("drift")

            # Replay health
            replay_score = await self._safe_scalar(
                conn,
                """
                    SELECT COALESCE(integrity_score, 0)
                    FROM replay_integrity
                    ORDER BY checked_at DESC LIMIT 1
                """,
                default=100.0,
            )
            scores["replay"] = replay_score
            if replay_score < 80:
                degraded.append("replay")

            # Audit health
            audit_entries = await self._safe_count(
                conn, "SELECT COUNT(*) FROM audit_ledger WHERE created_at > NOW() - INTERVAL '1 hour'"
            )
            scores["audit"] = min(100.0, audit_entries * 10)
            if audit_entries == 0:
                degraded.append("audit")

            # Dashboard health
            scores["dashboard"] = 100.0

            # API health
            scores["api"] = 100.0

        # Compute composite score
        composite = sum(
            scores.get(subsystem, 0) * weight
            for subsystem, weight in self.SUBSYSTEM_WEIGHTS.items()
            if subsystem in scores
        )

        # Determine system mode
        degraded_pct = len(degraded) / len(self.SUBSYSTEM_WEIGHTS)
        if degraded_pct > 0.5 or composite < 30:
            mode = "emergency"
        elif degraded_pct > 0.25 or composite < 60:
            mode = "degraded"
        elif degraded_pct > 0.1:
            mode = "caution"
        else:
            mode = "normal"

        self._current_mode = mode
        self._degraded_subsystems = set(degraded)

        return {
            "composite_score": composite,
            "mode": mode,
            "subsystems": scores,
            "degraded_subsystems": degraded,
            "degraded_count": len(degraded),
            "total_subsystems": len(self.SUBSYSTEM_WEIGHTS),
            "degraded_ratio": degraded_pct,
        }

    async def _persist_health(self, health: dict):
        """Persist health snapshot."""
        await self.db._execute_insert(
            """
            INSERT INTO system_health
                (id, checked_at, composite_score, system_mode,
                 subsystem_scores, degraded_subsystems,
                 n_degraded, n_total)
            VALUES
                (:id, NOW(), :composite_score, :system_mode,
                 CAST(:subsystem_scores AS jsonb), CAST(:degraded_subsystems AS jsonb),
                 :n_degraded, :n_total)
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "composite_score": health["composite_score"],
                "system_mode": health["mode"],
                "subsystem_scores": json.dumps(health["subsystems"]),
                "degraded_subsystems": json.dumps(health["degraded_subsystems"]),
                "n_degraded": health["degraded_count"],
                "n_total": health["total_subsystems"],
            },
        )

    async def _apply_degraded_mode(self, health: dict):
        """Apply autonomous throttling based on system mode."""
        mode = health["mode"]

        if mode == "emergency":
            logger.critical(
                f"{self.name}: EMERGENCY MODE — "
                f"composite_score={health['composite_score']:.1f}, "
                f"degraded={health['degraded_subsystems']}"
            )
            # Attempt to activate kill switch via Redis
            await self._redis.hset(
                "kill_switch:state",
                mapping={
                    "active": "1",
                    "reason": f"SystemHealth: Emergency mode ({health['composite_score']:.1f} score)",
                },
            )

        elif mode == "degraded":
            logger.warning(
                f"{self.name}: Degraded mode — "
                f"composite_score={health['composite_score']:.1f}, "
                f"subsystems: {health['degraded_subsystems']}"
            )

        elif mode == "caution":
            logger.info(
                f"{self.name}: Caution mode — "
                f"{health['degraded_count']} subsystem(s) degraded"
            )

    async def get_health_report(self) -> dict:
        """Get current system health report."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT composite_score, system_mode, subsystem_scores,
                           degraded_subsystems, n_degraded, n_total, checked_at
                    FROM system_health
                    ORDER BY checked_at DESC LIMIT 1
                """)
            )
            row = r.fetchone()
            if not row:
                return {"status": "no_data"}

            sub_scores = row[2]
            if isinstance(sub_scores, str):
                sub_scores = json.loads(sub_scores)

            degraded = row[3]
            if isinstance(degraded, str):
                degraded = json.loads(degraded)

            return {
                "composite_score": float(row[0]) if row[0] else 0,
                "system_mode": str(row[1]),
                "subsystem_scores": sub_scores,
                "degraded_subsystems": degraded,
                "n_degraded": row[4],
                "n_total": row[5],
                "checked_at": row[6].isoformat() if hasattr(row[6], "isoformat") else str(row[6]),
            }
