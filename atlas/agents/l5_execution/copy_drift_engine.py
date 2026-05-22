"""
copy_drift_engine.py — Phase 21B

Detects and quantifies follower divergence from leaders.
Measures: exposure, pnl, leverage, symbol allocation, execution timing,
slippage amplification, partial-fill divergence.

Drift states: synchronized, mild_drift, elevated_drift, critical_drift.
"""

from __future__ import annotations

import asyncio
import json
import math
import uuid
from typing import Any

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent


# Drift severity thresholds
DRIFT_THRESHOLDS = {
    "synchronized": 0.05,
    "mild_drift": 0.15,
    "elevated_drift": 0.35,
    "critical_drift": 1.0,
}


class CopyDriftEngine(BaseAgent):
    """L5 Agent — Quantitative copy drift measurement and classification."""

    name = "CopyDriftEngine"
    agent_type = "copy_drift"
    layer = "L5"

    def __init__(self, redis_client, db_client):
        super().__init__(self.name, self.agent_type, self.layer, redis_client)
        self.db = db_client
        self._run_interval = 120  # Every 2 minutes

    async def run(self):
        logger.info(f"{self.name}: Starting copy drift monitoring")
        while self.status == "running":
            try:
                await self._measure_all_drift()
            except Exception as e:
                logger.error(f"{self.name}: Drift measurement error: {e}")
            for _ in range(self._run_interval // 10):
                await asyncio.sleep(10)
                if self.status != "running":
                    return

    async def _measure_all_drift(self):
        """Measure drift for all active leader-follower pairs."""
        pairs = await self._load_pairs()
        for leader_id, follower_id, allocation_ratio in pairs:
            try:
                await self._measure_drift(
                    leader_id, follower_id, allocation_ratio
                )
            except Exception as e:
                logger.debug(
                    f"{self.name}: Drift for {leader_id}↔{follower_id}: {e}"
                )

    async def _load_pairs(self) -> list[tuple[str, str, float]]:
        """Load active pairs with allocation ratios."""
        try:
            async with self.db.engine.connect() as conn:
                r = await conn.execute(text("""
                    SELECT l.leader_id, f.follower_id,
                           COALESCE(f.allocation_ratio, 1.0) as alloc
                    FROM copy_leader_accounts l
                    JOIN copy_follower_accounts f ON f.leader_id = l.leader_id
                    WHERE l.is_active = TRUE AND f.is_active = TRUE
                """))
                return [
                    (str(row[0]), str(row[1]), float(row[2]))
                    for row in r.fetchall()
                ]
        except Exception:
            return []

    async def _measure_drift(
        self, leader_id: str, follower_id: str, allocation_ratio: float
    ):
        """Compute drift metrics for a single pair."""
        # Load latest position snapshots
        snapshots = await self._get_latest_snapshots(leader_id, follower_id)
        if not snapshots:
            return

        # Compute component drifts
        exposure_drift = self._compute_exposure_drift(
            snapshots, allocation_ratio
        )
        pnl_drift = self._compute_pnl_drift(snapshots)
        leverage_drift = self._compute_leverage_drift(snapshots)
        symbol_drift = self._compute_symbol_allocation_drift(
            snapshots, allocation_ratio
        )

        # Load execution timing metrics
        timing_drift_ms = await self._get_execution_timing_drift(
            leader_id, follower_id
        )
        slippage_amp = await self._get_slippage_amplification(
            leader_id, follower_id
        )
        partial_fill_div = await self._get_partial_fill_divergence(
            leader_id, follower_id
        )

        # Composite drift score (weighted)
        drift_score = (
            exposure_drift * 0.30
            + pnl_drift * 0.20
            + leverage_drift * 0.15
            + symbol_drift * 0.15
            + min(timing_drift_ms / 5000, 1.0) * 0.10
            + slippage_amp * 0.05
            + partial_fill_div * 0.05
        )

        # Classify severity
        drift_severity = self._classify_severity(drift_score)

        # Generate repair recommendation
        repair = self._recommend_repair(
            drift_severity, exposure_drift, pnl_drift,
            timing_drift_ms, slippage_amp
        )

        sync_quality = max(0.0, 1.0 - drift_score)

        # Persist
        trace_id = uuid.uuid4().hex[:16]
        await self.db._execute_insert(
            """
            INSERT INTO copy_drift_log
                (id, trace_id, leader_id, follower_id,
                 drift_score, drift_severity,
                 exposure_drift, pnl_drift, leverage_drift,
                 symbol_allocation_drift, execution_timing_drift_ms,
                 slippage_amplification, partial_fill_divergence,
                 sync_quality_score, repair_recommendation,
                 metadata, detected_at)
            VALUES
                (:id, :trace_id, :leader_id, :follower_id,
                 :drift_score, :severity,
                 :exp_drift, :pnl_drift, :lev_drift,
                 :sym_drift, :timing_ms,
                 :slip_amp, :pf_div,
                 :sync_q, :repair,
                 CAST(:metadata AS jsonb), NOW())
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "trace_id": trace_id,
                "leader_id": leader_id,
                "follower_id": follower_id,
                "drift_score": round(drift_score, 4),
                "severity": drift_severity,
                "exp_drift": round(exposure_drift, 4),
                "pnl_drift": round(pnl_drift, 4),
                "lev_drift": round(leverage_drift, 4),
                "sym_drift": round(symbol_drift, 4),
                "timing_ms": timing_drift_ms,
                "slip_amp": round(slippage_amp, 4),
                "pf_div": round(partial_fill_div, 4),
                "sync_q": round(sync_quality, 4),
                "repair": repair,
                "metadata": json.dumps({
                    "allocation_ratio": allocation_ratio,
                    "n_symbols": len(snapshots),
                }),
            },
        )

        if drift_severity in ("elevated_drift", "critical_drift"):
            logger.warning(
                f"{self.name}: {drift_severity} detected — "
                f"leader={leader_id} follower={follower_id} "
                f"drift_score={drift_score:.3f}"
            )

    async def _get_latest_snapshots(
        self, leader_id: str, follower_id: str
    ) -> list[dict]:
        """Get latest position state snapshots for the pair."""
        try:
            async with self.db.engine.connect() as conn:
                r = await conn.execute(text("""
                    SELECT DISTINCT ON (symbol)
                        symbol, leader_qty, follower_qty,
                        leader_exposure, follower_exposure,
                        leader_unrealized_pnl, follower_unrealized_pnl,
                        sync_quality_score
                    FROM copy_position_state
                    WHERE leader_id = :lid AND follower_id = :fid
                    ORDER BY symbol, snapshot_at DESC
                """), {"lid": leader_id, "fid": follower_id})
                return [
                    {
                        "symbol": row[0],
                        "leader_qty": float(row[1] or 0),
                        "follower_qty": float(row[2] or 0),
                        "leader_exposure": float(row[3] or 0),
                        "follower_exposure": float(row[4] or 0),
                        "leader_upnl": float(row[5] or 0),
                        "follower_upnl": float(row[6] or 0),
                        "sync_quality": float(row[7] or 1),
                    }
                    for row in r.fetchall()
                ]
        except Exception:
            return []

    def _compute_exposure_drift(
        self, snapshots: list[dict], alloc_ratio: float
    ) -> float:
        """Compute exposure drift normalized by allocation ratio."""
        if not snapshots:
            return 0.0
        total_leader = sum(abs(s["leader_exposure"]) for s in snapshots)
        total_follower = sum(abs(s["follower_exposure"]) for s in snapshots)
        expected_follower = total_leader * alloc_ratio
        if expected_follower < 0.01:
            return 0.0 if total_follower < 0.01 else 1.0
        return min(1.0, abs(total_follower - expected_follower) / expected_follower)

    def _compute_pnl_drift(self, snapshots: list[dict]) -> float:
        """Compute PnL drift between leader and follower."""
        if not snapshots:
            return 0.0
        leader_pnl = sum(s["leader_upnl"] for s in snapshots)
        follower_pnl = sum(s["follower_upnl"] for s in snapshots)
        max_pnl = max(abs(leader_pnl), abs(follower_pnl), 1.0)
        return min(1.0, abs(leader_pnl - follower_pnl) / max_pnl)

    def _compute_leverage_drift(self, snapshots: list[dict]) -> float:
        """Compute leverage drift (ratio of exposure to quantity)."""
        if not snapshots:
            return 0.0
        leader_lev = sum(abs(s["leader_exposure"]) for s in snapshots)
        follower_lev = sum(abs(s["follower_exposure"]) for s in snapshots)
        max_lev = max(leader_lev, follower_lev, 1.0)
        return min(1.0, abs(leader_lev - follower_lev) / max_lev)

    def _compute_symbol_allocation_drift(
        self, snapshots: list[dict], alloc_ratio: float
    ) -> float:
        """Compute per-symbol allocation drift."""
        if not snapshots:
            return 0.0
        drifts = []
        for s in snapshots:
            expected = abs(s["leader_qty"]) * alloc_ratio
            actual = abs(s["follower_qty"])
            if expected < 0.001:
                d = 0.0 if actual < 0.001 else 1.0
            else:
                d = min(1.0, abs(actual - expected) / expected)
            drifts.append(d)
        return sum(drifts) / max(len(drifts), 1)

    async def _get_execution_timing_drift(
        self, leader_id: str, follower_id: str
    ) -> int:
        """Average execution timing drift in ms."""
        try:
            async with self.db.engine.connect() as conn:
                r = await conn.execute(text("""
                    SELECT AVG(execution_latency_ms)
                    FROM copy_replay_events
                    WHERE leader_id = :lid AND follower_id = :fid
                      AND created_at > NOW() - INTERVAL '1 hour'
                """), {"lid": leader_id, "fid": follower_id})
                val = r.scalar()
                return int(val or 0)
        except Exception:
            return 0

    async def _get_slippage_amplification(
        self, leader_id: str, follower_id: str
    ) -> float:
        """Average slippage amplification in bps."""
        try:
            async with self.db.engine.connect() as conn:
                r = await conn.execute(text("""
                    SELECT AVG(ABS(slippage_bps))
                    FROM copy_replay_events
                    WHERE leader_id = :lid AND follower_id = :fid
                      AND created_at > NOW() - INTERVAL '1 hour'
                """), {"lid": leader_id, "fid": follower_id})
                val = r.scalar()
                return min(1.0, float(val or 0) / 50.0)  # Normalize to 0-1
        except Exception:
            return 0.0

    async def _get_partial_fill_divergence(
        self, leader_id: str, follower_id: str
    ) -> float:
        """Detect partial fill divergence."""
        try:
            async with self.db.engine.connect() as conn:
                r = await conn.execute(text("""
                    SELECT COUNT(*) FILTER (WHERE
                        ABS(leader_qty - follower_qty) / GREATEST(leader_qty, 0.001) > 0.1
                    ) as partial_fills,
                    COUNT(*) as total
                    FROM copy_replay_events
                    WHERE leader_id = :lid AND follower_id = :fid
                      AND created_at > NOW() - INTERVAL '1 hour'
                """), {"lid": leader_id, "fid": follower_id})
                row = r.fetchone()
                if row and row[1] > 0:
                    return min(1.0, float(row[0]) / float(row[1]))
                return 0.0
        except Exception:
            return 0.0

    @staticmethod
    def _classify_severity(drift_score: float) -> str:
        """Classify drift severity."""
        if drift_score < DRIFT_THRESHOLDS["synchronized"]:
            return "synchronized"
        elif drift_score < DRIFT_THRESHOLDS["mild_drift"]:
            return "mild_drift"
        elif drift_score < DRIFT_THRESHOLDS["elevated_drift"]:
            return "elevated_drift"
        else:
            return "critical_drift"

    @staticmethod
    def _recommend_repair(
        severity: str, exp_drift: float, pnl_drift: float,
        timing_ms: int, slippage: float
    ) -> str:
        """Generate repair recommendation based on drift components."""
        if severity == "synchronized":
            return "No repair needed"

        parts = []
        if exp_drift > 0.2:
            parts.append("Rebalance follower exposure to match leader allocation")
        if pnl_drift > 0.3:
            parts.append("Investigate PnL divergence — possible missed fills")
        if timing_ms > 2000:
            parts.append("Execution latency elevated — check broker connectivity")
        if slippage > 0.5:
            parts.append("Slippage amplification detected — reduce follower order size")

        if severity == "critical_drift":
            parts.insert(0, "CRITICAL: Consider freezing follower and forcing reconciliation")

        return "; ".join(parts) if parts else f"Monitor drift ({severity})"
