"""strategy_retirement_engine.py — Phase 12: Institutional Strategy Retirement.

Monitors and manages strategy lifecycle:
  - Rolling degradation detection
  - Underperformance persistence (must persist for N cycles)
  - Overfit relapse (strategy that was good but regressed)
  - Live-vs-backtest divergence tracking
  - Drift-triggered retirement signals
  - Capital withdrawal recommendations

Outputs:
  - retirement_recommendations: per-strategy retirement flags
  - capital_withdrawal_signals: % of capital to withdraw
  - strategy_lifecycle_state: active / monitor / retirement_pending / retired
"""

import asyncio
import json
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional

import numpy as np
from loguru import logger

from atlas.core.agent_base import BaseAgent


class StrategyRetirementEngine(BaseAgent):
    """Institutional strategy retirement — lifecycle governance."""

    name = "StrategyRetirementEngine"
    agent_type = "strategy_retirement"
    layer = "L7"

    def __init__(self, redis_client=None, db_client=None, run_interval: int = 3600):
        super().__init__(
            name=self.name,
            agent_type=self.agent_type,
            layer=self.layer,
            redis_client=redis_client,
        )
        self.db = db_client
        self.run_interval = run_interval

        # Retirement thresholds
        self.MIN_SCORE_SURVIVAL = 30.0          # Below this → retirement risk
        self.DEGRADATION_PERSISTENCE_CYCLES = 3  # Must degrade for 3 cycles
        self.MAX_DIVERGENCE_PCT = 0.50          # 50% divergence from backtest
        self.OVERFIT_RELAPSE_THRESHOLD = 0.35   # 35% score drop indicates relapse

        # Retirement lifecycle
        self.LIFECYCLE_STATES = ["active", "monitor", "retirement_pending", "retired"]

        # Per-strategy tracking
        self._degradation_count: dict[str, int] = defaultdict(int)

    async def run(self):
        logger.info(f"{self.name}: starting retirement analysis (every {self.run_interval}s)")
        while self.status == "running":
            try:
                retirement = await self._compute_retirement_analysis()
                if retirement:
                    await self._persist_retirement(retirement)
                    await self._publish_retirement(retirement)
            except Exception as e:
                logger.error(f"{self.name}: retirement analysis failed: {e}")
            await asyncio.sleep(self.run_interval)

    async def _compute_retirement_analysis(self) -> Optional[dict]:
        """Compute comprehensive retirement analysis for all strategies."""
        if not self.db:
            return None

        strategies = await self._fetch_strategies()
        drift_report = await self._fetch_latest_drift()

        if not strategies:
            return None

        lifecycle_states = {}
        retirement_recommendations = []
        capital_withdrawal_signals = []

        for s in strategies:
            sid = s["id"]
            lifecycle = self._assess_strategy_lifecycle(s, drift_report)
            lifecycle_states[sid] = lifecycle

            if lifecycle["state"] in ("retirement_pending", "retired"):
                retirement_recommendations.append({
                    "strategy_id": sid,
                    "strategy_name": s["name"],
                    "current_state": lifecycle["state"],
                    "reason": lifecycle["reason"],
                    "severity": lifecycle["severity"],
                    "recent_score": lifecycle["recent_score"],
                    "peak_score": lifecycle["peak_score"],
                })

                withdrawal_pct = self._compute_withdrawal_pct(lifecycle)
                if withdrawal_pct > 0:
                    capital_withdrawal_signals.append({
                        "strategy_id": sid,
                        "strategy_name": s["name"],
                        "withdrawal_pct": withdrawal_pct,
                        "reason": lifecycle["reason"],
                    })

        # Sort by severity
        retirement_recommendations.sort(key=lambda x: {"high": 0, "medium": 1, "low": 2}.get(x["severity"], 3))

        report = {
            "id": str(uuid.uuid4()),
            "analyzed_at": datetime.utcnow().isoformat(),
            "n_strategies_analyzed": len(strategies),
            "n_active": sum(1 for l in lifecycle_states.values() if l["state"] == "active"),
            "n_monitor": sum(1 for l in lifecycle_states.values() if l["state"] == "monitor"),
            "n_retirement_pending": sum(1 for l in lifecycle_states.values() if l["state"] == "retirement_pending"),
            "n_retired": sum(1 for l in lifecycle_states.values() if l["state"] == "retired"),
            "lifecycle_states": lifecycle_states,
            "retirement_recommendations": retirement_recommendations,
            "capital_withdrawal_signals": capital_withdrawal_signals,
        }
        return report

    async def _fetch_strategies(self) -> list[dict]:
        """Fetch strategies with backtest results for lifecycle tracking."""
        try:
            async with self.db.engine.connect() as conn:
                from sqlalchemy.sql import text
                # Get latest scores per strategy
                result = await conn.execute(
                    text("""
                        SELECT DISTINCT ON (s.id)
                            s.id, s.name, s.status, s.created_at,
                            b.short_window_score, b.sharpe, b.win_rate,
                            b.total_trades, b.max_drawdown, b.created_at as backtest_time
                        FROM strategies s
                        JOIN backtest_results b ON s.id = b.strategy_id
                        WHERE b.short_window_score IS NOT NULL
                        ORDER BY s.id, b.created_at DESC
                        LIMIT 100
                    """)
                )
                rows = result.fetchall()

                # Also get historical scores for degradation tracking
                hist_result = await conn.execute(
                    text("""
                        SELECT s.id, b.short_window_score, b.created_at
                        FROM strategies s
                        JOIN backtest_results b ON s.id = b.strategy_id
                        WHERE b.short_window_score IS NOT NULL
                        ORDER BY s.id, b.created_at ASC
                        LIMIT 1000
                    """)
                )
                hist_rows = hist_result.fetchall()

            # Build historical score map
            hist_scores = defaultdict(list)
            for r in hist_rows:
                hist_scores[str(r[0])].append({
                    "score": float(r[1]) if r[1] is not None else 0,
                    "time": r[2],
                })

            out = []
            for r in rows:
                sid = str(r[0])
                hist = hist_scores.get(sid, [])
                peak_score = max([h["score"] for h in hist]) if hist else 0
                avg_score = float(np.mean([h["score"] for h in hist])) if hist else 0

                out.append({
                    "id": sid,
                    "name": r[1],
                    "status": r[2],
                    "created_at": r[3],
                    "recent_score": float(r[4]) if r[4] is not None else 0,
                    "sharpe": float(r[5]) if r[5] is not None else 0,
                    "win_rate": float(r[6]) if r[6] is not None else 0,
                    "total_trades": int(r[7]) if r[7] is not None else 0,
                    "max_drawdown": float(r[8]) if r[8] is not None else 0,
                    "backtest_time": r[9],
                    "peak_score": peak_score,
                    "avg_score": avg_score,
                    "score_history": hist[-10:],  # Last 10 scores
                })
            return out
        except Exception as e:
            logger.warning(f"{self.name}: fetch strategies failed: {e}")
            return []

    async def _fetch_latest_drift(self) -> dict:
        """Fetch latest drift detection report."""
        try:
            async with self.db.engine.connect() as conn:
                from sqlalchemy.sql import text
                result = await conn.execute(
                    text("""
                        SELECT id, composite_severity, feature_drift_score,
                               strategy_drift_score, regime_drift_score,
                               execution_drift_score, retirement_candidates,
                               detected_at
                        FROM drift_detection
                        ORDER BY detected_at DESC
                        LIMIT 1
                    """)
                )
                row = result.fetchone()
                if row:
                    retirement_candidates = row[6]
                    if isinstance(retirement_candidates, str):
                        retirement_candidates = json.loads(retirement_candidates)
                    return {
                        "id": str(row[0]),
                        "composite_severity": float(row[1]) if row[1] else 0,
                        "feature_drift_score": float(row[2]) if row[2] else 0,
                        "strategy_drift_score": float(row[3]) if row[3] else 0,
                        "regime_drift_score": float(row[4]) if row[4] else 0,
                        "execution_drift_score": float(row[5]) if row[5] else 0,
                        "retirement_candidates": retirement_candidates or [],
                        "detected_at": row[7],
                    }
        except Exception:
            pass
        return {}

    def _assess_strategy_lifecycle(self, strategy: dict, drift_report: dict) -> dict:
        """Assess the lifecycle state of a single strategy."""
        sid = strategy["id"]
        recent_score = strategy["recent_score"]
        peak_score = max(strategy["peak_score"], 1)
        avg_score = strategy["avg_score"]

        # Check for retirement candidates from drift report
        drift_candidates = {c["strategy_id"] for c in drift_report.get("retirement_candidates", [])}

        # 1. Score-based assessment
        score_degradation = (peak_score - recent_score) / peak_score if peak_score > 0 else 0

        # 2. Overfit relapse check
        overfit_relapse = score_degradation > self.OVERFIT_RELAPSE_THRESHOLD

        # 3. Live-vs-backtest divergence (score has dropped significantly from average)
        divergence = abs(avg_score - recent_score) / max(avg_score, 1)

        # 4. Persistence tracking
        if score_degradation > 0.15:
            self._degradation_count[sid] += 1
        else:
            self._degradation_count[sid] = max(0, self._degradation_count[sid] - 1)

        persistence = self._degradation_count[sid]

        # Determine state
        if sid in drift_candidates or (overfit_relapse and persistence >= self.DEGRADATION_PERSISTENCE_CYCLES):
            state = "retired"
            reason = "overfit_relapse_and_drift_confirmed"
            severity = "high"
        elif recent_score < self.MIN_SCORE_SURVIVAL and persistence >= self.DEGRADATION_PERSISTENCE_CYCLES:
            state = "retirement_pending"
            reason = "persistent_below_minimum_score"
            severity = "high"
        elif divergence > self.MAX_DIVERGENCE_PCT and persistence >= 2:
            state = "retirement_pending"
            reason = "live_backtest_divergence"
            severity = "medium"
        elif score_degradation > 0.15 and persistence >= 2:
            state = "retirement_pending"
            reason = "score_degradation_persistence"
            severity = "medium"
        elif score_degradation > 0.10 or persistence >= 1:
            state = "monitor"
            reason = "initial_degradation_detected"
            severity = "low"
        else:
            state = "active"
            reason = "normal_performance"
            severity = "none"

        return {
            "state": state,
            "reason": reason,
            "severity": severity,
            "recent_score": round(recent_score, 2),
            "peak_score": round(peak_score, 2),
            "avg_score": round(avg_score, 2),
            "score_degradation_pct": round(float(score_degradation * 100), 1),
            "divergence_pct": round(float(divergence * 100), 1),
            "degradation_persistence_count": persistence,
            "overfit_relapse_detected": overfit_relapse,
        }

    def _compute_withdrawal_pct(self, lifecycle: dict) -> float:
        """Compute the % of capital to withdraw based on lifecycle state."""
        if lifecycle["state"] == "retired":
            return 1.0  # Full withdrawal
        elif lifecycle["state"] == "retirement_pending":
            if lifecycle["severity"] == "high":
                return 0.75
            elif lifecycle["severity"] == "medium":
                return 0.50
            return 0.25
        elif lifecycle["state"] == "monitor":
            return 0.0  # No withdrawal, just monitoring
        return 0.0

    async def _persist_retirement(self, report: dict) -> None:
        """Persist retirement analysis."""
        if not self.db:
            return
        try:
            await self.db._execute_insert(
                """
                INSERT INTO strategy_retirement
                    (id, analyzed_at, n_strategies_analyzed,
                     n_active, n_monitor, n_retirement_pending, n_retired,
                     lifecycle_states, retirement_recommendations,
                     capital_withdrawal_signals, metadata)
                VALUES
                    (:id, :analyzed_at, :n_strategies_analyzed,
                     :n_active, :n_monitor, :n_retirement_pending, :n_retired,
                     :lifecycle_states, :retirement_recommendations,
                     :capital_withdrawal_signals, :metadata)
                """,
                {
                    "id": report["id"],
                    "analyzed_at": report["analyzed_at"],
                    "n_strategies_analyzed": report["n_strategies_analyzed"],
                    "n_active": report["n_active"],
                    "n_monitor": report["n_monitor"],
                    "n_retirement_pending": report["n_retirement_pending"],
                    "n_retired": report["n_retired"],
                    "lifecycle_states": json.dumps(report["lifecycle_states"]),
                    "retirement_recommendations": json.dumps(report["retirement_recommendations"]),
                    "capital_withdrawal_signals": json.dumps(report["capital_withdrawal_signals"]),
                    "metadata": json.dumps({"method": "score_divergence_and_persistence"}),
                },
            )
        except Exception as e:
            logger.warning(f"{self.name}: persist failed: {e}")

    async def _publish_retirement(self, report: dict) -> None:
        """Publish retirement signals to Redis."""
        if not self._redis:
            return
        try:
            signal = {
                "type": "strategy_retirement",
                "analyzed_at": report["analyzed_at"],
                "n_retirement_pending": report["n_retirement_pending"],
                "n_retired": report["n_retired"],
                "withdrawal_signals": report["capital_withdrawal_signals"][:5],
            }
            await self._redis.publish("strategy_retirement_updates", json.dumps(signal))
        except Exception as e:
            logger.warning(f"{self.name}: publish failed: {e}")
