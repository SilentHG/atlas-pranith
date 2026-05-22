"""drift_detection_engine.py — Phase 12: Institutional Drift Detection.

Monitors for statistical drift across multiple dimensions:
  - Feature drift: distribution shift in feature values
  - Strategy drift: strategy behavior deviating from backtest
  - Regime drift: market regime shifts
  - Execution drift: execution quality degradation
  - Performance decay: strategy PnL degradation over time

Outputs:
  - drift_alerts: per-dimension drift signals
  - decay_severity: 0 (no decay) to 1 (critical)
  - retrain_recommendations: which agents/strategies need attention
  - retirement_candidates: strategies that should be retired
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
from atlas.core.serialization import safe_json_dumps


class DriftDetectionEngine(BaseAgent):
    """Institutional drift detection — multi-dimensional drift monitoring."""

    name = "DriftDetectionEngine"
    agent_type = "drift_detection"
    layer = "L7"

    def __init__(self, redis_client=None, db_client=None, run_interval: int = 1800):
        super().__init__(
            name=self.name,
            agent_type=self.agent_type,
            layer=self.layer,
            redis_client=redis_client,
        )
        self.db = db_client
        self.run_interval = run_interval

        # Drift thresholds
        self.FEATURE_DRIFT_THRESHOLD = 0.15       # 15% PSI divergence
        self.STRATEGY_DRIFT_THRESHOLD = 0.20      # 20% score deviation
        self.PERFORMANCE_DECAY_THRESHOLD = 0.10   # 10% per week

        # Rolling window for drift computation
        self.LOOKBACK_DAYS = 30
        self.BASELINE_PERIODS = 3  # number of prior periods to compare

    async def run(self):
        logger.info(f"{self.name}: starting drift detection (every {self.run_interval}s)")
        while self.status == "running":
            try:
                drift_report = await self._compute_drift_report()
                if drift_report:
                    await self._persist_drift(drift_report)
                    await self._publish_drift(drift_report)
                    # Check for retirement candidates
                    if drift_report.get("retirement_candidates"):
                        await self._notify_retirement(drift_report["retirement_candidates"])
            except Exception as e:
                logger.error(f"{self.name}: drift analysis failed: {e}")
            await asyncio.sleep(self.run_interval)

    async def _compute_drift_report(self) -> Optional[dict]:
        """Compute multi-dimensional drift analysis."""
        if not self.db:
            return None

        # 1. Feature drift
        feature_drift = await self._detect_feature_drift()

        # 2. Strategy performance drift
        strategy_drift = await self._detect_strategy_drift()

        # 3. Regime drift
        regime_drift = await self._detect_regime_drift()

        # 4. Execution drift
        execution_drift = await self._detect_execution_drift()

        # 5. Composite severity
        all_drifts = [feature_drift, strategy_drift, regime_drift, execution_drift]
        composite_severity = self._compute_composite_severity(all_drifts)

        # 6. Retirement candidates
        retirement_candidates = self._identify_retirement_candidates(strategy_drift)

        # 7. Retrain recommendations
        retrain_recommendations = self._generate_retrain_recommendations(all_drifts)

        report = {
            "id": str(uuid.uuid4()),
            "detected_at": datetime.utcnow().isoformat(),
            "feature_drift": feature_drift,
            "strategy_drift": strategy_drift,
            "regime_drift": regime_drift,
            "execution_drift": execution_drift,
            "composite_severity": round(float(composite_severity), 4),
            "retirement_candidates": retirement_candidates,
            "retrain_recommendations": retrain_recommendations,
            "n_strategies_monitored": len(strategy_drift.get("per_strategy", [])),
        }
        return report

    async def _detect_feature_drift(self) -> dict:
        """Detect drift in feature distributions."""
        try:
            async with self.db.engine.connect() as conn:
                from sqlalchemy.sql import text
                result = await conn.execute(
                    text("""
                        SELECT feature_name, feature_importance_score,
                               decay_score, n_uses, computed_at
                        FROM feature_importance
                        ORDER BY computed_at DESC
                        LIMIT 100
                    """)
                )
                rows = result.fetchall()

            features = []
            for r in rows:
                features.append({
                    "name": r[0],
                    "importance": float(r[1]) if r[1] is not None else 0,
                    "decay": float(r[2]) if r[2] is not None else 1.0,
                    "n_uses": int(r[3]) if r[3] is not None else 0,
                    "computed_at": r[4],
                })

            # Detect decaying features
            decaying_features = [f for f in features if f["decay"] < (1.0 - self.FEATURE_DRIFT_THRESHOLD)]
            # Detect emerging features (high importance but low usage)
            emerging_features = [f for f in features if f["importance"] > 0.5 and f["n_uses"] < 10]

            drift_score = 0.0
            if features:
                avg_decay = float(np.mean([f["decay"] for f in features]))
                drift_score = 1.0 - avg_decay

            return {
                "drift_detected": len(decaying_features) > 0,
                "drift_score": round(drift_score, 4),
                "decaying_features": [f["name"] for f in decaying_features[:10]],
                "emerging_features": [f["name"] for f in emerging_features[:5]],
                "n_features_analyzed": len(features),
                "avg_feature_decay": round(float(np.mean([f["decay"] for f in features])), 4) if features else 1.0,
            }
        except Exception as e:
            logger.warning(f"{self.name}: feature drift detection failed: {e}")
            return {"drift_detected": False, "drift_score": 0, "error": str(e)}

    async def _detect_strategy_drift(self) -> dict:
        """Detect drift in strategy performance over time."""
        try:
            # Fetch strategies with multiple backtest results for trend analysis
            async with self.db.engine.connect() as conn:
                from sqlalchemy.sql import text
                result = await conn.execute(
                    text("""
                        SELECT s.id, s.name, s.status,
                               b.short_window_score, b.sharpe, b.win_rate,
                               b.total_trades, b.created_at
                        FROM strategies s
                        JOIN backtest_results b ON s.id = b.strategy_id
                        WHERE b.short_window_score IS NOT NULL
                        ORDER BY s.name, b.created_at DESC
                        LIMIT 500
                    """)
                )
                rows = result.fetchall()

            # Group by strategy
            strategy_scores = defaultdict(list)
            for r in rows:
                sid = str(r[0])
                strategy_scores[sid].append({
                    "name": r[1],
                    "score": float(r[3]) if r[3] is not None else 0,
                    "sharpe": float(r[4]) if r[4] is not None else 0,
                    "win_rate": float(r[5]) if r[5] is not None else 0,
                    "trades": int(r[6]) if r[6] is not None else 0,
                    "time": r[7],
                })

            # Compute drift per strategy
            drifting_strategies = []
            for sid, scores in strategy_scores.items():
                if len(scores) < 2:
                    continue
                # Compare most recent vs average of older scores
                scores_sorted = sorted(scores, key=lambda x: x["time"], reverse=True)
                recent = scores_sorted[0]
                older = scores_sorted[1:]
                avg_older_score = float(np.mean([s["score"] for s in older]))

                if avg_older_score > 1:
                    drift_pct = (recent["score"] - avg_older_score) / avg_older_score
                    if abs(drift_pct) > self.STRATEGY_DRIFT_THRESHOLD:
                        drifting_strategies.append({
                            "strategy_id": sid,
                            "strategy_name": recent["name"],
                            "drift_pct": round(float(drift_pct), 4),
                            "recent_score": round(recent["score"], 2),
                            "avg_prior_score": round(avg_older_score, 2),
                            "direction": "improving" if drift_pct > 0 else "decaying",
                        })

            drift_score = len(drifting_strategies) / max(len(strategy_scores), 1)
            return {
                "drift_detected": len(drifting_strategies) > 0,
                "drift_score": round(drift_score, 4),
                "per_strategy": drifting_strategies[:20],
                "n_strategies_analyzed": len(strategy_scores),
                "n_drifting": len(drifting_strategies),
            }
        except Exception as e:
            logger.warning(f"{self.name}: strategy drift detection failed: {e}")
            return {"drift_detected": False, "drift_score": 0, "error": str(e)}

    async def _detect_regime_drift(self) -> dict:
        """Detect regime drift by comparing recent vs historical regime distribution."""
        try:
            async with self.db.engine.connect() as conn:
                from sqlalchemy.sql import text
                result = await conn.execute(
                    text("""
                        SELECT volatility_regime, trend_regime, timestamp
                        FROM market_regime_memory
                        ORDER BY timestamp DESC
                        LIMIT 500
                    """)
                )
                rows = result.fetchall()

            if len(rows) < 20:
                return {"drift_detected": False, "drift_score": 0, "error": "insufficient_data"}

            # Split into recent and historical
            half = len(rows) // 2
            recent = rows[:half]
            historical = rows[half:]

            # Compare regime distributions
            recent_dist = defaultdict(int)
            historical_dist = defaultdict(int)
            for r in recent:
                recent_dist[f"{r[0]}|{r[1]}"] += 1
            for r in historical:
                historical_dist[f"{r[0]}|{r[1]}"] += 1

            # Normalize
            total_recent = sum(recent_dist.values()) or 1
            total_hist = sum(historical_dist.values()) or 1
            recent_pct = {k: v / total_recent for k, v in recent_dist.items()}
            hist_pct = {k: v / total_hist for k, v in historical_dist.items()}

            # PSI-style divergence
            all_keys = set(list(recent_pct.keys()) + list(hist_pct.keys()))
            psi = 0.0
            for k in all_keys:
                r = recent_pct.get(k, 0.001)  # Small baseline to avoid log(0)
                h = hist_pct.get(k, 0.001)
                psi += (r - h) * np.log(r / h)

            drift_detected = psi > 0.5
            return {
                "drift_detected": drift_detected,
                "drift_score": round(float(psi), 4),
                "regime_shift_magnitude": round(float(psi), 4),
                "recent_regime_distribution": dict(recent_pct),
                "historical_regime_distribution": dict(hist_pct),
            }
        except Exception as e:
            logger.warning(f"{self.name}: regime drift detection failed: {e}")
            return {"drift_detected": False, "drift_score": 0, "error": str(e)}

    async def _detect_execution_drift(self) -> dict:
        """Detect drift in execution quality over time."""
        try:
            async with self.db.engine.connect() as conn:
                from sqlalchemy.sql import text
                result = await conn.execute(
                    text("""
                        SELECT execution_regime, fill_quality_score,
                               avg_slippage_bps, rejection_rate, timestamp
                        FROM execution_intelligence
                        ORDER BY timestamp DESC
                        LIMIT 100
                    """)
                )
                rows = result.fetchall()

            if len(rows) < 5:
                return {"drift_detected": False, "drift_score": 0, "error": "insufficient_data"}

            recent = rows[:len(rows) // 2]
            older = rows[len(rows) // 2:]

            recent_fill = float(np.mean([r[1] for r in recent if r[1] is not None])) if recent else 0
            older_fill = float(np.mean([r[1] for r in older if r[1] is not None])) if older else 0

            recent_slippage = float(np.mean([r[2] for r in recent if r[2] is not None])) if recent else 0
            older_slippage = float(np.mean([r[2] for r in older if r[2] is not None])) if older else 0

            fill_degradation = max(0, older_fill - recent_fill) if older_fill > 0 else 0
            slippage_increase = max(0, recent_slippage - older_slippage)

            drift_score = min(1.0, (fill_degradation * 0.5 + slippage_increase * 0.005))
            return {
                "drift_detected": drift_score > 0.1,
                "drift_score": round(drift_score, 4),
                "fill_degradation": round(float(fill_degradation), 4),
                "slippage_increase_bps": round(float(slippage_increase), 2),
                "recent_avg_fill_quality": round(recent_fill, 4),
                "older_avg_fill_quality": round(older_fill, 4),
            }
        except Exception as e:
            logger.warning(f"{self.name}: execution drift detection failed: {e}")
            return {"drift_detected": False, "drift_score": 0, "error": str(e)}

    def _compute_composite_severity(self, drifts: list[dict]) -> float:
        """Compute weighted composite drift severity."""
        weights = {"feature": 0.2, "strategy": 0.35, "regime": 0.25, "execution": 0.2}
        total = 0.0
        for i, (key, weight) in enumerate(weights.items()):
            if i < len(drifts):
                score = drifts[i].get("drift_score", 0)
                if drifts[i].get("drift_detected"):
                    score = max(score, 0.1)  # Minimum floor if detected
                total += weight * score
        return total

    def _identify_retirement_candidates(self, strategy_drift: dict) -> list[dict]:
        """Identify strategies that should be considered for retirement."""
        candidates = []
        for s in strategy_drift.get("per_strategy", []):
            if s["direction"] == "decaying" and abs(s["drift_pct"]) > 0.3:
                candidates.append({
                    "strategy_id": s["strategy_id"],
                    "strategy_name": s["strategy_name"],
                    "drift_pct": s["drift_pct"],
                    "recent_score": s["recent_score"],
                    "reason": "persistent_performance_decay",
                    "severity": "high" if abs(s["drift_pct"]) > 0.5 else "medium",
                })
        candidates.sort(key=lambda x: x["drift_pct"])
        return candidates[:10]

    def _generate_retrain_recommendations(self, drifts: list[dict]) -> list[dict]:
        """Generate agent/component retrain recommendations."""
        recommendations = []
        labels = ["feature_importance", "ideator_strategy", "regime_classifier", "execution_model"]

        for i, drift in enumerate(drifts):
            if i >= len(labels):
                break
            if drift.get("drift_detected") and drift.get("drift_score", 0) > 0.2:
                recommendations.append({
                    "component": labels[i],
                    "severity": "high" if drift["drift_score"] > 0.4 else "medium",
                    "drift_score": drift["drift_score"],
                    "reason": f"drift_detected_at_{drift['drift_score']:.2f}",
                })
        return recommendations

    async def _persist_drift(self, report: dict) -> None:
        """Persist drift report to drift_detection table."""
        if not self.db:
            return
        try:
            await self.db._execute_insert(
                """
                INSERT INTO drift_detection
                    (id, detected_at, feature_drift_score, strategy_drift_score,
                     regime_drift_score, execution_drift_score,
                     composite_severity, n_strategies_monitored,
                     retirement_candidates, retrain_recommendations,
                     metadata)
                VALUES
                    (:id, :detected_at, :feature_drift_score,
                     :strategy_drift_score, :regime_drift_score,
                     :execution_drift_score, :composite_severity,
                     :n_strategies_monitored, :retirement_candidates,
                     :retrain_recommendations, :metadata)
                """,
                {
                    "id": report["id"],
                    "detected_at": report["detected_at"],
                    "feature_drift_score": report["feature_drift"].get("drift_score", 0),
                    "strategy_drift_score": report["strategy_drift"].get("drift_score", 0),
                    "regime_drift_score": report["regime_drift"].get("drift_score", 0),
                    "execution_drift_score": report["execution_drift"].get("drift_score", 0),
                    "composite_severity": report["composite_severity"],
                    "n_strategies_monitored": report["n_strategies_monitored"],
                    "retirement_candidates": safe_json_dumps(report["retirement_candidates"]),
                    "retrain_recommendations": safe_json_dumps(report["retrain_recommendations"]),
                    "metadata": safe_json_dumps({"method": "psi_and_trend_analysis"}),
                },
            )
        except Exception as e:
            logger.warning(f"{self.name}: persist failed: {e}")

    async def _publish_drift(self, report: dict) -> None:
        """Publish drift alerts to Redis."""
        if not self._redis:
            return
        try:
            signal = {
                "type": "drift_detection",
                "detected_at": report["detected_at"],
                "composite_severity": report["composite_severity"],
                "feature_drift_detected": report["feature_drift"].get("drift_detected", False),
                "strategy_drift_detected": report["strategy_drift"].get("drift_detected", False),
                "regime_drift_detected": report["regime_drift"].get("drift_detected", False),
                "execution_drift_detected": report["execution_drift"].get("drift_detected", False),
                "retirement_candidates": len(report["retirement_candidates"]),
            }
            await self._redis.publish("drift_detection_updates", safe_json_dumps(signal))
        except Exception as e:
            logger.warning(f"{self.name}: publish failed: {e}")

    async def _notify_retirement(self, candidates: list[dict]) -> None:
        """Notify retirement engine about potential candidates."""
        if not self._redis:
            return
        try:
            signal = {
                "type": "retirement_candidates",
                "detected_at": datetime.utcnow().isoformat(),
                "candidates": candidates,
            }
            await self._redis.publish("retirement_candidate_updates", safe_json_dumps(signal))
        except Exception as e:
            logger.warning(f"{self.name}: retirement notification failed: {e}")
