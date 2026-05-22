"""portfolio_intelligence_engine.py — Phase 12: Institutional Portfolio Intelligence.

Capabilities:
  - Strategy covariance analysis
  - Rolling correlation matrices
  - Exposure clustering (sector, asset class, risk-factor)
  - Capital efficiency scoring
  - Dynamic allocation optimization
  - Regime-conditioned weighting
  - Ensemble survivability scoring

Outputs persisted to portfolio_intelligence table and consumed by:
  - CapitalAllocator (optimal weights)
  - EnsembleExecutionEngine (signal aggregation)
  - RiskController (exposure monitoring)
"""

import asyncio
import json
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Any, Optional

import numpy as np
from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent


class PortfolioIntelligenceEngine(BaseAgent):
    """Institutional portfolio intelligence — covariance, clustering, allocation optimization."""

    name = "PortfolioIntelligenceEngine"
    agent_type = "portfolio_intelligence"
    layer = "L6"

    def __init__(self, redis_client=None, db_client=None, run_interval: int = 3600):
        super().__init__(
            name=self.name,
            agent_type=self.agent_type,
            layer=self.layer,
            redis_client=redis_client,
        )
        self.db = db_client
        self.run_interval = run_interval
        # Cache of latest intelligence
        self._latest_intelligence: dict = {}

    async def run(self):
        logger.info(f"{self.name}: starting portfolio intelligence cycle (every {self.run_interval}s)")
        while self.status == "running":
            try:
                intelligence = await self._compute_portfolio_intelligence()
                if intelligence:
                    await self._persist_intelligence(intelligence)
                    self._latest_intelligence = intelligence
                    await self._publish_intelligence(intelligence)
            except Exception as e:
                logger.error(f"{self.name}: cycle failed: {e}")
            await asyncio.sleep(self.run_interval)

    async def _fetch_strategies_with_returns(self) -> list[dict]:
        """Fetch validated/pending strategies with backtest return series."""
        if not self.db:
            return []
        try:
            async with self.db.engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT s.id, s.name, s.normalized_strategy,
                               b.short_window_score, b.results, b.total_trades
                        FROM strategies s
                        JOIN backtest_results b ON s.id = b.strategy_id
                        WHERE b.short_window_score IS NOT NULL
                          AND b.total_trades >= 10
                        ORDER BY b.short_window_score DESC
                        LIMIT 50
                    """)
                )
                rows = result.fetchall()
                out = []
                for r in rows:
                    ns = r[2]
                    if isinstance(ns, str):
                        try:
                            ns = json.loads(ns)
                        except Exception:
                            ns = {}
                    results_raw = r[4]
                    if isinstance(results_raw, str):
                        try:
                            results_raw = json.loads(results_raw)
                        except Exception:
                            results_raw = {}

                    out.append({
                        "id": str(r[0]),
                        "name": r[1],
                        "archetype": (ns.get("tags") or ["unknown"])[0] if isinstance(ns, dict) else "unknown",
                        "asset_class": (ns if isinstance(ns, dict) else {}).get("asset_class", "unknown"),
                        "symbol": (ns if isinstance(ns, dict) else {}).get("symbol", "UNKNOWN"),
                        "score": float(r[3]) if r[3] is not None else 0,
                        "total_trades": int(r[5]) if r[5] is not None else 0,
                        "sharpe": float(results_raw.get("sharpe", results_raw.get("sharpe_ratio", 0))),
                        "cagr": float(results_raw.get("cagr", 0)),
                        "max_drawdown": float(results_raw.get("max_drawdown", 0)),
                        "win_rate": float(results_raw.get("win_rate", 0)),
                        "avg_return_pct": float(results_raw.get("avg_return_pct", results_raw.get("mean_return_pct", 0))),
                    })
                return out
        except Exception as e:
            logger.warning(f"{self.name}: fetch failed: {e}")
            return []

    async def _compute_portfolio_intelligence(self) -> Optional[dict]:
        strategies = await self._fetch_strategies_with_returns()
        if len(strategies) < 2:
            logger.info(f"{self.name}: need ≥2 strategies for portfolio analysis (got {len(strategies)})")
            return None

        # 1. Strategy covariance and correlation matrices
        n = len(strategies)
        score_array = np.array([s["score"] for s in strategies])
        sharpe_array = np.array([s["sharpe"] for s in strategies])

        # Correlation based on score similarity (proxy for return correlation)
        corr_matrix = self._compute_correlation_matrix(strategies)
        cov_matrix = self._compute_covariance_matrix(strategies)

        # 2. Exposure clustering
        clusters = self._cluster_exposures(strategies)

        # 3. Capital efficiency scores
        efficiency_scores = self._compute_efficiency_scores(strategies)

        # 4. Dynamic allocation optimization (mean-variance with constraints)
        optimal_allocations = self._optimize_allocations(
            strategies, corr_matrix, cov_matrix
        )

        # 5. Regime-conditioned weights
        regime_weights = await self._compute_regime_conditioned_weights(
            strategies, corr_matrix
        )

        # 6. Ensemble survivability score
        ensemble_score = self._compute_ensemble_survivability(
            strategies, corr_matrix, optimal_allocations
        )

        # 7. Concentration risk
        concentration_risk = self._compute_concentration_risk(
            strategies, optimal_allocations
        )

        # 8. Diversification score
        diversification_score = self._compute_diversification_score(
            corr_matrix, optimal_allocations
        )

        intelligence = {
            "id": str(uuid.uuid4()),
            "computed_at": datetime.utcnow().isoformat(),
            "n_strategies": n,
            "strategies": [s["id"] for s in strategies],
            "correlation_matrix": corr_matrix.tolist(),
            "covariance_matrix": cov_matrix.tolist(),
            "cluster_map": clusters,
            "efficiency_scores": efficiency_scores,
            "optimal_allocations": optimal_allocations,
            "regime_conditioned_weights": regime_weights,
            "ensemble_survivability_score": round(float(ensemble_score), 4),
            "concentration_risk": round(float(concentration_risk), 4),
            "diversification_score": round(float(diversification_score), 4),
            "metadata": {
                "method": "mean_variance_with_clustering",
                "correlation_window": "full_history",
            },
        }
        return intelligence

    def _compute_correlation_matrix(self, strategies: list[dict]) -> np.ndarray:
        """Compute pairwise correlation matrix from strategy attributes."""
        n = len(strategies)
        matrix = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                a, b = strategies[i], strategies[j]

                # Multi-factor similarity: score + sharpe + drawdown similarity
                score_sim = 1.0 - abs(a["score"] - b["score"]) / max(100, 1)
                sharpe_sim = 1.0 - min(1.0, abs(a["sharpe"] - b["sharpe"]) / max(3, 1))
                dd_sim = 1.0 - min(1.0, abs(a["max_drawdown"] - b["max_drawdown"]) / max(0.5, 0.01))

                # Archetype similarity (same archetype → more correlated)
                archetype_sim = 1.0 if a["archetype"] == b["archetype"] else 0.3

                # Symbol overlap
                symbol_sim = 1.0 if a["symbol"] == b["symbol"] else 0.2

                combined = 0.25 * score_sim + 0.25 * sharpe_sim + 0.15 * dd_sim + 0.2 * archetype_sim + 0.15 * symbol_sim
                combined = max(-1.0, min(1.0, combined))
                matrix[i][j] = combined
                matrix[j][i] = combined
        return matrix

    def _compute_covariance_matrix(self, strategies: list[dict]) -> np.ndarray:
        """Approximate covariance from correlation and score-volatility."""
        n = len(strategies)
        corr = self._compute_correlation_matrix(strategies)

        # Volatility proxy: inverse of win_rate-adjusted score
        vols = []
        for s in strategies:
            wr = max(s["win_rate"], 0.01)
            vol = (100.0 - s["score"]) / (wr * 10.0)
            vols.append(max(vol, 0.5))
        vol_array = np.array(vols)

        return np.outer(vol_array, vol_array) * corr

    def _cluster_exposures(self, strategies: list[dict]) -> dict:
        """Group strategies by archetype and symbol for exposure clustering."""
        clusters = defaultdict(list)
        for s in strategies:
            key = f"{s['archetype']}|{s['symbol']}"
            clusters[key].append(s["id"])

        cluster_map = {}
        for key, members in clusters.items():
            archetype, symbol = key.split("|", 1)
            cluster_map[key] = {
                "archetype": archetype,
                "symbol": symbol,
                "n_strategies": len(members),
                "strategy_ids": members,
                "avg_score": np.mean([s["score"] for st in strategies if st["id"] in members]),
            }
        return cluster_map

    def _compute_efficiency_scores(self, strategies: list[dict]) -> list[dict]:
        """Capital efficiency: score per unit of drawdown risk."""
        scores = []
        for s in strategies:
            dd = max(s["max_drawdown"], 0.01)
            efficiency = s["score"] / dd
            scores.append({
                "strategy_id": s["id"],
                "strategy_name": s["name"],
                "efficiency": round(float(efficiency), 4),
                "score": round(s["score"], 2),
                "max_drawdown": round(s["max_drawdown"], 4),
            })
        scores.sort(key=lambda x: -x["efficiency"])
        return scores

    def _optimize_allocations(self, strategies: list[dict], corr_matrix: np.ndarray, cov_matrix: np.ndarray) -> list[dict]:
        """Mean-variance optimization with constraints:
        - No short selling (weights ≥ 0)
        - Max 30% per strategy
        - Sum to 1.0
        """
        n = len(strategies)
        scores = np.array([s["score"] for s in strategies])

        # Simple risk-parity inspired: weight proportional to score / sum(score * corr_row)
        risk_contrib = np.zeros(n)
        for i in range(n):
            risk_contrib[i] = scores[i] / (np.sum(corr_matrix[i] * scores) + 1e-10)

        if np.sum(risk_contrib) > 0:
            raw_weights = risk_contrib / np.sum(risk_contrib)
        else:
            raw_weights = np.ones(n) / n

        # Apply constraints: max 30% per strategy
        weights = np.minimum(raw_weights, 0.30)
        weights = weights / np.sum(weights)

        allocations = []
        for i, s in enumerate(strategies):
            allocations.append({
                "strategy_id": s["id"],
                "strategy_name": s["name"],
                "weight": round(float(weights[i]), 4),
                "score": round(s["score"], 2),
                "sharpe": round(s["sharpe"], 2),
            })
        allocations.sort(key=lambda x: -x["weight"])
        return allocations

    async def _compute_regime_conditioned_weights(self, strategies: list[dict], corr_matrix: np.ndarray) -> dict:
        """Compute regime-conditioned weights using current scout intelligence."""
        regime_weights = {}
        try:
            if self.db:
                scout = await self.db.get_latest_scout_intelligence()
                regime = scout.get("regime", {})
                vol_regime = regime.get("volatility", "normal")
                liq_regime = regime.get("liquidity", "normal")

                vol_mult = 1.5 if vol_regime in ("high", "extreme") else 1.0
                liq_mult = 1.3 if liq_regime in ("low", "thin") else 1.0

                for i, s in enumerate(strategies):
                    score = s["score"]
                    sharp_score = s["sharpe"] * 10

                    # Vol-regime adjustment: favor strategies with lower drawdown in high vol
                    if vol_regime in ("high", "extreme"):
                        dd_penalty = max(0.5, 1.0 - s["max_drawdown"])
                        score = score * dd_penalty

                    # Liquidity-regime adjustment
                    wr_bonus = 1.0 + (s["win_rate"] - 0.5) * liq_mult
                    score = score * wr_bonus

                    regime_weights[s["id"]] = {
                        "raw_score": round(s["score"], 2),
                        "adjusted_score": round(float(score), 2),
                        "vol_regime_adjustment": round(vol_mult, 2),
                        "liq_regime_adjustment": round(liq_mult, 2),
                    }
        except Exception as e:
            logger.warning(f"{self.name}: regime-conditioned weights failed: {e}")

        return regime_weights

    def _compute_ensemble_survivability(self, strategies: list[dict], corr_matrix: np.ndarray, allocations: list[dict]) -> float:
        """Score how well the ensemble would survive adverse conditions."""
        scores = np.array([s["score"] for s in strategies])
        n = len(strategies)
        if n < 2:
            return 0.0

        # Weight allocation impact
        alloc_map = {a["strategy_id"]: a["weight"] for a in allocations}
        weights = np.array([alloc_map.get(s["id"], 0) for s in strategies])

        if np.sum(weights) == 0:
            return 0.0

        weights = weights / np.sum(weights)

        # Portfolio score = weighted average
        portfolio_score = float(np.dot(weights, scores))

        # Correlation penalty: high average correlation reduces survivability
        upper_tri = corr_matrix[np.triu_indices(n, k=1)]
        avg_corr = float(np.mean(upper_tri)) if len(upper_tri) > 0 else 0
        corr_penalty = 1.0 - min(0.5, max(0, avg_corr - 0.3))

        # Diversification benefit
        n_clusters = len(set(
            (s["archetype"], s["symbol"]) for s in strategies
        ))
        cluster_benefit = min(1.0, n_clusters / 5.0)

        survivability = portfolio_score * corr_penalty * (0.7 + 0.3 * cluster_benefit)
        return max(0.0, min(100.0, survivability))

    def _compute_concentration_risk(self, strategies: list[dict], allocations: list[dict]) -> float:
        """Herfindahl-Hirschman Index of allocation concentration."""
        weights = [a["weight"] for a in allocations]
        if not weights:
            return 0.0
        hhi = sum(w ** 2 for w in weights)
        # Normalize: HHI of 1 = fully concentrated, approaching 0 = diversified
        n = len(weights)
        min_hhi = 1.0 / n if n > 0 else 0
        normalized = (hhi - min_hhi) / (1.0 - min_hhi) if (1.0 - min_hhi) > 0 else 1.0
        return max(0.0, min(1.0, normalized))

    def _compute_diversification_score(self, corr_matrix: np.ndarray, allocations: list[dict]) -> float:
        """Diversification score: 1 - effective number of bets / N."""
        n = len(allocations)
        if n < 2:
            return 0.0

        upper_tri = corr_matrix[np.triu_indices(n, k=1)]
        avg_corr = float(np.mean(upper_tri)) if len(upper_tri) > 0 else 0

        weights = np.array([a["weight"] for a in allocations])
        if np.sum(weights) == 0:
            return 0.0
        weights = weights / np.sum(weights)

        # Effective N = 1 / sum(weights^2)
        effective_n = 1.0 / np.sum(weights ** 2) if np.sum(weights ** 2) > 0 else 1

        # Score: how close effective diversification is to N, adjusted by correlation
        raw = min(1.0, effective_n / n)
        corr_penalty = 1.0 - min(0.5, max(0, avg_corr - 0.2))
        return max(0.0, raw * corr_penalty)

    async def _persist_intelligence(self, intelligence: dict) -> None:
        """Persist portfolio intelligence to portfolio_intelligence table."""
        if not self.db:
            return
        try:
            await self.db._execute_insert(
                """
                INSERT INTO portfolio_intelligence
                    (id, computed_at, n_strategies, strategy_ids,
                     correlation_matrix, covariance_matrix, cluster_map,
                     efficiency_scores, optimal_allocations,
                     regime_conditioned_weights,
                     ensemble_survivability_score,
                     concentration_risk, diversification_score,
                     metadata)
                VALUES
                    (:id, :computed_at, :n_strategies, :strategy_ids,
                     :correlation_matrix, :covariance_matrix, :cluster_map,
                     :efficiency_scores, :optimal_allocations,
                     :regime_conditioned_weights,
                     :ensemble_survivability_score,
                     :concentration_risk, :diversification_score,
                     :metadata)
                """,
                {
                    "id": intelligence["id"],
                    "computed_at": intelligence["computed_at"],
                    "n_strategies": intelligence["n_strategies"],
                    "strategy_ids": json.dumps(intelligence["strategies"]),
                    "correlation_matrix": json.dumps(intelligence["correlation_matrix"]),
                    "covariance_matrix": json.dumps(intelligence["covariance_matrix"]),
                    "cluster_map": json.dumps(intelligence["cluster_map"]),
                    "efficiency_scores": json.dumps(intelligence["efficiency_scores"]),
                    "optimal_allocations": json.dumps(intelligence["optimal_allocations"]),
                    "regime_conditioned_weights": json.dumps(intelligence["regime_conditioned_weights"]),
                    "ensemble_survivability_score": intelligence["ensemble_survivability_score"],
                    "concentration_risk": intelligence["concentration_risk"],
                    "diversification_score": intelligence["diversification_score"],
                    "metadata": json.dumps(intelligence["metadata"]),
                },
            )
        except Exception as e:
            logger.warning(f"{self.name}: persist failed: {e}")

    async def _publish_intelligence(self, intelligence: dict) -> None:
        """Publish to Redis for downstream consumers."""
        if not self._redis:
            return
        try:
            signal = {
                "type": "portfolio_intelligence",
                "computed_at": intelligence["computed_at"],
                "n_strategies": intelligence["n_strategies"],
                "ensemble_survivability_score": intelligence["ensemble_survivability_score"],
                "concentration_risk": intelligence["concentration_risk"],
                "diversification_score": intelligence["diversification_score"],
                "top_allocations": intelligence["optimal_allocations"][:5],
            }
            await self._redis.publish("portfolio_intelligence_updates", json.dumps(signal))
        except Exception as e:
            logger.warning(f"{self.name}: publish failed: {e}")

    async def get_portfolio_snapshot(self) -> dict:
        """Public method for downstream consumers (CapitalAllocator, EnsembleExecution)."""
        return self._latest_intelligence
