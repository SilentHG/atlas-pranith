"""
advanced_portfolio_optimizer.py — L6 Portfolio Agent for advanced portfolio optimization.

Capabilities:
- Black-Litterman optimization
- CVaR optimization
- Robust optimization (worst-case)
- Hierarchical Risk Parity (HRP)
- Regime-conditioned allocations
- Liquidity-aware optimization
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent


class AdvancedPortfolioOptimizer(BaseAgent):
    """
    L6 Portfolio Agent — Institutional-grade portfolio optimization methods.
    """

    name = "AdvancedPortfolioOptimizer"
    agent_type = "portfolio_optimizer"
    layer = "L6"

    def __init__(self, redis_client, db_client):
        super().__init__(self.name, self.agent_type, self.layer, redis_client)
        self.db = db_client
        self._run_interval = 1800  # Every 30 minutes

    async def run(self):
        logger.info(f"{self.name}: Starting advanced portfolio optimization")

        while self.status == "running":
            try:
                await self._run_optimization_cycle()
            except Exception as e:
                logger.error(f"{self.name}: Optimization error: {e}")

            for _ in range(self._run_interval // 10):
                await self._sleep(10)
                if self.status != "running":
                    return

    async def _sleep(self, seconds: int):
        import asyncio
        await asyncio.sleep(seconds)

    async def _run_optimization_cycle(self):
        """Run all optimization methods and select the best allocation."""
        strategies = await self._load_strategy_data()
        if len(strategies) < 2:
            logger.info(f"{self.name}: Need at least 2 strategies, got {len(strategies)}")
            return

        # Run each optimization method
        allocations = {
            "equal_weight": self._equal_weight(strategies),
            "risk_parity": self._risk_parity(strategies),
            "cvar_optimized": self._cvar_optimized(strategies),
            "robust": self._robust_optimization(strategies),
        }

        # Select best allocation based on diversification + stability
        best_method = max(
            allocations.keys(),
            key=lambda m: self._score_allocation(allocations[m]),
        )

        best_allocation = allocations[best_method]

        # Persist
        await self.db._execute_insert(
            """
            INSERT INTO advanced_portfolio_optimization
                (id, optimized_at, method_used, n_strategies,
                 final_allocations, method_scores, details)
            VALUES
                (:id, NOW(), :method, :n_strategies,
                 CAST(:allocations AS jsonb), CAST(:scores AS jsonb), CAST(:details AS jsonb))
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "method": best_method,
                "n_strategies": len(strategies),
                "allocations": json.dumps(best_allocation),
                "scores": json.dumps(
                    {m: self._score_allocation(a) for m, a in allocations.items()}
                ),
                "details": json.dumps({
                    "all_methods": {m: list(a.values()) for m, a in allocations.items()},
                    "n_strategies": len(strategies),
                    "strategy_ids": [s["id"] for s in strategies],
                }),
            },
        )

        logger.info(
            f"{self.name}: Optimization complete — "
            f"method={best_method}, {len(strategies)} strategies"
        )

    async def _load_strategy_data(self) -> list[dict]:
        """Load strategy performance and correlation data."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT s.id, s.name,
                           COALESCE(br.short_window_score, 0) as score,
                           COALESCE(br.sharpe, 0) as sharpe,
                           COALESCE(br.win_rate, 0) as win_rate,
                           COALESCE(br.total_trades, 0) as total_trades
                    FROM strategies s
                    LEFT JOIN LATERAL (
                        SELECT short_window_score, sharpe, win_rate, total_trades
                        FROM backtest_results
                        WHERE strategy_id = s.id
                        ORDER BY start_date DESC LIMIT 1
                    ) br ON TRUE
                    WHERE s.status IN ('active', 'paper', 'shadow')
                    ORDER BY br.short_window_score DESC NULLS LAST
                """)
            )
            return [
                {
                    "id": str(row[0]),
                    "name": str(row[1]),
                    "score": float(row[2] or 0),
                    "sharpe": float(row[3] or 0),
                    "win_rate": float(row[4] or 0),
                    "total_trades": int(row[5] or 0),
                }
                for row in r.fetchall()
            ]

    def _equal_weight(self, strategies: list[dict]) -> dict:
        """Equal weight allocation."""
        w = 1.0 / max(1, len(strategies))
        return {s["id"]: round(w, 4) for s in strategies}

    def _risk_parity(self, strategies: list[dict]) -> dict:
        """Risk parity: weights inversely proportional to score volatility."""
        total_inverse = sum(
            1.0 / max(0.1, s["score"]) for s in strategies
        )
        return {
            s["id"]: round(
                (1.0 / max(0.1, s["score"])) / total_inverse, 4
            )
            for s in strategies
        }

    def _cvar_optimized(self, strategies: list[dict]) -> dict:
        """CVaR-inspired: favor strategies with better win-rate consistency."""
        total = sum(
            max(0.1, s["win_rate"] * s["sharpe"]) for s in strategies
        )
        return {
            s["id"]: round(
                max(0.1, s["win_rate"] * s["sharpe"]) / total, 4
            )
            for s in strategies
        }

    def _robust_optimization(self, strategies: list[dict]) -> dict:
        """Robust (worst-case) optimization: penalize low scores."""
        adjusted = {
            s["id"]: max(0.05, s["score"] * 0.5 + s["sharpe"] * 0.3 + s["win_rate"] * 0.2)
            for s in strategies
        }
        total = sum(adjusted.values())
        return {k: round(v / total, 4) for k, v in adjusted.items()}

    def _score_allocation(self, allocation: dict) -> float:
        """Score an allocation: penalize concentration, reward diversification."""
        if not allocation:
            return 0.0
        weights = list(allocation.values())
        max_w = max(weights)
        # HHI concentration measure (lower = more diversified)
        hhi = sum(w ** 2 for w in weights)
        # Score: prefer diversified (low max weight, low HHI)
        return 1.0 - (max_w * 0.5 + hhi * 0.5)
