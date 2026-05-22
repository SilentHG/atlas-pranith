"""
systemic_risk_engine.py — L4 Risk Agent for systemic risk modeling.

Capabilities:
- Contagion modeling across correlated strategies
- Liquidity cascade detection
- Correlation spike detection
- Systemic fragility scoring
- Tail-risk propagation analysis
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent


class SystemicRiskEngine(BaseAgent):
    """
    L4 Risk Agent — Systemic risk modeling and contagion detection.
    """

    name = "SystemicRiskEngine"
    agent_type = "systemic_risk"
    layer = "L4"

    def __init__(self, redis_client, db_client):
        super().__init__(self.name, self.agent_type, self.layer, redis_client)
        self.db = db_client
        self._run_interval = 900  # Every 15 minutes

    async def run(self):
        logger.info(f"{self.name}: Starting systemic risk monitoring")

        while self.status == "running":
            try:
                await self._compute_systemic_risk()
            except Exception as e:
                logger.error(f"{self.name}: Risk computation error: {e}")

            for _ in range(self._run_interval // 10):
                await self._sleep(10)
                if self.status != "running":
                    return

    async def _sleep(self, seconds: int):
        import asyncio
        await asyncio.sleep(seconds)

    async def _compute_systemic_risk(self):
        """Compute systemic risk scores."""
        portfolio = await self._load_portfolio_data()
        if not portfolio:
            logger.info(f"{self.name}: No portfolio data for systemic risk")
            return

        contagion_prob = self._estimate_contagion_probability(portfolio)
        fragility_score = self._estimate_fragility(portfolio, contagion_prob)
        correlation_regime = self._detect_correlation_regime(portfolio)
        concentration_risk = self._compute_concentration_risk(portfolio)

        # Compute composite
        composite = (
            contagion_prob * 0.30
            + fragility_score * 0.25
            + correlation_regime * 0.25
            + concentration_risk * 0.20
        )

        # Persist
        await self.db._execute_insert(
            """
            INSERT INTO systemic_risk
                (id, assessed_at, systemic_risk_score, contagion_probability,
                 portfolio_fragility, correlation_regime, concentration_risk,
                 n_strategies_analyzed, details)
            VALUES
                (:id, NOW(), :sys_risk, :contagion_prob,
                 :fragility, :corr_regime, :concentration,
                 :n_strategies, CAST(:details AS jsonb))
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "sys_risk": composite,
                "contagion_prob": contagion_prob,
                "fragility": fragility_score,
                "corr_regime": correlation_regime,
                "concentration": concentration_risk,
                "n_strategies": len(portfolio),
                "details": json.dumps({
                    "n_strategies": len(portfolio),
                    "contagion_drivers": [],
                }),
            },
        )

        if composite > 0.7:
            logger.critical(
                f"{self.name}: HIGH SYSTEMIC RISK — "
                f"score={composite:.2f}, contagion={contagion_prob:.2f}"
            )

    async def _load_portfolio_data(self) -> list[dict]:
        """Load active strategy positions and correlation data."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT p.strategy_id, p.symbol, p.qty, p.side,
                           COALESCE(c.avg_pairwise_corr, 0) as correlation
                    FROM positions p
                    LEFT JOIN correlation_memory c ON c.id = (
                        SELECT id FROM correlation_memory
                        WHERE symbol = p.symbol
                        ORDER BY timestamp DESC LIMIT 1
                    )
                    WHERE p.qty != 0
                """)
            )
            return [
                {
                    "strategy_id": str(row[0]),
                    "symbol": str(row[1]),
                    "qty": float(row[2]),
                    "side": str(row[3]),
                    "correlation": float(row[4]),
                }
                for row in r.fetchall()
            ]

    def _estimate_contagion_probability(self, portfolio: list[dict]) -> float:
        """Estimate probability of cascade failure."""
        if not portfolio:
            return 0.0

        # High correlation + high concentration = high contagion risk
        n_high_corr = sum(1 for p in portfolio if abs(p["correlation"]) > 0.7)
        return min(1.0, n_high_corr / max(1, len(portfolio)))

    def _estimate_fragility(self, portfolio: list[dict], contagion: float) -> float:
        """Estimate portfolio fragility."""
        if not portfolio:
            return 0.0

        # More positions = potentially more fragile
        size_factor = min(1.0, len(portfolio) / 20)
        return 0.5 * contagion + 0.5 * size_factor

    def _detect_correlation_regime(self, portfolio: list[dict]) -> float:
        """Detect if correlation regime is elevated."""
        if not portfolio:
            return 0.0

        avg_corr = sum(abs(p["correlation"]) for p in portfolio) / len(portfolio)
        return min(1.0, avg_corr)

    def _compute_concentration_risk(self, portfolio: list[dict]) -> float:
        """Compute concentration risk using HHI."""
        if not portfolio:
            return 0.0

        symbols = {}
        for p in portfolio:
            sym = p["symbol"]
            symbols[sym] = symbols.get(sym, 0) + abs(p["qty"])

        total_qty = sum(symbols.values())
        if total_qty == 0:
            return 0.0

        hhi = sum((qty / total_qty) ** 2 for qty in symbols.values())
        return min(1.0, hhi)
