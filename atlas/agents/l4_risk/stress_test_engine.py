"""
stress_test_engine.py — L4 Risk Agent for historical scenario stress testing.

Scenarios:
- 2008 financial crisis
- COVID crash
- Flash crash
- Liquidity vacuum
- Exchange outage
- Volatility explosion
- Overnight gap

Outputs:
- survival_probability per scenario
- capital_drawdown estimates
- recovery_duration estimates
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent


# Predefined stress scenarios
STRESS_SCENARIOS = {
    "2008_crisis": {
        "label": "2008 Financial Crisis",
        "equity_drawdown": -0.54,
        "vol_multiplier": 3.5,
        "correlation_spike": 0.85,
        "liquidity_drop": 0.6,
        "recovery_days": 580,
    },
    "covid_crash": {
        "label": "COVID-19 Crash (2020)",
        "equity_drawdown": -0.34,
        "vol_multiplier": 4.0,
        "correlation_spike": 0.80,
        "liquidity_drop": 0.5,
        "recovery_days": 130,
    },
    "flash_crash": {
        "label": "Flash Crash (2010)",
        "equity_drawdown": -0.09,
        "vol_multiplier": 6.0,
        "correlation_spike": 0.90,
        "liquidity_drop": 0.8,
        "recovery_days": 1,
    },
    "liquidity_vacuum": {
        "label": "Liquidity Vacuum",
        "equity_drawdown": -0.15,
        "vol_multiplier": 3.0,
        "correlation_spike": 0.70,
        "liquidity_drop": 0.9,
        "recovery_days": 10,
    },
    "exchange_outage": {
        "label": "Exchange Outage",
        "equity_drawdown": -0.05,
        "vol_multiplier": 2.0,
        "correlation_spike": 0.60,
        "liquidity_drop": 1.0,
        "recovery_days": 1,
    },
    "volatility_explosion": {
        "label": "Volatility Explosion",
        "equity_drawdown": -0.20,
        "vol_multiplier": 5.0,
        "correlation_spike": 0.75,
        "liquidity_drop": 0.4,
        "recovery_days": 30,
    },
    "overnight_gap": {
        "label": "Overnight Gap",
        "equity_drawdown": -0.25,
        "vol_multiplier": 3.0,
        "correlation_spike": 0.65,
        "liquidity_drop": 0.3,
        "recovery_days": 15,
    },
}


class StressTestEngine(BaseAgent):
    """
    L4 Risk Agent — Historical scenario stress testing for portfolio resilience.
    """

    name = "StressTestEngine"
    agent_type = "stress_test"
    layer = "L4"

    def __init__(self, redis_client, db_client):
        super().__init__(self.name, self.agent_type, self.layer, redis_client)
        self.db = db_client
        self._run_interval = 3600  # Every hour

    async def run(self):
        logger.info(f"{self.name}: Starting stress test engine")

        while self.status == "running":
            try:
                await self._run_all_scenarios()
            except Exception as e:
                logger.error(f"{self.name}: Stress test error: {e}")

            for _ in range(self._run_interval // 10):
                await self._sleep(10)
                if self.status != "running":
                    return

    async def _sleep(self, seconds: int):
        import asyncio
        await asyncio.sleep(seconds)

    async def _run_all_scenarios(self):
        """Run all predefined stress scenarios against current portfolio."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT strategy_id, symbol, qty, avg_price, side
                    FROM positions WHERE qty != 0
                """)
            )
            positions = [
                {
                    "strategy_id": str(row[0]),
                    "symbol": str(row[1]),
                    "qty": float(row[2]),
                    "avg_price": float(row[3]) if row[3] else 0,
                    "side": str(row[4]),
                }
                for row in r.fetchall()
            ]

        if not positions:
            logger.info(f"{self.name}: No positions to stress test")
            return

        results = []
        for scenario_key, scenario in STRESS_SCENARIOS.items():
            result = self._apply_scenario(positions, scenario_key, scenario)
            results.append(result)

        # Find worst scenario
        worst = min(results, key=lambda r: r["survival_probability"])

        # Persist results
        await self.db._execute_insert(
            """
            INSERT INTO stress_test_results
                (id, tested_at, n_scenarios, n_positions,
                 worst_scenario, min_survival_probability,
                 max_drawdown, avg_recovery_days, scenario_results)
            VALUES
                (:id, NOW(), :n_scenarios, :n_positions,
                 :worst_scenario, :min_prob, :max_dd,
                 :avg_recovery, CAST(:results AS jsonb))
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "n_scenarios": len(results),
                "n_positions": len(positions),
                "worst_scenario": worst["scenario"],
                "min_prob": worst["survival_probability"],
                "max_dd": worst["estimated_drawdown"],
                "avg_recovery": (
                    sum(r["estimated_recovery_days"] for r in results) / len(results)
                ),
                "results": json.dumps(results),
            },
        )

        logger.info(
            f"{self.name}: Stress tests complete — "
            f"{len(results)} scenarios, worst={worst['scenario']} "
            f"(survival={worst['survival_probability']:.1%})"
        )

    def _apply_scenario(
        self,
        positions: list[dict],
        scenario_key: str,
        scenario: dict,
    ) -> dict:
        """
        Apply stress scenario to positions and estimate impact.
        """
        total_value = sum(abs(p["qty"] * p["avg_price"]) for p in positions)

        # Estimate drawdown based on position exposure
        equity_exposure = sum(
            abs(p["qty"] * p["avg_price"])
            for p in positions
        )
        if total_value > 0:
            equity_weight = equity_exposure / total_value
        else:
            equity_weight = 1.0

        estimated_drawdown = scenario["equity_drawdown"] * equity_weight
        survival_probability = max(0.0, 1.0 - abs(estimated_drawdown) * 1.5)
        capital_at_risk = abs(total_value * estimated_drawdown)

        return {
            "scenario": scenario_key,
            "label": scenario["label"],
            "estimated_drawdown": round(estimated_drawdown, 4),
            "survival_probability": round(survival_probability, 4),
            "capital_at_risk": round(capital_at_risk, 2),
            "estimated_recovery_days": scenario["recovery_days"],
            "correlation_spike": scenario["correlation_spike"],
            "liquidity_drop": scenario["liquidity_drop"],
        }
