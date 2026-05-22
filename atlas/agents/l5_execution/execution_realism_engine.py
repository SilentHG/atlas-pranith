"""execution_realism_engine.py — Phase 12: Institutional Execution Realism.

Simulates realistic market-microstructure effects:
  - Order book depth simulation
  - Partial fills with queue position
  - Spread widening under liquidity stress
  - Liquidity exhaustion events
  - Market impact curves (Almgren-Chriss style)
  - Latency modeling (network + exchange)

Outputs:
  - realistic_fill_estimates: {fill_probability, expected_slippage, expected_partial_pct}
  - impact_adjusted_returns: original_return - impact_cost
  - execution_degradation_score: 0 (no degradation) to 1 (complete failure)

Consumes:
  - Scout intelligence (liquidity regime, spread, slippage risk)
  - Backtest trades (strategy trade history)
"""

import asyncio
import json
import math
import uuid
from datetime import datetime
from typing import Any, Optional

import numpy as np
from loguru import logger

from atlas.core.agent_base import BaseAgent


class ExecutionRealismEngine(BaseAgent):
    """Institutional execution realism — microstructural simulation."""

    name = "ExecutionRealismEngine"
    agent_type = "execution_realism"
    layer = "L5"

    def __init__(self, redis_client=None, db_client=None, run_interval: int = 900):
        super().__init__(
            name=self.name,
            agent_type=self.agent_type,
            layer=self.layer,
            redis_client=redis_client,
        )
        self.db = db_client
        self.run_interval = run_interval

        # Market impact parameters (Almgren-Chriss inspired)
        self.PERMANENT_IMPACT_COEFF = 0.1   # bps per % of ADV
        self.TEMPORARY_IMPACT_COEFF = 0.5   # bps per % of ADV
        self.ADV_DECAY_HALF_LIFE = 60       # minutes

        # Latency model
        self.NETWORK_LATENCY_MS = 10        # 10ms base network latency
        self.EXCHANGE_LATENCY_MS = 5        # 5ms exchange processing
        self.LATENCY_STD_MS = 3             # 3ms jitter

        self._latest_simulation: dict = {}

    async def run(self):
        logger.info(f"{self.name}: starting execution realism engine (every {self.run_interval}s)")
        while self.status == "running":
            try:
                simulation = await self._run_simulation()
                if simulation:
                    await self._persist_simulation(simulation)
                    self._latest_simulation = simulation
                    await self._publish_simulation(simulation)
            except Exception as e:
                logger.error(f"{self.name}: cycle failed: {e}")
            await asyncio.sleep(self.run_interval)

    async def _fetch_strategy_trades(self) -> list[dict]:
        """Fetch recent paper/backtest trades for simulation."""
        if not self.db:
            return []
        try:
            async with self.db.engine.connect() as conn:
                from sqlalchemy.sql import text
                # Get paper trades
                result = await conn.execute(
                    text("""
                        SELECT id, strategy_id, symbol, side, quantity, fill_price, time, status
                        FROM paper_trades
                        ORDER BY time DESC
                        LIMIT 200
                    """)
                )
                rows = result.fetchall()
                out = []
                for r in rows:
                    out.append({
                        "id": str(r[0]),
                        "strategy_id": str(r[1]) if r[1] else "",
                        "symbol": r[2],
                        "side": r[3],
                        "quantity": float(r[4]) if r[4] else 0,
                        "fill_price": float(r[5]) if r[5] else 0,
                        "time": r[6],
                        "status": r[7],
                    })
                return out
        except Exception as e:
            logger.warning(f"{self.name}: fetch trades failed: {e}")
            return []

    async def _fetch_scout_intelligence(self) -> dict:
        """Fetch current liquidity and execution scout data."""
        try:
            if self.db:
                return await self.db.get_latest_scout_intelligence()
        except Exception:
            pass
        return {}

    async def _run_simulation(self) -> Optional[dict]:
        """Run execution realism simulation on recent trades."""
        trades = await self._fetch_strategy_trades()
        scout = await self._fetch_scout_intelligence()

        if len(trades) < 3:
            logger.info(f"{self.name}: need ≥3 trades for simulation (got {len(trades)})")
            return None

        # Extract liquidity parameters
        liq = scout.get("liquidity", {})
        liq_score = liq.get("score", 0.8)
        spread_bps = liq.get("spread_bps", 10.0)
        slippage_risk = liq.get("risk", 0.2)

        exec_data = scout.get("execution", {})
        exec_quality = exec_data.get("fill_score", 0.9)

        # Simulate fills for each trade
        simulated_fills = []
        for trade in trades:
            fill_result = self._simulate_fill(
                trade, liq_score, spread_bps, slippage_risk, exec_quality
            )
            simulated_fills.append(fill_result)

        # Compute aggregate statistics
        avg_fill_prob = float(np.mean([f["fill_probability"] for f in simulated_fills]))
        avg_expected_slippage = float(np.mean([f["expected_slippage_bps"] for f in simulated_fills]))
        avg_partial_pct = float(np.mean([f["expected_partial_pct"] for f in simulated_fills]))
        avg_latency_ms = float(np.mean([f["simulated_latency_ms"] for f in simulated_fills]))
        avg_market_impact_bps = float(np.mean([f["market_impact_bps"] for f in simulated_fills]))

        # Liquidity exhaustion scenario
        exhaustion_sim = self._simulate_liquidity_exhaustion(
            trades, liq_score, spread_bps
        )

        # Execution degradation score
        degradation = self._compute_degradation_score(
            avg_fill_prob, avg_partial_pct, exhaustion_sim, liq_score
        )

        simulation = {
            "id": str(uuid.uuid4()),
            "simulated_at": datetime.utcnow().isoformat(),
            "n_trades_simulated": len(simulated_fills),
            "avg_fill_probability": round(float(avg_fill_prob), 4),
            "avg_expected_slippage_bps": round(float(avg_expected_slippage), 4),
            "avg_expected_partial_pct": round(float(avg_partial_pct), 4),
            "avg_simulated_latency_ms": round(float(avg_latency_ms), 2),
            "avg_market_impact_bps": round(float(avg_market_impact_bps), 4),
            "exhaustion_scenario": {
                "exhaustion_probability": round(float(exhaustion_sim["probability"]), 4),
                "expected_exhaustion_slippage_bps": round(float(exhaustion_sim["slippage_bps"]), 2),
                "fill_collapse_pct": round(float(exhaustion_sim["fill_collapse_pct"]), 2),
            },
            "execution_degradation_score": round(float(degradation), 4),
            "liquidity_state_at_simulation": {
                "liq_score": round(liq_score, 4),
                "spread_bps": round(spread_bps, 2),
                "slippage_risk": round(slippage_risk, 4),
                "fill_quality": round(exec_quality, 4),
            },
            "simulated_fills": simulated_fills[:20],  # Keep response size manageable
        }
        return simulation

    def _simulate_fill(self, trade: dict, liq_score: float, spread_bps: float,
                       slippage_risk: float, exec_quality: float) -> dict:
        """Simulate a realistic fill with market microstructure effects."""

        # 1. Base fill probability from liquidity regime
        base_fill_prob = 0.95 * liq_score * exec_quality

        # 2. Queue position: random uniform [0, 1]
        queue_position = float(np.random.uniform(0, 1))

        # 3. Spread widening: wider spread = more slippage
        base_spread_bps = spread_bps
        widened_spread = base_spread_bps * (1.0 + slippage_risk * np.random.exponential(0.5))

        # 4. Partial fill probability
        if queue_position < 0.3:
            # Good queue position — likely full fill
            partial_pct = float(np.random.uniform(0.8, 1.0))
            fill_prob = min(1.0, base_fill_prob * 1.2)
        elif queue_position < 0.7:
            # Medium queue position — possible partial fill
            partial_pct = float(np.random.uniform(0.4, 0.9))
            fill_prob = base_fill_prob
        else:
            # Bad queue position — high chance of partial or no fill
            partial_pct = float(np.random.uniform(0.1, 0.6))
            fill_prob = base_fill_prob * 0.5

        # 5. Market impact (Almgren-Chriss simplified)
        trade_value = trade.get("quantity", 1) * trade.get("fill_price", 100)
        adv_estimate = 1_000_000  # Assume $1M daily volume
        participation_rate = trade_value / (adv_estimate + 1)
        perm_impact = self.PERMANENT_IMPACT_COEFF * participation_rate
        temp_impact = self.TEMPORARY_IMPACT_COEFF * participation_rate * 0.5
        total_impact = perm_impact + temp_impact

        # 6. Latency simulation
        network_jitter = float(np.random.normal(0, self.LATENCY_STD_MS))
        total_latency = self.NETWORK_LATENCY_MS + self.EXCHANGE_LATENCY_MS + network_jitter

        # 7. Expected slippage
        expected_slippage = widened_spread * 0.5 + total_impact

        # Fill value
        fill_prob = max(0.0, min(1.0, fill_prob))

        return {
            "trade_id": str(trade.get("id", "")),
            "strategy_id": trade.get("strategy_id", ""),
            "symbol": trade.get("symbol", ""),
            "side": trade.get("side", ""),
            "quantity": trade.get("quantity", 0),
            "fill_probability": round(fill_prob, 4),
            "expected_slippage_bps": round(float(expected_slippage), 4),
            "expected_partial_pct": round(float(partial_pct), 4),
            "simulated_latency_ms": round(float(total_latency), 2),
            "market_impact_bps": round(float(total_impact), 4),
            "queue_position": round(queue_position, 4),
        }

    def _simulate_liquidity_exhaustion(self, trades: list[dict], liq_score: float, spread_bps: float) -> dict:
        """Simulate what happens during liquidity exhaustion events."""
        # Base exhaustion probability
        base_exhaustion_prob = 0.05 * (1.0 - liq_score) * (spread_bps / 10.0)
        exhaustion_prob = min(0.5, max(0.01, base_exhaustion_prob))

        # During exhaustion: spreads widen 5-20x, fill rates collapse
        exhaustion_spread = spread_bps * np.random.uniform(5, 20)
        exhaustion_slippage = exhaustion_spread * 0.5 + 20  # additional impact
        fill_collapse = np.random.uniform(0.5, 0.9)  # 50-90% fill collapse

        return {
            "probability": float(exhaustion_prob),
            "slippage_bps": float(exhaustion_slippage),
            "fill_collapse_pct": float(fill_collapse * 100),
        }

    def _compute_degradation_score(self, avg_fill_prob: float, avg_partial_pct: float,
                                    exhaustion: dict, liq_score: float) -> float:
        """Composite degradation score: 0 = perfect, 1 = completely degraded."""
        fill_degradation = 1.0 - (avg_fill_prob * avg_partial_pct)
        exhaustion_risk = exhaustion["probability"]
        liq_degradation = 1.0 - liq_score

        raw = 0.5 * fill_degradation + 0.3 * exhaustion_risk + 0.2 * liq_degradation
        return max(0.0, min(1.0, raw))

    async def _persist_simulation(self, simulation: dict) -> None:
        """Persist execution realism results."""
        if not self.db:
            return
        try:
            await self.db._execute_insert(
                """
                INSERT INTO execution_realism
                    (id, simulated_at, n_trades_simulated,
                     avg_fill_probability, avg_expected_slippage_bps,
                     avg_expected_partial_pct, avg_simulated_latency_ms,
                     avg_market_impact_bps, exhaustion_scenario,
                     execution_degradation_score, liquidity_state,
                     simulated_fills, metadata)
                VALUES
                    (:id, :simulated_at, :n_trades_simulated,
                     :avg_fill_probability, :avg_expected_slippage_bps,
                     :avg_expected_partial_pct, :avg_simulated_latency_ms,
                     :avg_market_impact_bps, :exhaustion_scenario,
                     :execution_degradation_score, :liquidity_state,
                     :simulated_fills, :metadata)
                """,
                {
                    "id": simulation["id"],
                    "simulated_at": simulation["simulated_at"],
                    "n_trades_simulated": simulation["n_trades_simulated"],
                    "avg_fill_probability": simulation["avg_fill_probability"],
                    "avg_expected_slippage_bps": simulation["avg_expected_slippage_bps"],
                    "avg_expected_partial_pct": simulation["avg_expected_partial_pct"],
                    "avg_simulated_latency_ms": simulation["avg_simulated_latency_ms"],
                    "avg_market_impact_bps": simulation["avg_market_impact_bps"],
                    "exhaustion_scenario": json.dumps(simulation["exhaustion_scenario"]),
                    "execution_degradation_score": simulation["execution_degradation_score"],
                    "liquidity_state": json.dumps(simulation["liquidity_state_at_simulation"]),
                    "simulated_fills": json.dumps(simulation["simulated_fills"]),
                    "metadata": json.dumps({"method": "almgren_chriss_microstructure"}),
                },
            )
        except Exception as e:
            logger.warning(f"{self.name}: persist failed: {e}")

    async def _publish_simulation(self, simulation: dict) -> None:
        """Publish execution realism update."""
        if not self._redis:
            return
        try:
            signal = {
                "type": "execution_realism",
                "simulated_at": simulation["simulated_at"],
                "n_trades": simulation["n_trades_simulated"],
                "avg_fill_prob": simulation["avg_fill_probability"],
                "avg_slippage_bps": simulation["avg_expected_slippage_bps"],
                "execution_degradation": simulation["execution_degradation_score"],
            }
            await self._redis.publish("execution_realism_updates", json.dumps(signal))
        except Exception as e:
            logger.warning(f"{self.name}: publish failed: {e}")

    async def estimate_fill_quality(self, symbol: str, quantity: float, price: float) -> dict:
        """Public method: estimate realistic fill quality for a given trade."""
        scout = await self._fetch_scout_intelligence()
        liq = scout.get("liquidity", {})
        liq_score = liq.get("score", 0.8)
        spread_bps = liq.get("spread_bps", 10.0)
        slippage_risk = liq.get("risk", 0.2)
        exec_data = scout.get("execution", {})
        exec_quality = exec_data.get("fill_score", 0.9)

        dummy_trade = {"quantity": quantity, "fill_price": price}
        result = self._simulate_fill(dummy_trade, liq_score, spread_bps, slippage_risk, exec_quality)
        return {
            "fill_probability": result["fill_probability"],
            "expected_slippage_bps": result["expected_slippage_bps"],
            "expected_partial_pct": result["expected_partial_pct"],
            "market_impact_bps": result["market_impact_bps"],
            "execution_degradation": self._compute_degradation_score(
                result["fill_probability"], result["expected_partial_pct"],
                self._simulate_liquidity_exhaustion([dummy_trade], liq_score, spread_bps),
                liq_score,
            ),
        }

    async def get_execution_realism_snapshot(self) -> dict:
        """Public method for downstream consumers."""
        return self._latest_simulation
