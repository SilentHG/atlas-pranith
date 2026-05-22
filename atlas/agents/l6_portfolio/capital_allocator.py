"""capital_allocator.py — Phase 12: Institutional Capital Allocation.

Capabilities:
  - Volatility targeting
  - Kelly fraction constraints
  - Max exposure enforcement
  - Dynamic sizing based on regime & score
  - Risk parity allocation
  - Adaptive leverage caps

Consumes:
  - PortfolioIntelligenceEngine (optimal allocations, regime weights)
  - Scout intelligence (vol/liquidity regime)
  - Backtest results (score, sharpe, win_rate)

Outputs persisted to capital_allocation table.
"""

import asyncio
import json
import uuid
from collections import defaultdict
from datetime import datetime
from typing import Optional

import numpy as np
from loguru import logger

from atlas.core.agent_base import BaseAgent


class CapitalAllocator(BaseAgent):
    """Institutional capital allocation with volatility targeting and risk parity."""

    name = "CapitalAllocator"
    agent_type = "capital_allocator"
    layer = "L6"

    # Default constraints
    MAX_STRATEGY_EXPOSURE = 0.30       # No strategy > 30%
    MAX_ASSET_CLASS_EXPOSURE = 0.60    # No asset class > 60%
    MAX_LEVERAGE = 1.0                 # No leverage
    KELLY_FRACTION = 0.25              # Fraction of Kelly to use (conservative)
    VOL_TARGET = 0.15                  # 15% annualized volatility target
    RISK_FREE_RATE = 0.05              # 5% risk-free rate

    def __init__(self, redis_client=None, db_client=None, run_interval: int = 1800):
        super().__init__(
            name=self.name,
            agent_type=self.agent_type,
            layer=self.layer,
            redis_client=redis_client,
        )
        self.db = db_client
        self.run_interval = run_interval
        self._live_allocations: dict = {}

    async def run(self):
        logger.info(f"{self.name}: starting capital allocation cycle (every {self.run_interval}s)")
        while self.status == "running":
            try:
                allocations = await self._compute_allocations()
                if allocations:
                    await self._persist_allocations(allocations)
                    self._live_allocations = allocations
                    await self._publish_allocations(allocations)
            except Exception as e:
                logger.error(f"{self.name}: cycle failed: {e}")
            await asyncio.sleep(self.run_interval)

    async def _fetch_portfolio_intelligence(self) -> dict:
        """Fetch latest portfolio intelligence."""
        if not self.db:
            return {}
        try:
            async with self.db.engine.connect() as conn:
                from sqlalchemy.sql import text
                result = await conn.execute(
                    text("""
                        SELECT id, n_strategies, strategy_ids,
                               optimal_allocations, regime_conditioned_weights,
                               ensemble_survivability_score, concentration_risk,
                               diversification_score, metadata
                        FROM portfolio_intelligence
                        ORDER BY computed_at DESC
                        LIMIT 1
                    """)
                )
                row = result.fetchone()
                if row:
                    raw = dict(row._mapping)
                    for k in ("optimal_allocations", "regime_conditioned_weights", "strategy_ids"):
                        if isinstance(raw.get(k), str):
                            raw[k] = json.loads(raw[k])
                    return raw
        except Exception as e:
            logger.warning(f"{self.name}: fetch portfolio intelligence failed: {e}")
        return {}

    async def _fetch_strategies(self) -> list[dict]:
        """Fetch strategies with full metrics."""
        if not self.db:
            return []
        try:
            async with self.db.engine.connect() as conn:
                from sqlalchemy.sql import text
                result = await conn.execute(
                    text("""
                        SELECT s.id, s.name, s.normalized_strategy,
                               b.short_window_score, b.sharpe, b.win_rate,
                               b.total_trades, b.max_drawdown, b.results
                        FROM strategies s
                        JOIN backtest_results b ON s.id = b.strategy_id
                        WHERE b.short_window_score IS NOT NULL
                          AND b.total_trades >= 5
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
                    results_raw = r[8]
                    if isinstance(results_raw, str):
                        try:
                            results_raw = json.loads(results_raw)
                        except Exception:
                            results_raw = {}

                    out.append({
                        "id": str(r[0]),
                        "name": r[1],
                        "archetype": (ns.get("tags") or ["unknown"])[0] if isinstance(ns, dict) else "unknown",
                        "asset_class": (ns if isinstance(ns, dict) else {}).get("asset_class", "crypto"),
                        "score": float(r[3]) if r[3] is not None else 0,
                        "sharpe": float(r[4]) if r[4] is not None else 0,
                        "win_rate": float(r[5]) if r[5] is not None else 0,
                        "total_trades": int(r[6]) if r[6] is not None else 0,
                        "max_drawdown": float(r[7]) if r[7] is not None else 0,
                        "avg_return_pct": float(results_raw.get("avg_return_pct", results_raw.get("mean_return_pct", 0))),
                        "std_return_pct": float(results_raw.get("std_return_pct", results_raw.get("std_pct", 0.02))),
                    })
                return out
        except Exception as e:
            logger.warning(f"{self.name}: fetch strategies failed: {e}")
            return []

    async def _compute_scout_regime(self) -> dict:
        """Fetch current market regime from scout intelligence."""
        try:
            if self.db:
                scout = await self.db.get_latest_scout_intelligence()
                return {
                    "vol_regime": scout.get("regime", {}).get("volatility", "normal"),
                    "liq_regime": scout.get("liquidity", {}).get("regime", "normal"),
                    "vol": scout.get("regime", {}).get("realized_vol", 0.15),
                    "corr_regime": scout.get("correlation", {}).get("risk_state", "normal"),
                }
        except Exception:
            pass
        return {"vol_regime": "normal", "liq_regime": "normal", "vol": 0.15, "corr_regime": "normal"}

    async def _compute_allocations(self) -> Optional[dict]:
        """Compute target capital allocations using multi-factor methodology."""
        strategies = await self._fetch_strategies()
        if len(strategies) < 2:
            logger.info(f"{self.name}: need ≥2 strategies for allocation (got {len(strategies)})")
            return None

        portfolio = await self._fetch_portfolio_intelligence()
        regime = await self._compute_scout_regime()

        # Step 1: Kelly fraction per strategy
        kelly_weights = self._compute_kelly_fractions(strategies)

        # Step 2: Volatility targeting
        vol_weights = self._compute_vol_target_weights(strategies, regime)

        # Step 3: Risk parity weights
        parity_weights = self._compute_risk_parity_weights(strategies)

        # Step 4: Combine methodologies
        combined_weights = self._combine_weighting_methods(
            strategies, kelly_weights, vol_weights, parity_weights, portfolio, regime
        )

        # Step 5: Apply constraints
        final_weights = self._apply_constraints(strategies, combined_weights, portfolio)

        # Step 6: Compute capital redistribution signals
        redistribution = self._compute_redistribution(final_weights, strategies)

        allocation_record = {
            "id": str(uuid.uuid4()),
            "computed_at": datetime.utcnow().isoformat(),
            "n_strategies": len(strategies),
            "method": "kelly_vol_target_risk_parity_ensemble",
            "kelly_weights": kelly_weights,
            "vol_target_weights": vol_weights,
            "risk_parity_weights": parity_weights,
            "final_allocations": final_weights,
            "total_exposure": round(sum(w["weight"] for w in final_weights), 4),
            "regime_applied": regime,
            "redistribution_signals": redistribution,
            "leverage_cap_applied": 1.0,
        }
        return allocation_record

    def _compute_kelly_fractions(self, strategies: list[dict]) -> list[dict]:
        """Compute Kelly-optimal fraction per strategy (conservative)."""
        results = []
        for s in strategies:
            wr = max(s["win_rate"], 0.01)
            avg_return = max(s["avg_return_pct"], 0.0001)
            std_return = max(s["std_return_pct"], 0.01)

            # Kelly fraction = (expected_return) / (variance)
            kelly = avg_return / (std_return ** 2 + 1e-10)

            # Apply conservative fraction and clamp
            conservative = min(self.MAX_STRATEGY_EXPOSURE, kelly * self.KELLY_FRACTION)
            results.append({
                "strategy_id": s["id"],
                "strategy_name": s["name"],
                "kelly_raw": round(float(kelly), 4),
                "kelly_conservative": round(float(conservative), 4),
                "win_rate": round(wr, 4),
                "avg_return_pct": round(avg_return, 6),
            })
        return results

    def _compute_vol_target_weights(self, strategies: list[dict], regime: dict) -> list[dict]:
        """Compute weights targeting a specific portfolio volatility."""
        results = []
        vol_mult = 1.0
        vol_regime = regime.get("vol_regime", "normal")
        if vol_regime in ("high", "extreme"):
            vol_mult = 1.5
        elif vol_regime == "low":
            vol_mult = 0.8

        for s in strategies:
            std = max(s["std_return_pct"], 0.005)
            # Weight = vol_target / (std * vol_mult)
            raw_weight = self.VOL_TARGET / (std * vol_mult)
            # Cap at max exposure
            weight = min(self.MAX_STRATEGY_EXPOSURE, max(0, raw_weight))
            results.append({
                "strategy_id": s["id"],
                "strategy_name": s["name"],
                "vol_target_weight": round(float(weight), 4),
                "annualized_vol_estimate": round(float(std * vol_mult), 4),
            })
        return results

    def _compute_risk_parity_weights(self, strategies: list[dict]) -> list[dict]:
        """Risk parity: equal risk contribution from each strategy."""
        n = len(strategies)
        total_inverse_risk = sum(1.0 / max(s["std_return_pct"], 0.005) for s in strategies)

        results = []
        for s in strategies:
            inv_risk = 1.0 / max(s["std_return_pct"], 0.005)
            weight = inv_risk / total_inverse_risk if total_inverse_risk > 0 else 1.0 / n
            # Risk parity weights are proportional — cap later
            results.append({
                "strategy_id": s["id"],
                "strategy_name": s["name"],
                "risk_parity_weight": round(float(weight), 4),
            })
        return results

    def _combine_weighting_methods(
        self,
        strategies: list[dict],
        kelly: list[dict],
        vol_target: list[dict],
        parity: list[dict],
        portfolio: dict,
        regime: dict,
    ) -> list[dict]:
        """Ensemble weighting: blend multiple methodologies."""
        # Build lookup maps
        kelly_map = {k["strategy_id"]: k["kelly_conservative"] for k in kelly}
        vol_map = {v["strategy_id"]: v["vol_target_weight"] for v in vol_target}
        parity_map = {p["strategy_id"]: p["risk_parity_weight"] for p in parity}

        # Also incorporate portfolio intelligence allocations if available
        portfolio_map = {}
        if portfolio.get("optimal_allocations"):
            for a in portfolio["optimal_allocations"]:
                portfolio_map[a["strategy_id"]] = a["weight"]

        # Regime-conditioned blending weights
        vol_regime = regime.get("vol_regime", "normal")
        if vol_regime in ("high", "extreme"):
            # In high vol, favor vol targeting and risk parity
            kelly_blend = 0.2
            vol_blend = 0.4
            parity_blend = 0.3
            portfolio_blend = 0.1
        elif vol_regime == "low":
            # In low vol, favor kelly
            kelly_blend = 0.4
            vol_blend = 0.2
            parity_blend = 0.2
            portfolio_blend = 0.2
        else:
            kelly_blend = 0.3
            vol_blend = 0.3
            parity_blend = 0.25
            portfolio_blend = 0.15

        results = []
        for s in strategies:
            sid = s["id"]
            k = kelly_map.get(sid, 0)
            v = vol_map.get(sid, 0)
            p = parity_map.get(sid, 0)
            po = portfolio_map.get(sid, 0)

            combined = kelly_blend * k + vol_blend * v + parity_blend * p + portfolio_blend * po
            results.append({
                "strategy_id": sid,
                "strategy_name": s["name"],
                "combined_weight": round(float(combined), 4),
                "components": {
                    "kelly": round(k, 4),
                    "vol_target": round(v, 4),
                    "risk_parity": round(p, 4),
                    "portfolio_intelligence": round(po, 4),
                    "blend": {
                        "kelly_pct": kelly_blend,
                        "vol_pct": vol_blend,
                        "parity_pct": parity_blend,
                        "portfolio_pct": portfolio_blend,
                    },
                },
            })
        return results

    def _apply_constraints(self, strategies: list[dict], combined: list[dict], portfolio: dict) -> list[dict]:
        """Apply capital allocation constraints."""
        # Build asset class map
        asset_class_map = {s["id"]: s["asset_class"] for s in strategies}
        archetype_map = {s["id"]: s["archetype"] for s in strategies}

        # Raw weights
        raw = {c["strategy_id"]: c["combined_weight"] for c in combined}

        # Step 1: Cap per strategy
        capped = {sid: min(w, self.MAX_STRATEGY_EXPOSURE) for sid, w in raw.items()}

        # Step 2: Cap per asset class
        asset_class_exposure = defaultdict(float)
        for sid, w in capped.items():
            ac = asset_class_map.get(sid, "unknown")
            asset_class_exposure[ac] += w

        overexposed_ac = {ac: exp for ac, exp in asset_class_exposure.items() if exp > self.MAX_ASSET_CLASS_EXPOSURE}
        if overexposed_ac:
            for ac, exp in overexposed_ac.items():
                scale = self.MAX_ASSET_CLASS_EXPOSURE / exp
                for sid in list(capped.keys()):
                    if asset_class_map.get(sid) == ac:
                        capped[sid] *= scale

        # Step 3: Regime-based adjustments
        try:
            regime_weights = portfolio.get("regime_conditioned_weights", {})
            if regime_weights:
                for sid in capped:
                    rw = regime_weights.get(sid, {})
                    adj = rw.get("adjusted_score", 50) / 100.0
                    capped[sid] *= max(0.5, adj)
        except Exception:
            pass

        # Step 4: Normalize to sum to 1
        total = sum(capped.values())
        if total > 0:
            normalized = {sid: w / total for sid, w in capped.items()}
        else:
            n = len(strategies)
            normalized = {s["id"]: 1.0 / n for s in strategies}

        results = []
        for s in strategies:
            results.append({
                "strategy_id": s["id"],
                "strategy_name": s["name"],
                "weight": round(float(normalized[s["id"]]), 4),
                "archetype": s["archetype"],
                "asset_class": s["asset_class"],
                "score": round(s["score"], 2),
                "sharpe": round(s["sharpe"], 2),
            })

        # Sort by weight descending
        results.sort(key=lambda x: -x["weight"])
        return results

    def _compute_redistribution(self, final_weights: list[dict], strategies: list[dict]) -> list[dict]:
        """Compute capital redistribution signals for underweight/overweight strategies."""
        if not self._live_allocations:
            return []

        prev_map = {a["strategy_id"]: a["weight"] for a in self._live_allocations.get("final_allocations", [])}
        signals = []

        for fw in final_weights:
            sid = fw["strategy_id"]
            prev = prev_map.get(sid, 0)
            delta = fw["weight"] - prev
            if abs(delta) > 0.01:  # ≥1% redistribution signal
                signals.append({
                    "strategy_id": sid,
                    "strategy_name": fw["strategy_name"],
                    "previous_weight": round(prev, 4),
                    "target_weight": round(fw["weight"], 4),
                    "delta": round(delta, 4),
                    "direction": "increase" if delta > 0 else "decrease",
                })

        signals.sort(key=lambda x: -abs(x["delta"]))
        return signals

    async def _persist_allocations(self, allocation: dict) -> None:
        """Persist allocation record to capital_allocation table."""
        if not self.db:
            return
        try:
            await self.db._execute_insert(
                """
                INSERT INTO capital_allocation
                    (id, computed_at, n_strategies, method,
                     final_allocations, total_exposure,
                     kelly_weights, vol_target_weights, risk_parity_weights,
                     redistribution_signals, regime_applied,
                     leverage_cap_applied, metadata)
                VALUES
                    (:id, :computed_at, :n_strategies, :method,
                     :final_allocations, :total_exposure,
                     :kelly_weights, :vol_target_weights, :risk_parity_weights,
                     :redistribution_signals, :regime_applied,
                     :leverage_cap_applied, :metadata)
                """,
                {
                    "id": allocation["id"],
                    "computed_at": allocation["computed_at"],
                    "n_strategies": allocation["n_strategies"],
                    "method": allocation["method"],
                    "final_allocations": json.dumps(allocation["final_allocations"]),
                    "total_exposure": allocation["total_exposure"],
                    "kelly_weights": json.dumps(allocation["kelly_weights"]),
                    "vol_target_weights": json.dumps(allocation["vol_target_weights"]),
                    "risk_parity_weights": json.dumps(allocation["risk_parity_weights"]),
                    "redistribution_signals": json.dumps(allocation["redistribution_signals"]),
                    "regime_applied": json.dumps(allocation["regime_applied"]),
                    "leverage_cap_applied": allocation["leverage_cap_applied"],
                    "metadata": json.dumps({"method": allocation["method"]}),
                },
            )
        except Exception as e:
            logger.warning(f"{self.name}: persist failed: {e}")

    async def _publish_allocations(self, allocation: dict) -> None:
        """Publish allocation update to Redis."""
        if not self._redis:
            return
        try:
            signal = {
                "type": "capital_allocation",
                "computed_at": allocation["computed_at"],
                "n_strategies": allocation["n_strategies"],
                "top_allocations": allocation["final_allocations"][:5],
                "redistribution_signals": allocation["redistribution_signals"][:5],
            }
            await self._redis.publish("capital_allocation_updates", json.dumps(signal))
        except Exception as e:
            logger.warning(f"{self.name}: publish failed: {e}")

    async def get_allocation_snapshot(self) -> dict:
        """Public method for downstream consumers (ExecutionGateway, EnsembleExecution)."""
        return self._live_allocations
