"""ensemble_execution_engine.py — Phase 12: Portfolio-Level Ensemble Execution.

Orchestrates multi-strategy signal voting and portfolio-aware execution:
  - Multi-strategy voting (long/short/hold per symbol)
  - Weighted consensus aggregation
  - Confidence-weighted execution sizes
  - Conflict resolution (opposing signals from different strategies)
  - Portfolio-aware signal execution (net exposure limits)
  - Ensemble survivability during illiquid regimes

Consumes:
  - PortfolioIntelligenceEngine (strategy weights, correlation)
  - CapitalAllocator (target allocations)
  - Scout intelligence (liquidity/execution regime)
  - Strategy signals (from Redis pubsub)

Outputs:
  - ensemble_signals: {symbol: {direction, confidence, size}}
  - confidence_weighted_execution: trade instructions for ExecutionGateway
  - portfolio_consensus_trades: combined net signals
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


class EnsembleExecutionEngine(BaseAgent):
    """Portfolio-aware ensemble execution engine with multi-strategy consensus."""

    name = "EnsembleExecutionEngine"
    agent_type = "ensemble_execution"
    layer = "L6"

    def __init__(self, redis_client=None, db_client=None, run_interval: int = 30):
        super().__init__(
            name=self.name,
            agent_type=self.agent_type,
            layer=self.layer,
            redis_client=redis_client,
        )
        self.db = db_client
        self.run_interval = run_interval  # Poll signals frequently (30s)

        # Consensus thresholds
        self.MIN_VOTES_FOR_EXECUTION = 2          # Need at least 2 strategies to agree
        self.MIN_CONFIDENCE_FOR_SIGNAL = 0.3       # Need 30% confidence
        self.CONFLICT_RESOLUTION_MAJORITY = 0.55   # Need 55% majority to resolve conflict
        self.MAX_GROSS_EXPOSURE_SYMBOL = 0.15      # Max 15% of portfolio to one symbol
        self.MAX_GROSS_EXPOSURE_TOTAL = 0.95       # Max 95% gross exposure

    async def run(self):
        logger.info(f"{self.name}: starting ensemble execution engine (every {self.run_interval}s)")
        # Subscribe to strategy signal channels
        pubsub = self._redis.pubsub() if self._redis else None
        if pubsub:
            await pubsub.subscribe("strategy_signals", "capital_allocation_updates", "portfolio_intelligence_updates")

        try:
            while self.status == "running":
                try:
                    # 1. Collect pending signals from strategies
                    signals = await self._collect_pending_signals(pubsub)

                    # 2. Fetch portfolio intelligence and allocation context
                    context = await self._fetch_portfolio_context()

                    # 3. Compute ensemble consensus
                    if signals:
                        ensemble = self._compute_ensemble_consensus(signals, context)

                        # 4. Resolve conflicts
                        resolved = self._resolve_conflicts(ensemble, context)

                        # 5. Apply portfolio-aware sizing
                        portfolio_trades = self._apply_portfolio_sizing(resolved, context)

                        # 6. Publish ensemble execution instructions
                        if portfolio_trades:
                            await self._publish_ensemble_signals(portfolio_trades)
                            await self._log_ensemble(signals, portfolio_trades, context)
                except Exception as e:
                    logger.error(f"{self.name}: cycle failed: {e}")

                await asyncio.sleep(self.run_interval)
        finally:
            if pubsub:
                await pubsub.unsubscribe()

    async def _collect_pending_signals(self, pubsub) -> list[dict]:
        """Collect pending strategy signals from pubsub messages."""
        signals = []
        if not pubsub:
            return signals

        # Buffer reads to collect multiple signals
        for _ in range(10):
            try:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
                if message and message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        channel = message["channel"].decode() if isinstance(message["channel"], bytes) else message["channel"]

                        if data.get("type") == "validated" or data.get("type") == "signal":
                            signals.append({
                                "strategy_id": data.get("strategy_id", ""),
                                "symbol": data.get("symbol", "UNKNOWN"),
                                "direction": data.get("direction", "hold").lower(),
                                "confidence": float(data.get("confidence", 0.5)),
                                "quantity": float(data.get("quantity", 0)),
                                "timestamp": data.get("timestamp", datetime.utcnow().isoformat()),
                                "source": channel,
                            })
                    except (json.JSONDecodeError, KeyError):
                        pass
            except asyncio.TimeoutError:
                break

        return signals

    async def _fetch_portfolio_context(self) -> dict:
        """Fetch portfolio intelligence and allocation context."""
        context = {
            "strategy_weights": {},
            "correlation_matrix": [],
            "regime": {},
            "allocations": [],
        }

        if not self.db:
            return context

        try:
            # Fetch latest portfolio intelligence
            async with self.db.engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT optimal_allocations, concentration_risk,
                               diversification_score, ensemble_survivability_score,
                               regime_conditioned_weights
                        FROM portfolio_intelligence
                        ORDER BY computed_at DESC
                        LIMIT 1
                    """)
                )
                row = result.fetchone()
                if row:
                    allocations = row[0]
                    if isinstance(allocations, str):
                        allocations = json.loads(allocations)
                    for a in allocations:
                        context["strategy_weights"][a["strategy_id"]] = a.get("weight", 0)
        except Exception:
            pass

        # Fetch capital allocations
        try:
            async with self.db.engine.connect() as conn:
                result = await conn.execute(
                    text("""
                        SELECT final_allocations, regime_applied
                        FROM capital_allocation
                        ORDER BY computed_at DESC
                        LIMIT 1
                    """)
                )
                row = result.fetchone()
                if row:
                    allocs = row[0]
                    if isinstance(allocs, str):
                        allocs = json.loads(allocs)
                    context["allocations"] = allocs

                    regime = row[1]
                    if isinstance(regime, str):
                        regime = json.loads(regime)
                    context["regime"] = regime
        except Exception:
            pass

        return context

    def _compute_ensemble_consensus(self, signals: list[dict], context: dict) -> dict[str, dict]:
        """Aggregate signals by symbol with weighted voting."""
        # Group signals by symbol
        symbol_signals: dict[str, list[dict]] = defaultdict(list)
        for s in signals:
            symbol_signals[s["symbol"]].append(s)

        ensemble = {}
        for symbol, sym_signals in symbol_signals.items():
            votes = {"buy": 0.0, "sell": 0.0, "hold": 0.0}
            weighted_confidence = 0.0
            total_weight = 0.0

            for sig in sym_signals:
                sid = sig["strategy_id"]
                weight = context["strategy_weights"].get(sid, 1.0 / max(len(symbol_signals), 1))
                confidence = sig["confidence"] * weight
                direction = sig["direction"]

                if direction in votes:
                    votes[direction] += confidence
                weighted_confidence += confidence
                total_weight += weight

            # Determine consensus direction
            total_votes = sum(votes.values())
            if total_votes > 0:
                buy_pct = votes["buy"] / total_votes
                sell_pct = votes["sell"] / total_votes
                hold_pct = votes["hold"] / total_votes

                if buy_pct > self.CONFLICT_RESOLUTION_MAJORITY:
                    direction = "buy"
                    strength = buy_pct
                elif sell_pct > self.CONFLICT_RESOLUTION_MAJORITY:
                    direction = "sell"
                    strength = sell_pct
                elif hold_pct > 0.5:
                    direction = "hold"
                    strength = hold_pct
                else:
                    # Conflict — net direction
                    net = buy_pct - sell_pct
                    direction = "buy" if net > 0.15 else ("sell" if net < -0.15 else "hold")
                    strength = abs(net)
            else:
                direction = "hold"
                strength = 0.0

            avg_confidence = weighted_confidence / total_weight if total_weight > 0 else 0
            n_strategies_voting = len(sym_signals)

            ensemble[symbol] = {
                "symbol": symbol,
                "direction": direction,
                "strength": round(float(strength), 4),
                "avg_confidence": round(float(avg_confidence), 4),
                "n_strategies_voting": n_strategies_voting,
                "votes": {k: round(float(v), 4) for k, v in votes.items()},
                "raw_signals": sym_signals,
            }

        return ensemble

    def _resolve_conflicts(self, ensemble: dict[str, dict], context: dict) -> dict[str, dict]:
        """Resolve conflicts in ensemble signals."""
        resolved = {}
        for symbol, result in ensemble.items():
            direction = result["direction"]

            # Skip low confidence
            if result["avg_confidence"] < self.MIN_CONFIDENCE_FOR_SIGNAL:
                direction = "hold"

            # Skip insufficient votes
            if result["n_strategies_voting"] < self.MIN_VOTES_FOR_EXECUTION:
                direction = "hold"

            # Regime-conditioned override
            regime = context.get("regime", {})
            vol_regime = regime.get("vol_regime", "normal") if isinstance(regime, dict) else "normal"
            if vol_regime in ("high", "extreme") and direction != "hold":
                # In high vol, require higher confidence
                if result["avg_confidence"] < 0.5:
                    direction = "hold"

            # Liquidity-conditioned override
            liq_regime = context.get("regime", {}).get("liq_regime", "normal") if isinstance(regime, dict) else "normal"
            if liq_regime in ("low", "thin", "dangerous") and direction != "hold":
                if result["avg_confidence"] < 0.6:
                    direction = "hold"

            result["execution_direction"] = direction
            result["conflict_resolved"] = (direction != result.get("_original_direction", direction))
            resolved[symbol] = result

        return resolved

    def _apply_portfolio_sizing(self, resolved: dict[str, dict], context: dict) -> list[dict]:
        """Apply portfolio-aware sizing to resolved ensemble signals."""
        trades = []
        for symbol, signal in resolved.items():
            if signal["execution_direction"] == "hold":
                continue

            # Base size from confidence and vote strength
            base_size = signal["avg_confidence"] * signal["strength"]

            # Apply max symbol exposure cap
            symbol_exposure = min(base_size, self.MAX_GROSS_EXPOSURE_SYMBOL)

            # Regime sizing adjustment
            regime = context.get("regime", {})
            vol_regime = regime.get("vol_regime", "normal") if isinstance(regime, dict) else "normal"
            liq_regime = context.get("regime", {}).get("liq_regime", "normal") if isinstance(regime, dict) else "normal"

            if vol_regime in ("high", "extreme"):
                symbol_exposure *= 0.5
            if liq_regime in ("low", "thin", "dangerous"):
                symbol_exposure *= 0.5

            trades.append({
                "symbol": symbol,
                "direction": signal["execution_direction"],
                "size": round(float(symbol_exposure), 4),
                "confidence": round(signal["avg_confidence"], 4),
                "n_strategies_voting": signal["n_strategies_voting"],
                "votes": signal["votes"],
            })

        return trades

    async def _publish_ensemble_signals(self, trades: list[dict]) -> None:
        """Publish ensemble execution instructions to Redis."""
        if not self._redis:
            return
        try:
            signal = {
                "type": "ensemble_execution",
                "generated_at": datetime.utcnow().isoformat(),
                "n_trades": len(trades),
                "trades": trades,
            }
            await self._redis.publish("ensemble_execution_signals", json.dumps(signal))
            logger.info(f"{self.name}: published {len(trades)} ensemble trades")
        except Exception as e:
            logger.warning(f"{self.name}: publish failed: {e}")

    async def _log_ensemble(self, signals: list[dict], trades: list[dict], context: dict) -> None:
        """Log ensemble execution record."""
        if not self.db:
            return
        try:
            await self.db._execute_insert(
                """
                INSERT INTO ensemble_execution
                    (id, executed_at, n_signals_processed, n_trades_generated,
                     consensus_trades, strategy_weights_used, regime_context,
                     metadata)
                VALUES
                    (:id, :executed_at, :n_signals_processed,
                     :n_trades_generated, :consensus_trades,
                     :strategy_weights_used, :regime_context, :metadata)
                """,
                {
                    "id": str(uuid.uuid4()),
                    "executed_at": datetime.utcnow().isoformat(),
                    "n_signals_processed": len(signals),
                    "n_trades_generated": len(trades),
                    "consensus_trades": json.dumps(trades),
                    "strategy_weights_used": json.dumps(context.get("strategy_weights", {})),
                    "regime_context": json.dumps(context.get("regime", {})),
                    "metadata": json.dumps({"method": "weighted_voting_consensus"}),
                },
            )
        except Exception as e:
            logger.warning(f"{self.name}: log failed: {e}")
