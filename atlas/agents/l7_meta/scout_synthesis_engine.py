"""
scout_synthesis_engine.py — Phase 19F: Scout Synthesis Engine

Synthesizes heterogeneous scout outputs into coherent, confidence-weighted
institutional narratives.

ADVISORY ONLY — never emits direct trading signals, never allocates capital.

Inputs (from existing scouts):
  - RegimeScout → market_regime_memory
  - LiquidityScout → liquidity_intelligence
  - CorrelationScout → correlation_memory
  - ExecutionScout → execution_intelligence
  - Reddit/News/Discord scouts → external_scout_memory
  - SourceReliabilityEngine → source weights

Outputs:
  - Contextual market narratives
  - Scout agreement/disagreement metrics
  - Confidence-weighted synthesis
  - Environmental interpretations
  - Persisted to scout_synthesis_log
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Optional

import numpy as np
from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent
from atlas.core.serialization import safe_json_dumps


class ScoutSynthesisEngine(BaseAgent):
    """
    L7 Meta Agent — Scout intelligence synthesis.
    advisory_only=True: Produces contextual narratives, never trades.
    """

    name = "ScoutSynthesisEngine"
    agent_type = "scout_synthesis"
    layer = "L7"

    # Default source reliability weights (updated dynamically)
    DEFAULT_WEIGHTS = {
        "regime_scout": 0.85,
        "liquidity_scout": 0.80,
        "correlation_scout": 0.75,
        "execution_scout": 0.80,
        "reddit": 0.40,
        "news": 0.55,
        "discord": 0.35,
        "youtube": 0.30,
        "podcast": 0.25,
    }

    def __init__(
        self,
        redis_client,
        db_client,
        claude_client=None,
        run_interval: int = 1800,  # 30 minutes
    ):
        super().__init__(
            name=self.name,
            agent_type=self.agent_type,
            layer=self.layer,
            redis_client=redis_client,
            advisory_only=True,
        )
        self.db = db_client
        self._claude = claude_client
        self.run_interval = run_interval
        self._llm_enabled = os.environ.get("USE_LLM_META_ADVISOR", "true").lower() == "true"

    async def run(self):
        logger.info(
            f"{self.name}: starting scout synthesis (every {self.run_interval}s)"
        )
        while self.status == "running":
            try:
                await self._synthesis_cycle()
            except Exception as e:
                logger.error(f"{self.name}: synthesis cycle error: {e}", exc_info=True)

            for _ in range(self.run_interval // 10):
                if self.status != "running":
                    return
                await asyncio.sleep(10)

    async def _synthesis_cycle(self):
        """Execute one full scout synthesis cycle."""
        # 1. Gather scout signals
        signals = await self._gather_scout_signals()
        if not signals:
            logger.debug(f"{self.name}: no scout signals to synthesize")
            return

        # 2. Fetch dynamic weights
        dynamic_weights = await self._fetch_dynamic_weights(signals.keys())

        # 3. Compute agreement/disagreement metrics (deterministic)
        metrics = self._compute_agreement_metrics(signals, dynamic_weights)

        # 4. Generate contextual narrative (LLM or deterministic)
        if self._llm_enabled and self._claude:
            synthesis = await self._generate_llm_synthesis(signals, metrics)
        else:
            synthesis = self._generate_deterministic_synthesis(signals, metrics)

        # 4. Persist
        trace_id = uuid.uuid4().hex[:16]
        await self._persist_synthesis(trace_id, synthesis, signals, metrics)

        logger.info(
            f"{self.name}: synthesis complete — agreement={metrics.get('agreement_score', 0):.2f}, "
            f"sources={len(signals)}"
        )

    async def _gather_scout_signals(self) -> dict[str, Any]:
        """Gather latest signals from all scout subsystems."""
        signals: dict[str, Any] = {}
        if not self.db:
            return signals

        try:
            async with self.db.engine.connect() as conn:
                # Regime scout
                r = await conn.execute(text("""
                    SELECT symbol, volatility_regime, trend_regime,
                           liquidity_regime, correlation_regime,
                           confidence_score, timestamp
                    FROM market_regime_memory
                    ORDER BY timestamp DESC LIMIT 5
                """))
                regimes = []
                for row in r.fetchall():
                    regimes.append({
                        "symbol": row[0], "volatility": row[1],
                        "trend": row[2], "liquidity": row[3],
                        "correlation": row[4],
                        "confidence": float(row[5] or 0),
                    })
                if regimes:
                    signals["regime_scout"] = {
                        "type": "internal",
                        "data": regimes,
                        "summary_signal": regimes[0].get("trend", "unknown"),
                    }

                # Liquidity scout
                r = await conn.execute(text("""
                    SELECT symbol, liquidity_score, slippage_risk,
                           liquidity_regime, timestamp
                    FROM liquidity_intelligence
                    ORDER BY timestamp DESC LIMIT 5
                """))
                liquidity = []
                for row in r.fetchall():
                    liquidity.append({
                        "symbol": row[0],
                        "liquidity_score": float(row[1] or 0),
                        "slippage_risk": float(row[2] or 0),
                        "regime": row[3],
                    })
                if liquidity:
                    avg_liq = np.mean([l["liquidity_score"] for l in liquidity])
                    signals["liquidity_scout"] = {
                        "type": "internal",
                        "data": liquidity,
                        "summary_signal": "healthy" if avg_liq > 0.5 else "stressed",
                    }

                # Correlation scout
                r = await conn.execute(text("""
                    SELECT cluster_name, avg_pairwise_corr,
                           risk_state, correlation_spike_detected, timestamp
                    FROM correlation_memory
                    ORDER BY timestamp DESC LIMIT 3
                """))
                correlations = []
                for row in r.fetchall():
                    correlations.append({
                        "cluster": row[0],
                        "avg_corr": float(row[1] or 0),
                        "risk_state": row[2],
                        "spike": bool(row[3]),
                    })
                if correlations:
                    any_spike = any(c["spike"] for c in correlations)
                    signals["correlation_scout"] = {
                        "type": "internal",
                        "data": correlations,
                        "summary_signal": "spike_detected" if any_spike else "normal",
                    }

                # Execution scout
                r = await conn.execute(text("""
                    SELECT symbol, avg_slippage_bps, fill_quality_score,
                           execution_regime, timestamp
                    FROM execution_intelligence
                    ORDER BY timestamp DESC LIMIT 5
                """))
                execution = []
                for row in r.fetchall():
                    execution.append({
                        "symbol": row[0],
                        "slippage_bps": float(row[1] or 0),
                        "fill_quality": float(row[2] or 0),
                        "regime": row[3],
                    })
                if execution:
                    avg_quality = np.mean([e["fill_quality"] for e in execution])
                    signals["execution_scout"] = {
                        "type": "internal",
                        "data": execution,
                        "summary_signal": "healthy" if avg_quality > 0.7 else "degraded",
                    }

                # External scouts (aggregated by source)
                r = await conn.execute(text("""
                    SELECT source, AVG(sentiment) as avg_sent,
                           COUNT(*) as cnt,
                           AVG(hypothesis_score) as avg_hyp
                    FROM external_scout_memory
                    WHERE timestamp > NOW() - INTERVAL '24 hours'
                    GROUP BY source
                    HAVING COUNT(*) >= 2
                """))
                for row in r.fetchall():
                    source = row[0]
                    avg_sent = float(row[1] or 0)
                    signals[source] = {
                        "type": "external",
                        "data": {
                            "avg_sentiment": round(avg_sent, 3),
                            "signal_count": row[2],
                            "avg_hypothesis_score": float(row[3] or 0),
                        },
                        "summary_signal": "bullish" if avg_sent > 0.3 else (
                            "bearish" if avg_sent < -0.3 else "neutral"
                        ),
                    }

        except Exception as e:
            logger.warning(f"{self.name}: scout signal gathering error: {e}")

        return signals

    async def _fetch_dynamic_weights(self, sources: list[str]) -> dict:
        """Fetch dynamic trust scores from source_performance_log."""
        weights = {}
        if not self.db:
            return self.DEFAULT_WEIGHTS
            
        try:
            async with self.db.engine.connect() as conn:
                r = await conn.execute(text("""
                    SELECT source, dynamic_trust_score 
                    FROM source_performance_log
                """))
                for row in r.fetchall():
                    weights[row[0]] = float(row[1])
        except Exception as e:
            logger.debug(f"{self.name}: Error fetching dynamic weights: {e}")
            
        # Fallback to default if missing
        for s in sources:
            if s not in weights:
                weights[s] = self.DEFAULT_WEIGHTS.get(s, 0.3)
        return weights

    def _compute_agreement_metrics(self, signals: dict, dynamic_weights: dict) -> dict:
        """Compute agreement/disagreement, consensus reliability, and Shannon entropy."""
        if not signals:
            return {"agreement_score": 0.0, "disagreement_areas": [], "disagreement_entropy": 0.0}

        # Map summary signals to numeric direction
        signal_map = {
            "bullish": 1.0, "healthy": 0.5, "normal": 0.0,
            "neutral": 0.0, "unknown": 0.0,
            "bearish": -1.0, "stressed": -0.5, "degraded": -0.5,
            "spike_detected": -0.3,
        }

        directions = {}
        weights = {}
        for source, data in signals.items():
            summary = data.get("summary_signal", "unknown")
            direction = signal_map.get(summary, 0.0)
            weight = dynamic_weights.get(source, 0.3)
            directions[source] = direction
            weights[source] = weight

        if not directions:
            return {"agreement_score": 0.0, "disagreement_areas": []}

        # Weighted agreement: how aligned are all sources?
        weighted_values = [directions[s] * weights[s] for s in directions]
        total_weight = sum(weights.values())

        if total_weight == 0:
            return {"agreement_score": 0.0, "disagreement_areas": []}

        weighted_mean = sum(weighted_values) / total_weight

        # Agreement score: 1.0 = all sources agree, 0.0 = maximal disagreement
        deviations = [abs(directions[s] - weighted_mean) for s in directions]
        avg_deviation = np.mean(deviations)
        agreement_score = max(0.0, min(1.0, 1.0 - avg_deviation))

        # Disagreement Entropy (Shannon)
        # Convert directions to a probability distribution (shift to >0)
        shifted = np.array([directions[s] + 1.01 for s in directions])
        probs = shifted / np.sum(shifted)
        entropy = -np.sum(probs * np.log2(probs)) if len(probs) > 1 else 0.0
        max_entropy = np.log2(len(probs)) if len(probs) > 1 else 1.0
        normalized_entropy = entropy / max_entropy if max_entropy > 0 else 0.0

        # Consensus Reliability (weighted sum of trust for participating sources)
        consensus_reliability = np.mean(list(weights.values()))

        # Identify disagreement areas
        disagreement_areas = []
        for source, direction in directions.items():
            if abs(direction - weighted_mean) > 0.5:
                disagreement_areas.append({
                    "source": source,
                    "signal": signals[source].get("summary_signal", "unknown"),
                    "deviation": round(abs(direction - weighted_mean), 3),
                    "source_trust": weights[source]
                })

        return {
            "agreement_score": round(float(agreement_score), 3),
            "disagreement_entropy": round(float(normalized_entropy), 3),
            "consensus_reliability": round(float(consensus_reliability), 3),
            "weighted_direction": round(float(weighted_mean), 3),
            "disagreement_areas": disagreement_areas,
            "source_count": len(directions),
            "confidence_weights": {s: round(w, 3) for s, w in weights.items()},
        }

    async def _generate_llm_synthesis(self, signals: dict, metrics: dict) -> dict:
        """Use Claude to generate contextual market narrative."""
        signal_summary = json.dumps({
            s: {"signal": d.get("summary_signal"), "type": d.get("type")}
            for s, d in signals.items()
        }, indent=2)[:2000]

        system_prompt = (
            "You are a market intelligence analyst synthesizing multi-source scout data. "
            "Output ONLY valid JSON."
        )
        user_prompt = f"""Synthesize these scout signals into a market intelligence narrative.

SIGNALS:
{signal_summary}

AGREEMENT METRICS:
{json.dumps(metrics, indent=2)}

Output JSON:
{{
    "contextual_summary": "2-3 sentence market narrative",
    "market_state_interpretation": "one-line regime interpretation",
    "dominant_theme": "risk_on|risk_off|transitioning|uncertain",
    "confidence": 0.0-1.0,
    "actionable_observations": ["observation 1"]
}}"""

        try:
            raw = await self._claude.complete(
                user=user_prompt, system=system_prompt,
                max_tokens=400, temperature=0.4,
            )
            cleaned = raw.strip()
            f = cleaned.find("{")
            l = cleaned.rfind("}")
            if f == -1 or l == -1:
                return self._generate_deterministic_synthesis(signals, metrics)

            synthesis = json.loads(cleaned[f:l + 1])
            synthesis.setdefault("confidence", 0.5)
            return synthesis

        except Exception as e:
            logger.warning(f"{self.name}: LLM synthesis failed: {e}")
            return self._generate_deterministic_synthesis(signals, metrics)

    def _generate_deterministic_synthesis(self, signals: dict, metrics: dict) -> dict:
        """Deterministic synthesis when LLM unavailable."""
        agreement = metrics.get("agreement_score", 0)
        direction = metrics.get("weighted_direction", 0)
        disagreements = metrics.get("disagreement_areas", [])

        # Determine dominant theme
        if direction > 0.3:
            theme = "risk_on"
            interpretation = "Scout consensus is broadly constructive."
        elif direction < -0.3:
            theme = "risk_off"
            interpretation = "Scout consensus is cautious/defensive."
        elif agreement < 0.4:
            theme = "uncertain"
            interpretation = "Scouts are divided — low-confidence environment."
        else:
            theme = "transitioning"
            interpretation = "Mixed signals across sources — potential regime transition."

        # Build summary
        internal_sources = [s for s, d in signals.items() if d.get("type") == "internal"]
        external_sources = [s for s, d in signals.items() if d.get("type") == "external"]

        summary = (
            f"{len(internal_sources)} internal and {len(external_sources)} external scouts reporting. "
            f"Agreement score: {agreement:.2f}. Weighted direction: {direction:+.2f}. "
        )
        if disagreements:
            summary += f"{len(disagreements)} source(s) diverging from consensus."

        observations = []
        if agreement < 0.3:
            observations.append("Low scout agreement — increase position sizing caution.")
        if any(d.get("deviation", 0) > 0.7 for d in disagreements):
            observations.append("Major source disagreement detected — potential regime shift.")

        return {
            "contextual_summary": summary,
            "market_state_interpretation": interpretation,
            "dominant_theme": theme,
            "confidence": round(agreement * 0.8, 3),
            "actionable_observations": observations,
        }

    async def _persist_synthesis(
        self, trace_id: str, synthesis: dict, signals: dict, metrics: dict
    ) -> None:
        """Persist synthesis to scout_synthesis_log."""
        if not self.db:
            return
        try:
            await self.db._execute_insert(
                """
                INSERT INTO scout_synthesis_log
                    (id, trace_id, confidence, contextual_summary,
                     scout_agreement_score, scout_disagreement_areas,
                     market_state_interpretation, confidence_weights,
                     source_signals, advisory_only, metadata, created_at)
                VALUES
                    (:id, :trace_id, :confidence, :summary,
                     :agreement, CAST(:disagreements AS jsonb),
                     :interpretation, CAST(:weights AS jsonb),
                     CAST(:signals AS jsonb), TRUE, CAST(:metadata AS jsonb), NOW())
                """,
                {
                    "id": uuid.uuid4().hex[:16],
                    "trace_id": trace_id,
                    "confidence": synthesis.get("confidence", 0.0),
                    "summary": synthesis.get("contextual_summary", ""),
                    "agreement": metrics.get("agreement_score", 0.0),
                    "disagreements": safe_json_dumps(
                        metrics.get("disagreement_areas", [])
                    ),
                    "interpretation": synthesis.get("market_state_interpretation", ""),
                    "weights": safe_json_dumps(
                        metrics.get("confidence_weights", {})
                    ),
                    "signals": safe_json_dumps({
                        s: d.get("summary_signal", "unknown")
                        for s, d in signals.items()
                    }),
                    "metadata": safe_json_dumps({
                        "dominant_theme": synthesis.get("dominant_theme", "unknown"),
                        "source_count": len(signals),
                        "llm_used": self._llm_enabled,
                        "agent": self.name,
                    }),
                },
            )
        except Exception as e:
            logger.warning(f"{self.name}: persist synthesis failed: {e}")
