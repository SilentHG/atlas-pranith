"""
mutation_policy_engine.py — L7 Meta Agent for mutation policy learning and adaptation.

Capabilities:
- Adaptive mutation weighting based on historical success
- Mutation reinforcement learning (positive/negative signals)
- Mutation suppression for harmful mutation types
- Regime-conditioned mutation selection
- Phase 19E: LLM advisory reasoning for mutation direction priorities
  (deterministic weighting remains canonical — Claude advises only)
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent
from atlas.core.serialization import safe_json_dumps


class MutationPolicyEngine(BaseAgent):
    """
    L7 Meta Agent — Learns optimal mutation policies over time.
    Phase 19E: Added advisory reasoning layer (optional, LLM-powered).
    Deterministic _learn_policy() and select_mutation_type() remain canonical.
    """

    name = "MutationPolicyEngine"
    agent_type = "mutation_policy"
    layer = "L7"

    # Default mutation type weights
    DEFAULT_WEIGHTS = {
        "parameter_shift": 0.20,
        "indicator_replace": 0.15,
        "threshold_tighten": 0.15,
        "threshold_loosen": 0.15,
        "combine_with": 0.10,
        "regime_adapt": 0.10,
        "risk_adjust": 0.10,
        "exit_logic": 0.05,
    }

    def __init__(self, redis_client, db_client, claude_client=None):
        super().__init__(self.name, self.agent_type, self.layer, redis_client)
        self.db = db_client
        self._claude = claude_client
        self._run_interval = 3600  # Every hour
        self._weights = dict(self.DEFAULT_WEIGHTS)
        self._llm_enabled = os.environ.get("USE_LLM_META_ADVISOR", "true").lower() == "true"

    async def run(self):
        logger.info(f"{self.name}: Starting mutation policy learning")

        while self.status == "running":
            try:
                await self._learn_policy()

                # Phase 19E: Generate advisory reasoning after deterministic learning
                # Phase 26B: Scout-aware mutation policy adaptation
                scout = await self._fetch_scout_context_for_policy()
                if scout:
                    self._apply_scout_to_weights(scout)

                if self._llm_enabled:
                    await self._generate_mutation_advisory()
            except Exception as e:
                logger.error(f"{self.name}: Policy learning error: {e}")

            for _ in range(self._run_interval // 10):
                await self._sleep(10)
                if self.status != "running":
                    return

    async def _sleep(self, seconds: int):
        await asyncio.sleep(seconds)

    async def _learn_policy(self):
        """Analyze mutation outcomes and adjust weights. DETERMINISTIC — canonical."""
        mutation_outcomes = await self._load_mutation_outcomes()
        if not mutation_outcomes:
            return

        # Group by mutation type
        by_type: dict[str, list[dict]] = {}
        for m in mutation_outcomes:
            mtype = m.get("mutation_type", "unknown")
            if mtype not in by_type:
                by_type[mtype] = []
            by_type[mtype].append(m)

        # Compute success rate per type
        success_rates = {}
        for mtype, outcomes in by_type.items():
            successes = sum(1 for o in outcomes if o.get("outcome_score", 0) > 0)
            success_rates[mtype] = successes / max(1, len(outcomes))

        # Update weights: higher success rate = higher weight
        total_rate = sum(success_rates.values()) or 1.0
        new_weights = {
            mtype: max(0.01, rate / total_rate)
            for mtype, rate in success_rates.items()
            if mtype in self.DEFAULT_WEIGHTS
        }

        # Normalize weights to sum to 1.0
        total = sum(new_weights.values())
        if total > 0:
            new_weights = {k: v / total for k, v in new_weights.items()}
        else:
            new_weights = dict(self.DEFAULT_WEIGHTS)

        # Add back any missing types with minimum weight
        for mtype in self.DEFAULT_WEIGHTS:
            if mtype not in new_weights:
                new_weights[mtype] = 0.01

        # Renormalize
        total = sum(new_weights.values())
        self._weights = {k: v / total for k, v in new_weights.items()}

        # Persist policy
        await self.db._execute_insert(
            """
            INSERT INTO mutation_policy_state
                (id, learned_at, mutation_weights, per_type_success_rates,
                 n_observations, details)
            VALUES
                (:id, NOW(), CAST(:weights AS jsonb), CAST(:rates AS jsonb),
                 :n_obs, CAST(:details AS jsonb))
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "weights": json.dumps(self._weights),
                "rates": json.dumps(success_rates),
                "n_obs": len(mutation_outcomes),
                "details": json.dumps({
                    "per_type_counts": {k: len(v) for k, v in by_type.items()},
                }),
            },
        )

        logger.info(
            f"{self.name}: Policy updated — {len(mutation_outcomes)} observations, "
            f"{len(self._weights)} mutation types"
        )

    async def _load_mutation_outcomes(self) -> list[dict]:
        """Load recent mutation outcomes with scores."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT m.id, m.mutation_type, m.id as strategy_id,
                           COALESCE(br.short_window_score, 0) as outcome_score
                    FROM strategies m
                    LEFT JOIN LATERAL (
                        SELECT short_window_score
                        FROM backtest_results
                        WHERE strategy_id = m.id
                        ORDER BY start_date DESC LIMIT 1
                    ) br ON TRUE
                    WHERE m.author_agent = 'mutator'
                      AND m.created_at > NOW() - INTERVAL '7 days'
                    ORDER BY m.created_at DESC
                    LIMIT 500
                """)
            )
            return [
                {
                    "id": str(row[0]),
                    "mutation_type": str(row[1]) if row[1] else "unknown",
                    "strategy_id": str(row[2]),
                    "outcome_score": float(row[3] or 0),
                }
                for row in r.fetchall()
            ]



    # ================================================================
    # PHASE 26B — SCOUT-AWARE MUTATION POLICY ADAPTATION
    # ================================================================

    async def _fetch_scout_context_for_policy(self) -> dict | None:
        """Fetch scout context for mutation policy adaptation."""
        try:
            summary = await self.db.get_scout_influence_summary(hours=24)
            if summary and len(summary) > 0:
                regime = {}
                liquidity = {}
                execution = {}
                correlation = {}
                for row in summary:
                    ctx = row.get('regime_context', '') or ''
                    ctx_lower = ctx.lower()
                    if 'high_vol' in ctx_lower or 'panic' in ctx_lower:
                        regime['volatility'] = ctx
                    if 'thin' in ctx_lower or 'dangerous' in ctx_lower:
                        liquidity['regime'] = ctx
                    if 'degraded' in ctx_lower or 'stressed' in ctx_lower:
                        execution['regime'] = ctx
                return {'regime': regime, 'liquidity': liquidity, 'execution': execution, 'correlation': correlation}
            return None
        except Exception:
            return None

    def _apply_scout_to_weights(self, scout: dict) -> None:
        """Modify mutation weights based on scout conditions.
        Modifies self._weights in-memory (does NOT persist to DB).
        """
        regime = scout.get("regime", {})
        liquidity = scout.get("liquidity", {})
        execution = scout.get("execution", {})
        correlation = scout.get("correlation", {})

        vol = str(regime.get("volatility", ""))
        liq_regime = str(liquidity.get("regime", ""))
        exec_regime = str(execution.get("regime", ""))
        corr_risk = str(correlation.get("risk_state", ""))

        adjustments = {}

        # Volatility-based adjustments
        if "high_vol" in vol.lower() or "panic_vol" in vol.lower():
            adjustments = {
                "parameter_shift": 0.30,   # More parameter shifts in volatile markets
                "regime_adapt": 0.20,       # Regime adaptation becomes critical
                "combine_with": 0.05,       # Suppress combination mutations
            }
        elif "low_vol" in vol.lower():
            adjustments = {
                "indicator_replace": 0.25,
                "exit_logic": 0.15,
                "regime_adapt": 0.05,
            }

        # Liquidity-based adjustments
        if liq_regime in ("thin", "dangerous"):
            adjustments.update({
                "threshold_loosen": 0.25,   # Loosen thresholds to avoid false signals
                "threshold_tighten": 0.05,  # Don't tighten further in thin markets
            })

        # Execution-based adjustments
        if exec_regime in ("degraded", "stressed"):
            adjustments.update({
                "risk_adjust": 0.25,        # More risk adjustments
                "exit_logic": 0.20,         # Better exits
            })

        # Apply adjustments if any were computed
        if adjustments:
            new_weights = dict(self._weights)
            for k, v in adjustments.items():
                new_weights[k] = v

            # Normalize
            total = sum(new_weights.values())
            if total > 0:
                self._weights = {k: v / total for k, v in new_weights.items()}
                logger.info(
                    f"{self.name}: Scout-adjusted mutation weights "
                    f"(vol={vol}, liq={liq_regime}, exec={exec_regime})"
                )

    def select_mutation_type(self) -> str:
        """Select a mutation type based on learned weights. DETERMINISTIC — canonical."""
        import random

        types = list(self._weights.keys())
        weights = list(self._weights.values())
        return random.choices(types, weights=weights, k=1)[0]

    async def record_mutation_result(
        self,
        mutation_type: str,
        parent_strategy_id: str,
        child_strategy_id: str,
        success_score: float,
    ):
        """Record a mutation outcome to inform future policy."""
        await self.db._execute_insert(
            """
            INSERT INTO mutation_outcome_log
                (id, mutation_type, parent_strategy_id,
                 child_strategy_id, outcome_score, recorded_at)
            VALUES
                (:id, :mtype, :parent, :child, :score, NOW())
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "mtype": mutation_type,
                "parent": parent_strategy_id,
                "child": child_strategy_id,
                "score": success_score,
            },
        )

    # =================================================================
    # PHASE 19E — LLM ADVISORY LAYER (optional, never canonical)
    # =================================================================

    def _compute_entropy_metric(self) -> float:
        """Compute Shannon entropy of current mutation weight distribution.
        High entropy = diverse exploration. Low = concentrated exploitation.
        """
        values = [w for w in self._weights.values() if w > 0]
        if not values:
            return 0.0
        total = sum(values)
        probs = [v / total for v in values]
        entropy = -sum(p * math.log2(p) for p in probs if p > 0)
        max_entropy = math.log2(len(values)) if len(values) > 1 else 1.0
        return round(entropy / max_entropy, 4)  # Normalized 0-1

    async def _generate_mutation_advisory(self) -> None:
        """Generate advisory reasoning about mutation direction priorities.
        ADVISORY ONLY — does NOT modify self._weights or canonical policy.
        """
        entropy = self._compute_entropy_metric()

        # Determine exploration/exploitation balance
        if entropy > 0.85:
            ee_balance = "exploration_dominant"
        elif entropy > 0.5:
            ee_balance = "balanced"
        else:
            ee_balance = "exploitation_dominant"

        # Load leaderboard for context
        try:
            leaderboard = await self.db.get_mutation_leaderboard()
        except Exception:
            leaderboard = []

        advisory_text = ""
        diversification_advisory = ""

        if self._claude:
            try:
                system_prompt = (
                    "You are a mutation strategy advisor for a quantitative trading system. "
                    "Output ONLY valid JSON."
                )
                user_prompt = f"""Analyze this mutation policy state and advise:

CURRENT WEIGHTS: {json.dumps(self._weights, indent=2)}
ENTROPY: {entropy} ({ee_balance})
LEADERBOARD: {json.dumps(leaderboard[:5], indent=2, default=str)}

Output JSON:
{{
    "advisory": "1-2 sentence mutation direction recommendation",
    "diversification_advisory": "specific suggestion for weight rebalancing",
    "priority_mutations": ["top priority mutation type"],
    "suppress_mutations": ["underperforming mutation type to reduce"]
}}"""

                raw = await self._claude.complete(
                    user=user_prompt, system=system_prompt,
                    max_tokens=300, temperature=0.3,
                )
                cleaned = raw.strip()
                f = cleaned.find("{")
                l = cleaned.rfind("}")
                if f != -1 and l != -1:
                    parsed = json.loads(cleaned[f:l + 1])
                    advisory_text = parsed.get("advisory", "")
                    diversification_advisory = parsed.get("diversification_advisory", "")
            except Exception as e:
                logger.debug(f"{self.name}: LLM advisory skipped: {e}")

        # Deterministic fallback advisory
        if not advisory_text:
            if entropy < 0.3:
                advisory_text = "Entropy critically low — mutation space collapsing toward single type."
                diversification_advisory = "Reset weights toward uniform distribution to restore exploration."
            elif entropy < 0.5:
                advisory_text = "Exploitation-heavy policy — consider boosting underweight mutation types."
                diversification_advisory = "Gradually increase weights for suppressed types."
            else:
                advisory_text = "Mutation policy balanced — continue current exploration strategy."
                diversification_advisory = "No immediate rebalancing needed."

        # Persist advisory to mutation_policy_log (NOT to mutation_policy_state)
        try:
            await self.db._execute_insert(
                """
                INSERT INTO mutation_policy_log
                    (id, trace_id, confidence, advisory,
                     exploration_vs_exploitation, entropy_metric,
                     diversification_advisory, priority_weights,
                     leaderboard_snapshot, advisory_only, metadata, created_at)
                VALUES
                    (:id, :trace_id, :confidence, :advisory,
                     :ee_balance, :entropy,
                     :div_advisory, CAST(:weights AS jsonb),
                     CAST(:leaderboard AS jsonb), TRUE, CAST(:metadata AS jsonb), NOW())
                """,
                {
                    "id": uuid.uuid4().hex[:16],
                    "trace_id": uuid.uuid4().hex[:16],
                    "confidence": 0.6 if advisory_text else 0.3,
                    "advisory": advisory_text,
                    "ee_balance": ee_balance,
                    "entropy": entropy,
                    "div_advisory": diversification_advisory,
                    "weights": json.dumps(self._weights),
                    "leaderboard": safe_json_dumps(leaderboard[:5]),
                    "metadata": safe_json_dumps({
                        "llm_used": self._llm_enabled and self._claude is not None,
                        "agent": self.name,
                    }),
                },
            )
        except Exception as e:
            logger.warning(f"{self.name}: persist mutation advisory failed: {e}")

        logger.info(
            f"{self.name}: Mutation advisory — entropy={entropy:.3f} ({ee_balance})"
        )

