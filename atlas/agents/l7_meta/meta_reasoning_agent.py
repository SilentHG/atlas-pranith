"""
meta_reasoning_agent.py — Phase 19B: Meta Reasoning Agent

Claude-powered high-level institutional reasoning and systemic diagnosis.
ADVISORY ONLY — never executes trades, mutates strategies, validates directly,
or allocates capital.

Inputs:
  - Validator outcomes (strategy pass/fail rates)
  - Mutation patterns (leaderboard, entropy)
  - Scout intelligence (regime, liquidity, correlation, execution)
  - Drift metrics (feature, strategy, regime drift)
  - Execution anomalies (slippage, fill quality)
  - Portfolio fragility (concentration, drawdown)
  - Strategy mortality (retirement, lifecycle)

Outputs:
  - Strategic advisories
  - Governance warnings
  - Mutation recommendations
  - Drift diagnoses
  - Regime interpretations
  - Portfolio observations
  - Execution risk narratives

All outputs persisted to meta_reasoning_log with trace_id, confidence,
replay compatibility.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent
from atlas.core.serialization import safe_json_dumps


class MetaReasoningAgent(BaseAgent):
    """
    L7 Meta Agent — Institutional reasoning and systemic diagnosis.
    advisory_only=True: Cannot execute trades, mutate strategies, or allocate capital.
    """

    name = "MetaReasoningAgent"
    agent_type = "meta_reasoning"
    layer = "L7"

    # Advisory types this agent produces
    ADVISORY_TYPES = [
        "strategic_advisory",
        "governance_warning",
        "mutation_recommendation",
        "drift_diagnosis",
        "regime_interpretation",
        "portfolio_observation",
        "execution_risk_narrative",
    ]

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
            advisory_only=True,  # CRITICAL: advisory-only enforcement
        )
        self.db = db_client
        self._claude = claude_client
        self.run_interval = run_interval
        self._llm_enabled = os.environ.get("USE_LLM_META_ADVISOR", "true").lower() == "true"
        self._prior_analyses: list[dict] = []  # Meta-memory of recent analyses

    async def run(self):
        logger.info(
            f"{self.name}: starting meta reasoning (every {self.run_interval}s, "
            f"LLM={'enabled' if self._llm_enabled else 'disabled'})"
        )
        while self.status == "running":
            try:
                await self._reasoning_cycle()
            except Exception as e:
                logger.error(f"{self.name}: reasoning cycle error: {e}", exc_info=True)

            # Interruptible sleep
            for _ in range(self.run_interval // 10):
                if self.status != "running":
                    return
                await asyncio.sleep(10)

    async def _reasoning_cycle(self):
        """Execute one full reasoning cycle."""
        # 1. Gather system state snapshot
        state = await self._gather_system_state()
        if not state:
            logger.debug(f"{self.name}: insufficient system state for reasoning")
            return

        # 2. Generate reasoning (LLM or deterministic fallback)
        if self._llm_enabled and self._claude:
            advisory = await self._generate_llm_reasoning(state)
        else:
            advisory = self._generate_deterministic_reasoning(state)

        if not advisory:
            return

        # 3. Persist advisory with trace lineage
        trace_id = uuid.uuid4().hex[:16]
        await self._persist_advisory(trace_id, advisory, state)

        # 4. Update meta-memory (keep last 10 analyses for temporal comparison)
        self._prior_analyses.append({
            "trace_id": trace_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "summary": advisory.get("summary", ""),
            "confidence": advisory.get("confidence", 0.0),
        })
        self._prior_analyses = self._prior_analyses[-10:]

        logger.info(
            f"{self.name}: advisory generated — type={advisory.get('advisory_type', 'unknown')} "
            f"confidence={advisory.get('confidence', 0):.2f}"
        )

    async def _gather_system_state(self) -> dict[str, Any]:
        """Aggregate telemetry from all subsystems."""
        state: dict[str, Any] = {}
        if not self.db:
            return state

        try:
            async with self.db.engine.connect() as conn:
                # Strategy pipeline health
                r = await conn.execute(text("""
                    SELECT status, COUNT(*) as cnt FROM strategies
                    GROUP BY status ORDER BY cnt DESC LIMIT 10
                """))
                state["strategy_pipeline"] = {row[0]: row[1] for row in r.fetchall()}

                # Recent validation outcomes
                r = await conn.execute(text("""
                    SELECT COUNT(*) FILTER (WHERE status = 'validated') as validated,
                           COUNT(*) FILTER (WHERE status = 'failed_validation') as failed,
                           COUNT(*) FILTER (WHERE status = 'retired') as retired,
                           COUNT(*) as total
                    FROM strategies
                    WHERE created_at > NOW() - INTERVAL '24 hours'
                """))
                row = r.fetchone()
                if row:
                    state["recent_24h"] = {
                        "validated": row[0] or 0,
                        "failed": row[1] or 0,
                        "retired": row[2] or 0,
                        "total": row[3] or 0,
                    }

                # Latest drift metrics
                r = await conn.execute(text("""
                    SELECT feature_drift_score, strategy_drift_score,
                           regime_drift_score, composite_severity
                    FROM drift_detection
                    ORDER BY detected_at DESC LIMIT 1
                """))
                row = r.fetchone()
                if row:
                    state["drift"] = {
                        "feature": float(row[0]) if row[0] else 0,
                        "strategy": float(row[1]) if row[1] else 0,
                        "regime": float(row[2]) if row[2] else 0,
                        "composite": float(row[3]) if row[3] else 0,
                    }

                # Mutation leaderboard summary
                r = await conn.execute(text("""
                    SELECT mutation_type,
                           COUNT(*) as total,
                           COUNT(*) FILTER (WHERE improved = TRUE) as improved,
                           ROUND(AVG(score_delta)::numeric, 2) as avg_delta
                    FROM mutation_memory
                    WHERE created_at > NOW() - INTERVAL '7 days'
                    GROUP BY mutation_type
                    ORDER BY avg_delta DESC NULLS LAST
                    LIMIT 8
                """))
                state["mutation_summary"] = [
                    {"type": row[0], "total": row[1], "improved": row[2],
                     "avg_delta": float(row[3]) if row[3] else 0}
                    for row in r.fetchall()
                ]

                # Latest retirement scan
                r = await conn.execute(text("""
                    SELECT n_strategies_analyzed, n_retired, n_retirement_pending, n_monitor
                    FROM strategy_retirement
                    ORDER BY analyzed_at DESC LIMIT 1
                """))
                row = r.fetchone()
                if row:
                    state["retirement"] = {
                        "analyzed": row[0] or 0, "retired": row[1] or 0,
                        "pending": row[2] or 0, "monitor": row[3] or 0,
                    }

                # Scout intelligence summary
                r = await conn.execute(text("""
                    SELECT source, COUNT(*) as cnt,
                           AVG(sentiment) as avg_sentiment
                    FROM external_scout_memory
                    WHERE timestamp > NOW() - INTERVAL '24 hours'
                    GROUP BY source
                """))
                state["scout_signals"] = {
                    row[0]: {"count": row[1], "avg_sentiment": round(float(row[2] or 0), 3)}
                    for row in r.fetchall()
                }

                # Execution health
                r = await conn.execute(text("""
                    SELECT avg_slippage_bps, fill_quality_score, execution_regime
                    FROM execution_intelligence
                    ORDER BY timestamp DESC LIMIT 1
                """))
                row = r.fetchone()
                if row:
                    state["execution"] = {
                        "slippage_bps": float(row[0]) if row[0] else 0,
                        "fill_quality": float(row[1]) if row[1] else 0,
                        "regime": row[2] or "unknown",
                    }

                # Portfolio fragility
                r = await conn.execute(text("""
                    SELECT concentration_risk, diversification_score,
                           ensemble_survivability_score
                    FROM portfolio_intelligence
                    ORDER BY computed_at DESC LIMIT 1
                """))
                row = r.fetchone()
                if row:
                    state["portfolio"] = {
                        "concentration_risk": float(row[0]) if row[0] else 0,
                        "diversification": float(row[1]) if row[1] else 0,
                        "ensemble_survival": float(row[2]) if row[2] else 0,
                    }

        except Exception as e:
            logger.warning(f"{self.name}: state gathering partial failure: {e}")

        # Attach meta-memory of prior analyses
        state["prior_analyses"] = self._prior_analyses[-5:]

        return state

    async def _generate_llm_reasoning(self, state: dict) -> Optional[dict]:
        """Use Claude for high-level institutional reasoning."""
        state_summary = json.dumps(state, indent=2, default=str)[:4000]

        system_prompt = (
            "You are the Chief Intelligence Officer for an institutional quantitative "
            "trading system called ATLAS. You analyze system telemetry and produce "
            "strategic advisories. Output ONLY valid JSON. No markdown, no prose outside JSON."
        )

        user_prompt = f"""Analyze the following ATLAS system state and produce a strategic advisory.

SYSTEM STATE:
{state_summary}

PRIOR ANALYSES (for temporal comparison):
{json.dumps(self._prior_analyses[-3:], default=str)}

Produce ONE advisory in this exact JSON format:
{{
    "advisory_type": "strategic_advisory|governance_warning|drift_diagnosis|regime_interpretation|portfolio_observation|execution_risk_narrative|mutation_recommendation",
    "summary": "one-sentence headline",
    "confidence": 0.0-1.0,
    "reasoning": "2-3 sentence institutional reasoning",
    "recommendations": ["actionable recommendation 1", "..."],
    "risk_level": "low|medium|high|critical",
    "recurring_pattern": true|false,
    "temporal_trend": "improving|stable|degrading|unknown"
}}

Focus on the MOST IMPORTANT systemic observation. Prioritize:
1. Capital safety risks
2. Execution integrity concerns
3. Drift escalation patterns
4. Mutation entropy collapse
5. Portfolio fragility
6. Scout disagreement"""

        try:
            raw = await self._claude.complete(
                user=user_prompt,
                system=system_prompt,
                max_tokens=800,
                temperature=0.4,
            )
            # Parse JSON from response
            cleaned = raw.strip()
            f = cleaned.find("{")
            l = cleaned.rfind("}")
            if f == -1 or l == -1:
                logger.warning(f"{self.name}: no JSON in Claude response")
                return self._generate_deterministic_reasoning(state)

            advisory = json.loads(cleaned[f:l + 1])

            # Validate required fields
            advisory.setdefault("advisory_type", "strategic_advisory")
            advisory.setdefault("confidence", 0.5)
            advisory.setdefault("recommendations", [])
            advisory.setdefault("risk_level", "medium")

            return advisory

        except Exception as e:
            logger.warning(f"{self.name}: LLM reasoning failed ({e}), using deterministic fallback")
            return self._generate_deterministic_reasoning(state)

    def _generate_deterministic_reasoning(self, state: dict) -> dict:
        """Deterministic fallback reasoning when LLM is unavailable."""
        advisory_type = "strategic_advisory"
        summary = "System operating within normal parameters."
        confidence = 0.6
        recommendations = []
        risk_level = "low"

        # Check drift escalation
        drift = state.get("drift", {})
        composite_drift = drift.get("composite", 0)
        if composite_drift > 0.7:
            advisory_type = "drift_diagnosis"
            summary = f"Composite drift severity elevated at {composite_drift:.2f}."
            confidence = 0.8
            recommendations.append("Review retirement candidates for drift-triggered strategies.")
            risk_level = "high"
        elif composite_drift > 0.4:
            advisory_type = "drift_diagnosis"
            summary = f"Moderate drift detected ({composite_drift:.2f})."
            confidence = 0.7
            recommendations.append("Monitor drift trend for escalation.")
            risk_level = "medium"

        # Check validation failure rate
        recent = state.get("recent_24h", {})
        total = recent.get("total", 0)
        failed = recent.get("failed", 0)
        if total > 0 and failed / max(total, 1) > 0.8:
            advisory_type = "governance_warning"
            summary = f"Strategy validation failure rate critically high: {failed}/{total} in 24h."
            confidence = 0.9
            recommendations.append("Audit ideation parameters and feature selection.")
            recommendations.append("Check for template exhaustion or feature saturation.")
            risk_level = "high"

        # Check retirement pressure
        retirement = state.get("retirement", {})
        if retirement.get("pending", 0) > 5:
            recommendations.append(
                f"{retirement['pending']} strategies pending retirement — review capital allocation."
            )

        # Check mutation health
        mutation_summary = state.get("mutation_summary", [])
        if mutation_summary:
            all_negative = all(m.get("avg_delta", 0) < 0 for m in mutation_summary)
            if all_negative:
                advisory_type = "mutation_recommendation"
                summary = "All mutation types showing negative score deltas — entropy collapse risk."
                confidence = 0.75
                recommendations.append("Diversify mutation exploration space.")
                risk_level = "medium"

        # Check execution health
        execution = state.get("execution", {})
        if execution.get("slippage_bps", 0) > 10:
            recommendations.append(
                f"Execution slippage elevated at {execution['slippage_bps']:.1f}bps."
            )

        # Check portfolio concentration
        portfolio = state.get("portfolio", {})
        if portfolio.get("concentration_risk", 0) > 0.7:
            recommendations.append(
                f"Portfolio concentration risk at {portfolio['concentration_risk']:.2f} — diversify."
            )

        return {
            "advisory_type": advisory_type,
            "summary": summary,
            "confidence": confidence,
            "reasoning": "Deterministic analysis of system telemetry.",
            "recommendations": recommendations,
            "risk_level": risk_level,
            "recurring_pattern": False,
            "temporal_trend": "unknown",
        }

    async def _persist_advisory(
        self, trace_id: str, advisory: dict, state: dict
    ) -> None:
        """Persist advisory to meta_reasoning_log with full trace lineage."""
        if not self.db:
            return
        try:
            await self.db._execute_insert(
                """
                INSERT INTO meta_reasoning_log
                    (id, trace_id, advisory_type, confidence, reasoning_text,
                     system_state_snapshot, recommendations, advisory_only, metadata, created_at)
                VALUES
                    (:id, :trace_id, :advisory_type, :confidence, :reasoning_text,
                     CAST(:snapshot AS jsonb), CAST(:recommendations AS jsonb), TRUE, CAST(:metadata AS jsonb), NOW())
                """,
                {
                    "id": uuid.uuid4().hex[:16],
                    "trace_id": trace_id,
                    "advisory_type": advisory.get("advisory_type", "strategic_advisory"),
                    "confidence": advisory.get("confidence", 0.0),
                    "reasoning_text": (
                        f"{advisory.get('summary', '')} — {advisory.get('reasoning', '')}"
                    ),
                    "snapshot": safe_json_dumps(state),
                    "recommendations": safe_json_dumps(advisory.get("recommendations", [])),
                    "metadata": safe_json_dumps({
                        "risk_level": advisory.get("risk_level", "unknown"),
                        "temporal_trend": advisory.get("temporal_trend", "unknown"),
                        "recurring_pattern": advisory.get("recurring_pattern", False),
                        "llm_used": self._llm_enabled,
                        "agent": self.name,
                    }),
                },
            )
        except Exception as e:
            logger.warning(f"{self.name}: persist advisory failed: {e}")
