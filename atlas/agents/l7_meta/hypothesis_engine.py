"""
hypothesis_engine.py — Phase 19C: Hypothesis Engine

Converts system observations into measurable institutional hypotheses
with structured lifecycle management.

Flow:
  Observation → Hypothesis → Validation Task → Confidence Update
  → Regime Conditioning → Replay Survivability

Hypothesis States:
  active → weakening → dormant → invalidated (archived)
  active → confirmed (archived)

IMPORTANT:
  - No hard-delete of hypotheses
  - Uses confidence decay, archival, regime-conditioned reactivation
  - LLM used only for hypothesis GENERATION from observations
  - Validation, scoring, lifecycle management = deterministic
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent
from atlas.core.serialization import safe_json_dumps


@dataclass
class Hypothesis:
    """Structured hypothesis with institutional lifecycle."""
    id: str
    trace_id: str
    statement: str
    observation_source: str
    testable_prediction: str
    confidence: float = 0.5
    evidence_count: int = 0
    contradiction_count: int = 0
    regime_scope: str = ""
    replay_score: float = 0.0
    decay_rate: float = 0.01
    status: str = "active"
    evidence: list = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    last_confirmed_at: Optional[str] = None
    created_at: str = ""
    updated_at: str = ""


class HypothesisEngine(BaseAgent):
    """
    L7 Meta Agent — Hypothesis lifecycle engine.
    advisory_only=True: Produces research hypotheses, never executes.
    """

    name = "HypothesisEngine"
    agent_type = "hypothesis_engine"
    layer = "L7"

    # Lifecycle constants
    DECAY_INTERVAL_HOURS = 24
    CONFIDENCE_DECAY_PER_CYCLE = 0.02
    WEAKENING_THRESHOLD = 0.3
    DORMANT_THRESHOLD = 0.15
    INVALIDATION_THRESHOLD = 0.05
    CONFIRMATION_THRESHOLD = 0.85
    MAX_ACTIVE_HYPOTHESES = 50

    def __init__(
        self,
        redis_client,
        db_client,
        claude_client=None,
        run_interval: int = 21600,  # 6 hours
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
            f"{self.name}: starting hypothesis engine (every {self.run_interval}s)"
        )
        while self.status == "running":
            try:
                await self._hypothesis_cycle()
            except Exception as e:
                logger.error(f"{self.name}: hypothesis cycle error: {e}", exc_info=True)

            for _ in range(self.run_interval // 10):
                if self.status != "running":
                    return
                await asyncio.sleep(10)

    async def _hypothesis_cycle(self):
        """Execute one full hypothesis lifecycle cycle."""
        # 1. Gather observations from subsystems
        observations = await self._gather_observations()

        # 2. Generate new hypotheses from observations
        if observations:
            new_hypotheses = await self._generate_hypotheses(observations)
            for h in new_hypotheses:
                await self._persist_hypothesis(h)
            if new_hypotheses:
                logger.info(f"{self.name}: generated {len(new_hypotheses)} new hypotheses")

        # 3. Update confidence on existing active hypotheses
        await self._update_active_hypotheses()

        # 4. Apply confidence decay to stale hypotheses
        await self._apply_confidence_decay()

        # 5. Transition lifecycle states
        await self._transition_lifecycle_states()

    async def _gather_observations(self) -> list[dict]:
        """Gather observations from subsystems that could generate hypotheses."""
        observations = []
        if not self.db:
            return observations

        try:
            async with self.db.engine.connect() as conn:
                # Drift observations
                r = await conn.execute(text("""
                    SELECT feature_drift_score, strategy_drift_score,
                           regime_drift_score, composite_severity, detected_at
                    FROM drift_detection
                    ORDER BY detected_at DESC LIMIT 3
                """))
                for row in r.fetchall():
                    if float(row[3] or 0) > 0.3:
                        observations.append({
                            "source": "drift_detection",
                            "type": "drift_escalation",
                            "data": {
                                "feature_drift": float(row[0] or 0),
                                "strategy_drift": float(row[1] or 0),
                                "regime_drift": float(row[2] or 0),
                                "composite": float(row[3] or 0),
                            },
                            "detected_at": str(row[4]),
                        })

                # Mutation entropy observations
                r = await conn.execute(text("""
                    SELECT mutation_type,
                           COUNT(*) as total,
                           COUNT(*) FILTER (WHERE improved = TRUE) as improved,
                           AVG(score_delta) as avg_delta
                    FROM mutation_memory
                    WHERE created_at > NOW() - INTERVAL '48 hours'
                    GROUP BY mutation_type
                    HAVING COUNT(*) >= 3
                """))
                mutation_data = []
                for row in r.fetchall():
                    mutation_data.append({
                        "type": row[0], "total": row[1],
                        "improved": row[2], "avg_delta": float(row[3] or 0),
                    })
                if mutation_data:
                    all_negative = all(m["avg_delta"] < 0 for m in mutation_data)
                    if all_negative:
                        observations.append({
                            "source": "mutation_memory",
                            "type": "mutation_entropy_collapse",
                            "data": {"mutations": mutation_data},
                        })

                # Feature saturation observations
                r = await conn.execute(text("""
                    SELECT feature_name, n_uses, survival_rate, decay_score
                    FROM feature_importance
                    ORDER BY n_uses DESC LIMIT 5
                """))
                top_features = []
                for row in r.fetchall():
                    top_features.append({
                        "name": row[0], "uses": row[1] or 0,
                        "survival": float(row[2] or 0),
                        "decay": float(row[3] or 0),
                    })
                if top_features and top_features[0].get("uses", 0) > 50:
                    observations.append({
                        "source": "feature_importance",
                        "type": "feature_concentration",
                        "data": {"top_features": top_features},
                    })

                # Scout disagreement observations
                r = await conn.execute(text("""
                    SELECT source, AVG(sentiment) as avg_sent,
                           STDDEV(sentiment) as std_sent, COUNT(*) as cnt
                    FROM external_scout_memory
                    WHERE timestamp > NOW() - INTERVAL '24 hours'
                    GROUP BY source
                    HAVING COUNT(*) >= 3
                """))
                scout_sentiments = {}
                for row in r.fetchall():
                    scout_sentiments[row[0]] = {
                        "avg_sentiment": float(row[1] or 0),
                        "std_sentiment": float(row[2] or 0),
                        "count": row[3],
                    }
                if len(scout_sentiments) >= 2:
                    sentiments = [s["avg_sentiment"] for s in scout_sentiments.values()]
                    spread = max(sentiments) - min(sentiments)
                    if spread > 0.5:
                        observations.append({
                            "source": "scout_network",
                            "type": "scout_disagreement",
                            "data": {"sentiments": scout_sentiments, "spread": spread},
                        })

        except Exception as e:
            logger.warning(f"{self.name}: observation gathering error: {e}")

        return observations

    async def _generate_hypotheses(self, observations: list[dict]) -> list[Hypothesis]:
        """Generate hypotheses from observations using LLM or deterministic logic."""
        if self._llm_enabled and self._claude:
            return await self._generate_llm_hypotheses(observations)
        return self._generate_deterministic_hypotheses(observations)

    def _generate_deterministic_hypotheses(self, observations: list[dict]) -> list[Hypothesis]:
        """Deterministic hypothesis generation from structured observations."""
        hypotheses = []
        now = datetime.now(timezone.utc).isoformat()

        for obs in observations:
            obs_type = obs.get("type", "")
            source = obs.get("source", "")
            data = obs.get("data", {})
            h_id = uuid.uuid4().hex[:16]
            trace_id = uuid.uuid4().hex[:16]

            if obs_type == "drift_escalation":
                composite = data.get("composite", 0)
                dominant = max(
                    [("feature", data.get("feature_drift", 0)),
                     ("strategy", data.get("strategy_drift", 0)),
                     ("regime", data.get("regime_drift", 0))],
                    key=lambda x: x[1],
                )
                hypotheses.append(Hypothesis(
                    id=h_id, trace_id=trace_id,
                    statement=f"{dominant[0].title()} drift is the primary driver of composite drift ({composite:.2f})",
                    observation_source=source,
                    testable_prediction=f"If {dominant[0]} drift subsides below 0.2, composite drift will decrease by >30%",
                    confidence=min(0.7, composite),
                    regime_scope="all",
                    created_at=now, updated_at=now,
                ))

            elif obs_type == "mutation_entropy_collapse":
                mutations = data.get("mutations", [])
                worst = min(mutations, key=lambda m: m["avg_delta"]) if mutations else {}
                hypotheses.append(Hypothesis(
                    id=h_id, trace_id=trace_id,
                    statement=f"Mutation exploration space exhausted — all mutation types producing negative deltas",
                    observation_source=source,
                    testable_prediction="Introducing novel mutation operators will restore positive avg_delta within 48h",
                    confidence=0.6,
                    regime_scope="all",
                    metadata={"worst_mutation": worst.get("type", "unknown")},
                    created_at=now, updated_at=now,
                ))

            elif obs_type == "feature_concentration":
                top = data.get("top_features", [{}])[0]
                hypotheses.append(Hypothesis(
                    id=h_id, trace_id=trace_id,
                    statement=f"Feature '{top.get('name', 'unknown')}' is over-represented ({top.get('uses', 0)} uses) — saturation risk",
                    observation_source=source,
                    testable_prediction=f"Strategies avoiding '{top.get('name', '')}' will show higher survival rates in next 7 days",
                    confidence=0.5,
                    regime_scope="all",
                    created_at=now, updated_at=now,
                ))

            elif obs_type == "scout_disagreement":
                spread = data.get("spread", 0)
                hypotheses.append(Hypothesis(
                    id=h_id, trace_id=trace_id,
                    statement=f"Scout sentiment divergence ({spread:.2f}) signals regime transition",
                    observation_source=source,
                    testable_prediction="Market regime will transition within 24h of high scout disagreement",
                    confidence=0.4,
                    regime_scope="transitioning",
                    created_at=now, updated_at=now,
                ))

        return hypotheses

    async def _generate_llm_hypotheses(self, observations: list[dict]) -> list[Hypothesis]:
        """Use Claude to generate richer hypotheses from observations."""
        obs_summary = json.dumps(observations[:5], indent=2, default=str)[:3000]

        system_prompt = (
            "You are a quantitative research scientist generating testable hypotheses "
            "from system observations. Output ONLY a JSON array of hypothesis objects."
        )
        user_prompt = f"""Generate 1-3 testable hypotheses from these system observations:

{obs_summary}

Output JSON array, each object:
{{
    "statement": "clear hypothesis statement",
    "testable_prediction": "specific measurable prediction",
    "confidence": 0.0-1.0,
    "regime_scope": "all|trending|ranging|high_vol|low_vol|transitioning",
    "observation_source": "source name"
}}"""

        try:
            raw = await self._claude.complete(
                user=user_prompt, system=system_prompt,
                max_tokens=600, temperature=0.5,
            )
            cleaned = raw.strip()
            f = cleaned.find("[")
            l = cleaned.rfind("]")
            if f == -1 or l == -1:
                return self._generate_deterministic_hypotheses(observations)

            items = json.loads(cleaned[f:l + 1])
            now = datetime.now(timezone.utc).isoformat()
            hypotheses = []
            for item in items[:3]:
                hypotheses.append(Hypothesis(
                    id=uuid.uuid4().hex[:16],
                    trace_id=uuid.uuid4().hex[:16],
                    statement=item.get("statement", ""),
                    observation_source=item.get("observation_source", "llm_generated"),
                    testable_prediction=item.get("testable_prediction", ""),
                    confidence=float(item.get("confidence", 0.5)),
                    regime_scope=item.get("regime_scope", "all"),
                    created_at=now, updated_at=now,
                ))
            return hypotheses

        except Exception as e:
            logger.warning(f"{self.name}: LLM hypothesis generation failed: {e}")
            return self._generate_deterministic_hypotheses(observations)

    async def _update_active_hypotheses(self) -> None:
        """Check active hypotheses against current system state for evidence/contradiction."""
        if not self.db:
            return

        try:
            async with self.db.engine.connect() as conn:
                r = await conn.execute(text("""
                    SELECT id, statement, confidence, evidence_count,
                           contradiction_count, testable_prediction
                    FROM hypothesis_registry
                    WHERE status IN ('active', 'weakening')
                    ORDER BY confidence DESC
                    LIMIT 50
                """))
                active = r.fetchall()

            for row in active:
                h_id = row[0]
                current_confidence = float(row[2] or 0.5)

                # Simple evidence/contradiction updates based on system state
                # A real implementation would check specific predictions
                # against measurable outcomes
                evidence_delta, contradiction_delta = await self._evaluate_hypothesis(
                    h_id, row[5]
                )

                if evidence_delta > 0 or contradiction_delta > 0:
                    new_confidence = current_confidence
                    new_confidence += evidence_delta * 0.05
                    new_confidence -= contradiction_delta * 0.08
                    new_confidence = max(0.0, min(1.0, new_confidence))

                    async with self.db.engine.begin() as conn:
                        await conn.execute(text("""
                            UPDATE hypothesis_registry
                            SET confidence = :conf,
                                evidence_count = evidence_count + :ev,
                                contradiction_count = contradiction_count + :ct,
                                last_confirmed_at = CASE WHEN :ev > 0 THEN NOW() ELSE last_confirmed_at END,
                                updated_at = NOW()
                            WHERE id = :id
                        """), {
                            "conf": new_confidence,
                            "ev": evidence_delta,
                            "ct": contradiction_delta,
                            "id": h_id,
                        })

        except Exception as e:
            logger.warning(f"{self.name}: hypothesis update error: {e}")

    async def _evaluate_hypothesis(self, h_id: str, prediction: str) -> tuple[int, int]:
        """Evaluate a hypothesis prediction against current state. Returns (evidence, contradictions)."""
        # Deterministic evaluation logic
        # In production, this would check specific measurable outcomes
        return 0, 0

    async def _apply_confidence_decay(self) -> None:
        """Apply time-based confidence decay to hypotheses not recently confirmed."""
        if not self.db:
            return
        try:
            async with self.db.engine.begin() as conn:
                await conn.execute(text("""
                    UPDATE hypothesis_registry
                    SET confidence = GREATEST(0, confidence - decay_rate),
                        updated_at = NOW()
                    WHERE status IN ('active', 'weakening')
                      AND (last_confirmed_at IS NULL
                           OR last_confirmed_at < NOW() - INTERVAL '24 hours')
                """))
        except Exception as e:
            logger.warning(f"{self.name}: decay application error: {e}")

    async def _transition_lifecycle_states(self) -> None:
        """Transition hypotheses between lifecycle states based on confidence."""
        if not self.db:
            return
        try:
            async with self.db.engine.begin() as conn:
                # active → confirmed
                await conn.execute(text(f"""
                    UPDATE hypothesis_registry
                    SET status = 'confirmed', updated_at = NOW()
                    WHERE status = 'active' AND confidence >= {self.CONFIRMATION_THRESHOLD}
                """))

                # active → weakening
                await conn.execute(text(f"""
                    UPDATE hypothesis_registry
                    SET status = 'weakening', updated_at = NOW()
                    WHERE status = 'active' AND confidence < {self.WEAKENING_THRESHOLD}
                """))

                # weakening → dormant
                await conn.execute(text(f"""
                    UPDATE hypothesis_registry
                    SET status = 'dormant', updated_at = NOW()
                    WHERE status = 'weakening' AND confidence < {self.DORMANT_THRESHOLD}
                """))

                # dormant → invalidated (archived, never deleted)
                await conn.execute(text(f"""
                    UPDATE hypothesis_registry
                    SET status = 'invalidated', updated_at = NOW()
                    WHERE status = 'dormant' AND confidence < {self.INVALIDATION_THRESHOLD}
                """))

                # dormant reactivation: if confidence increases, reactivate
                await conn.execute(text(f"""
                    UPDATE hypothesis_registry
                    SET status = 'active', updated_at = NOW()
                    WHERE status = 'dormant' AND confidence >= {self.WEAKENING_THRESHOLD}
                """))

        except Exception as e:
            logger.warning(f"{self.name}: lifecycle transition error: {e}")

    async def _persist_hypothesis(self, h: Hypothesis) -> None:
        """Persist a new hypothesis to hypothesis_registry."""
        if not self.db:
            return
        try:
            await self.db._execute_insert(
                """
                INSERT INTO hypothesis_registry
                    (id, trace_id, statement, observation_source, testable_prediction,
                     confidence, evidence_count, contradiction_count, regime_scope,
                     replay_score, decay_rate, status, evidence, metadata,
                     last_confirmed_at, created_at, updated_at)
                VALUES
                    (:id, :trace_id, :statement, :observation_source, :testable_prediction,
                     :confidence, :evidence_count, :contradiction_count, :regime_scope,
                     :replay_score, :decay_rate, :status, CAST(:evidence AS jsonb), CAST(:metadata AS jsonb),
                     :last_confirmed_at, NOW(), NOW())
                """,
                {
                    "id": h.id,
                    "trace_id": h.trace_id,
                    "statement": h.statement,
                    "observation_source": h.observation_source,
                    "testable_prediction": h.testable_prediction,
                    "confidence": h.confidence,
                    "evidence_count": h.evidence_count,
                    "contradiction_count": h.contradiction_count,
                    "regime_scope": h.regime_scope,
                    "replay_score": h.replay_score,
                    "decay_rate": h.decay_rate,
                    "status": h.status,
                    "evidence": safe_json_dumps(h.evidence),
                    "metadata": safe_json_dumps(h.metadata),
                    "last_confirmed_at": h.last_confirmed_at,
                },
            )
        except Exception as e:
            logger.warning(f"{self.name}: persist hypothesis failed: {e}")
