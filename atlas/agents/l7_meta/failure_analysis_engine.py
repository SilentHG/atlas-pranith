"""
failure_analysis_engine.py — Phase 19D: Failure Analysis Engine

Postmortem reasoning on strategy failures, execution anomalies,
and systemic patterns. Uses Claude for root-cause analysis.

ADVISORY ONLY — never modifies strategies, execution state,
or governance policies.

Inputs:
  - Failed validation strategies
  - Retired strategies (from StrategyRetirementEngine)
  - Execution anomalies (from ExecutionIntelligence)
  - Drift spikes (from DriftDetection)
  - Scout disagreement
  - Portfolio drawdowns

Outputs:
  - Root-cause analysis
  - Systemic risk warnings
  - Mutation collapse alerts
  - Execution realism concerns
  - Feature saturation observations
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent
from atlas.core.serialization import safe_json_dumps


class FailureAnalysisEngine(BaseAgent):
    """
    L7 Meta Agent — Failure postmortem and systemic pattern detection.
    advisory_only=True: Cannot execute trades, mutate strategies, or allocate capital.
    """

    name = "FailureAnalysisEngine"
    agent_type = "failure_analysis"
    layer = "L7"

    def __init__(
        self,
        redis_client,
        db_client,
        claude_client=None,
        run_interval: int = 3600,  # 60 minutes
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
        self._prior_analyses: list[dict] = []  # Meta-memory

    async def run(self):
        logger.info(
            f"{self.name}: starting failure analysis (every {self.run_interval}s)"
        )
        while self.status == "running":
            try:
                await self._analysis_cycle()
            except Exception as e:
                logger.error(f"{self.name}: analysis cycle error: {e}", exc_info=True)

            for _ in range(self.run_interval // 10):
                if self.status != "running":
                    return
                await asyncio.sleep(10)

    async def _analysis_cycle(self):
        """Execute one full failure analysis cycle."""
        # 1. Gather failure data
        failure_data = await self._gather_failure_data()
        if not failure_data or failure_data.get("total_failures", 0) == 0:
            logger.debug(f"{self.name}: no failures to analyze")
            return

        # 2. Detect systemic patterns (deterministic)
        systemic_patterns = self._detect_systemic_patterns(failure_data)

        # 3. Generate root-cause analysis (LLM or deterministic)
        if self._llm_enabled and self._claude:
            analysis = await self._generate_llm_analysis(failure_data, systemic_patterns)
        else:
            analysis = self._generate_deterministic_analysis(failure_data, systemic_patterns)

        # 4. Persist
        trace_id = uuid.uuid4().hex[:16]
        await self._persist_analysis(trace_id, analysis, failure_data)

        # 5. Update meta-memory
        self._prior_analyses.append({
            "trace_id": trace_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "n_failures": failure_data.get("total_failures", 0),
            "top_pattern": systemic_patterns[0]["pattern"] if systemic_patterns else "none",
        })
        self._prior_analyses = self._prior_analyses[-10:]

        logger.info(
            f"{self.name}: analyzed {failure_data.get('total_failures', 0)} failures, "
            f"found {len(systemic_patterns)} systemic patterns"
        )

    async def _gather_failure_data(self) -> dict[str, Any]:
        """Aggregate failure signals from all subsystems."""
        data: dict[str, Any] = {"total_failures": 0}
        if not self.db:
            return data

        try:
            async with self.db.engine.connect() as conn:
                # Failed validation strategies
                r = await conn.execute(text("""
                    SELECT s.id, s.name, s.status, s.compile_error,
                           s.parameters, s.created_at
                    FROM strategies s
                    WHERE s.status IN ('failed_validation', 'repair_candidate', 'compile_error')
                      AND s.created_at > NOW() - INTERVAL '48 hours'
                    ORDER BY s.created_at DESC LIMIT 30
                """))
                failed_strategies = []
                for row in r.fetchall():
                    params = row[4]
                    if isinstance(params, str):
                        try:
                            params = json.loads(params)
                        except Exception:
                            params = {}
                    failed_strategies.append({
                        "id": str(row[0]), "name": row[1], "status": row[2],
                        "error": row[3] or "", "params": params,
                        "created_at": str(row[5]),
                    })
                data["failed_strategies"] = failed_strategies
                data["total_failures"] = len(failed_strategies)

                # Recently retired strategies
                r = await conn.execute(text("""
                    SELECT n_retired, n_retirement_pending,
                           retirement_recommendations, analyzed_at
                    FROM strategy_retirement
                    ORDER BY analyzed_at DESC LIMIT 1
                """))
                row = r.fetchone()
                if row:
                    recs = row[2]
                    if isinstance(recs, str):
                        try:
                            recs = json.loads(recs)
                        except Exception:
                            recs = []
                    data["retirement"] = {
                        "n_retired": row[0] or 0,
                        "n_pending": row[1] or 0,
                        "recommendations": recs or [],
                    }
                    data["total_failures"] += (row[0] or 0)

                # Drift spikes
                r = await conn.execute(text("""
                    SELECT feature_drift_score, strategy_drift_score,
                           regime_drift_score, composite_severity
                    FROM drift_detection
                    WHERE composite_severity > 0.4
                      AND detected_at > NOW() - INTERVAL '24 hours'
                    ORDER BY detected_at DESC LIMIT 3
                """))
                drift_spikes = []
                for row in r.fetchall():
                    drift_spikes.append({
                        "feature": float(row[0] or 0),
                        "strategy": float(row[1] or 0),
                        "regime": float(row[2] or 0),
                        "composite": float(row[3] or 0),
                    })
                data["drift_spikes"] = drift_spikes

                # Execution anomalies
                r = await conn.execute(text("""
                    SELECT symbol, avg_slippage_bps, rejection_rate,
                           fill_quality_score, execution_regime
                    FROM execution_intelligence
                    WHERE fill_quality_score < 0.5 OR rejection_rate > 0.1
                    ORDER BY timestamp DESC LIMIT 5
                """))
                exec_anomalies = []
                for row in r.fetchall():
                    exec_anomalies.append({
                        "symbol": row[0], "slippage_bps": float(row[1] or 0),
                        "rejection_rate": float(row[2] or 0),
                        "fill_quality": float(row[3] or 0),
                        "regime": row[4] or "unknown",
                    })
                data["execution_anomalies"] = exec_anomalies

                # Mutation performance
                r = await conn.execute(text("""
                    SELECT mutation_type,
                           COUNT(*) as total,
                           COUNT(*) FILTER (WHERE improved = TRUE) as improved,
                           AVG(score_delta) as avg_delta
                    FROM mutation_memory
                    WHERE created_at > NOW() - INTERVAL '48 hours'
                    GROUP BY mutation_type
                """))
                data["mutation_performance"] = [
                    {"type": row[0], "total": row[1], "improved": row[2],
                     "avg_delta": float(row[3] or 0)}
                    for row in r.fetchall()
                ]

        except Exception as e:
            logger.warning(f"{self.name}: failure data gathering error: {e}")

        return data

    def _detect_systemic_patterns(self, data: dict) -> list[dict]:
        """Deterministic pattern detection across failures."""
        patterns = []

        # Pattern 1: Feature saturation in failures
        failed = data.get("failed_strategies", [])
        if failed:
            feature_counts: Counter = Counter()
            for s in failed:
                params = s.get("params", {})
                if isinstance(params, dict):
                    for key in ("entry_conditions", "exit_conditions"):
                        conds = params.get(key, [])
                        if isinstance(conds, list):
                            for cond in conds:
                                if isinstance(cond, str):
                                    import re
                                    features = re.findall(r"\b[a-z_][a-z_0-9]+\b", cond)
                                    feature_counts.update(features)

            top_features = feature_counts.most_common(3)
            if top_features and top_features[0][1] > len(failed) * 0.6:
                patterns.append({
                    "pattern": "feature_saturation",
                    "severity": "high",
                    "description": f"Feature '{top_features[0][0]}' appears in {top_features[0][1]}/{len(failed)} failed strategies",
                    "evidence": dict(top_features),
                })

        # Pattern 2: Compile error clustering
        compile_errors = [s for s in failed if s.get("status") == "compile_error"]
        if len(compile_errors) > len(failed) * 0.5:
            patterns.append({
                "pattern": "compile_error_cluster",
                "severity": "high",
                "description": f"{len(compile_errors)}/{len(failed)} failures are compile errors — template generation issue",
                "evidence": {"compile_count": len(compile_errors), "total": len(failed)},
            })

        # Pattern 3: Mutation collapse
        mutation_perf = data.get("mutation_performance", [])
        if mutation_perf:
            negative_types = [m for m in mutation_perf if m["avg_delta"] < 0]
            if len(negative_types) == len(mutation_perf) and len(mutation_perf) >= 3:
                patterns.append({
                    "pattern": "mutation_entropy_collapse",
                    "severity": "high",
                    "description": "All mutation types producing negative score deltas",
                    "evidence": {m["type"]: m["avg_delta"] for m in mutation_perf},
                })

        # Pattern 4: Drift-correlated failures
        drift_spikes = data.get("drift_spikes", [])
        if drift_spikes and failed:
            patterns.append({
                "pattern": "drift_correlated_failures",
                "severity": "medium",
                "description": f"{len(drift_spikes)} drift spikes coinciding with {len(failed)} strategy failures",
                "evidence": {"drift_count": len(drift_spikes), "failure_count": len(failed)},
            })

        # Pattern 5: Execution degradation
        exec_anomalies = data.get("execution_anomalies", [])
        if len(exec_anomalies) >= 3:
            patterns.append({
                "pattern": "execution_degradation",
                "severity": "medium",
                "description": f"{len(exec_anomalies)} execution anomalies detected — fill quality concerns",
                "evidence": {"anomaly_count": len(exec_anomalies)},
            })

        return patterns

    async def _generate_llm_analysis(
        self, data: dict, patterns: list[dict]
    ) -> dict:
        """Use Claude for root-cause analysis of failures."""
        summary = json.dumps({
            "total_failures": data.get("total_failures", 0),
            "failed_strategies_sample": data.get("failed_strategies", [])[:5],
            "drift_spikes": data.get("drift_spikes", []),
            "mutation_performance": data.get("mutation_performance", []),
            "execution_anomalies": data.get("execution_anomalies", []),
            "systemic_patterns": patterns,
        }, indent=2, default=str)[:4000]

        system_prompt = (
            "You are a quantitative trading system reliability engineer performing "
            "failure postmortems. Output ONLY valid JSON."
        )
        user_prompt = f"""Analyze these system failures and produce a root-cause analysis.

FAILURE DATA:
{summary}

Output JSON:
{{
    "root_causes": ["primary cause", "secondary cause"],
    "systemic_risk_level": "low|medium|high|critical",
    "confidence": 0.0-1.0,
    "governance_recommendations": ["recommendation 1"],
    "mutation_collapse_warnings": ["warning if applicable"],
    "feature_saturation_alerts": ["alert if applicable"],
    "execution_realism_concerns": ["concern if applicable"],
    "temporal_pattern": "worsening|stable|improving|unknown"
}}"""

        try:
            raw = await self._claude.complete(
                user=user_prompt, system=system_prompt,
                max_tokens=600, temperature=0.3,
            )
            cleaned = raw.strip()
            f = cleaned.find("{")
            l = cleaned.rfind("}")
            if f == -1 or l == -1:
                return self._generate_deterministic_analysis(data, patterns)

            analysis = json.loads(cleaned[f:l + 1])
            analysis.setdefault("root_causes", [])
            analysis.setdefault("confidence", 0.5)
            analysis.setdefault("governance_recommendations", [])
            return analysis

        except Exception as e:
            logger.warning(f"{self.name}: LLM analysis failed: {e}")
            return self._generate_deterministic_analysis(data, patterns)

    def _generate_deterministic_analysis(
        self, data: dict, patterns: list[dict]
    ) -> dict:
        """Deterministic failure analysis fallback."""
        root_causes = []
        recommendations = []
        mutation_warnings = []
        feature_alerts = []
        execution_concerns = []
        risk_level = "low"
        confidence = 0.5

        for p in patterns:
            severity = p.get("severity", "low")
            if severity == "high":
                risk_level = "high"
                confidence = max(confidence, 0.7)

            if p["pattern"] == "feature_saturation":
                root_causes.append(f"Feature saturation: {p['description']}")
                feature_alerts.append(p["description"])
                recommendations.append("Diversify feature selection in strategy generation.")

            elif p["pattern"] == "compile_error_cluster":
                root_causes.append(f"Template failure: {p['description']}")
                recommendations.append("Audit template grammar for syntax edge cases.")

            elif p["pattern"] == "mutation_entropy_collapse":
                root_causes.append(f"Mutation collapse: {p['description']}")
                mutation_warnings.append(p["description"])
                recommendations.append("Expand mutation operator space or reset exploration weights.")

            elif p["pattern"] == "drift_correlated_failures":
                root_causes.append(f"Drift correlation: {p['description']}")
                recommendations.append("Investigate if drift is causing validation failures.")

            elif p["pattern"] == "execution_degradation":
                root_causes.append(f"Execution quality: {p['description']}")
                execution_concerns.append(p["description"])
                recommendations.append("Review execution parameters and broker connectivity.")

        if not root_causes:
            root_causes = ["No dominant failure pattern identified"]
            recommendations = ["Continue monitoring"]

        return {
            "root_causes": root_causes,
            "systemic_risk_level": risk_level,
            "confidence": confidence,
            "governance_recommendations": recommendations,
            "mutation_collapse_warnings": mutation_warnings,
            "feature_saturation_alerts": feature_alerts,
            "execution_realism_concerns": execution_concerns,
            "temporal_pattern": "unknown",
        }

    async def _persist_analysis(
        self, trace_id: str, analysis: dict, failure_data: dict
    ) -> None:
        """Persist failure analysis to failure_analysis table."""
        if not self.db:
            return
        try:
            await self.db._execute_insert(
                """
                INSERT INTO failure_analysis
                    (id, trace_id, analysis_type, confidence,
                     root_causes, systemic_patterns,
                     governance_recommendations,
                     mutation_collapse_warnings,
                     feature_saturation_alerts,
                     n_failures_analyzed, advisory_only, metadata, created_at)
                VALUES
                    (:id, :trace_id, :analysis_type, :confidence,
                     CAST(:root_causes AS jsonb), CAST(:systemic_patterns AS jsonb),
                     CAST(:governance_recommendations AS jsonb),
                     CAST(:mutation_collapse_warnings AS jsonb),
                     CAST(:feature_saturation_alerts AS jsonb),
                     :n_failures, TRUE, CAST(:metadata AS jsonb), NOW())
                """,
                {
                    "id": uuid.uuid4().hex[:16],
                    "trace_id": trace_id,
                    "analysis_type": "periodic_postmortem",
                    "confidence": analysis.get("confidence", 0.0),
                    "root_causes": safe_json_dumps(analysis.get("root_causes", [])),
                    "systemic_patterns": safe_json_dumps(analysis.get("root_causes", [])),
                    "governance_recommendations": safe_json_dumps(
                        analysis.get("governance_recommendations", [])
                    ),
                    "mutation_collapse_warnings": safe_json_dumps(
                        analysis.get("mutation_collapse_warnings", [])
                    ),
                    "feature_saturation_alerts": safe_json_dumps(
                        analysis.get("feature_saturation_alerts", [])
                    ),
                    "n_failures": failure_data.get("total_failures", 0),
                    "metadata": safe_json_dumps({
                        "risk_level": analysis.get("systemic_risk_level", "unknown"),
                        "temporal_pattern": analysis.get("temporal_pattern", "unknown"),
                        "llm_used": self._llm_enabled,
                        "agent": self.name,
                    }),
                },
            )
        except Exception as e:
            logger.warning(f"{self.name}: persist analysis failed: {e}")
