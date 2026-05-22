"""
source_reliability_engine.py — External scout for source reliability assessment.

Capabilities:
- Source trust scoring (reputation, accuracy, timeliness)
- Misinformation/contradiction detection
- Source decay tracking (degrading reliability over time)
- Source influence weighting
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent


# Initial trust scores by source type
DEFAULT_TRUST_SCORES = {
    "reddit_scout": 0.3,
    "youtube_scout": 0.2,
    "discord_scout": 0.2,
    "podcast_scout": 0.4,
    "competition_scout": 0.5,
    "news_intelligence_engine": 0.6,
    "regime_scout": 0.7,
    "liquidity_scout": 0.7,
    "correlation_scout": 0.7,
    "execution_scout": 0.8,
}


class SourceReliabilityEngine(BaseAgent):
    """
    External Scout — Assesses and tracks reliability of all intelligence sources.
    """

    name = "SourceReliabilityEngine"
    agent_type = "source_reliability"
    layer = "Scout"

    def __init__(self, redis_client, db_client):
        super().__init__(
            name="SourceReliabilityEngine",
            agent_type="source_reliability",
            layer="Scout",
            redis_client=redis_client,
        )
        self.db = db_client
        self._run_interval = 3600  # Every hour
        self._trust_scores = dict(DEFAULT_TRUST_SCORES)



    # ================================================================
    # PHASE 26C — ECONOMIC TRUST EVOLUTION
    # ================================================================

    async def _compute_economic_trust_scores(self):
        """Phase 26C: Compute trust scores based on REAL economic outcomes.
        Uses scout_economic_attribution table to measure:
        - Sharpe contribution per scout
        - Drawdown contribution per scout
        - Win rate contribution per scout
        - Validation survival rate
        - Regime specialization
        """
        async with self.db.engine.connect() as conn:
            # Get economic attribution summary per scout source
            r = await conn.execute(
                text("""
                    SELECT source_scout,
                           AVG(COALESCE(sharpe_contribution, 0)) as avg_sharpe,
                           AVG(COALESCE(pnl_contribution, 0)) as avg_pnl,
                           AVG(COALESCE(win_rate_contribution, 0)) as avg_win_rate,
                           COUNT(*) as total_decisions,
                           SUM(CASE WHEN survived_validation THEN 1 ELSE 0 END) as survived_count,
                           AVG(COALESCE(attribution_weight, 0)) as avg_weight
                    FROM scout_economic_attribution
                    WHERE created_at > NOW() - INTERVAL '7 days'
                    GROUP BY source_scout
                """)
            )
            economic_metrics = {}
            for row in r.fetchall():
                source = row[0]
                total = max(1, row[4])
                economic_metrics[source] = {
                    "avg_sharpe": float(row[1] or 0),
                    "avg_pnl": float(row[2] or 0),
                    "avg_win_rate": float(row[3] or 0),
                    "total_decisions": row[4],
                    "survival_rate": row[5] / total if total > 0 else 0,
                    "avg_weight": float(row[6] or 0),
                }

            # Get contradiction counts from quarantine
            r2 = await conn.execute(
                text("""
                    SELECT source, COUNT(*) as contradiction_count
                    FROM scout_poison_quarantine
                    WHERE created_at > NOW() - INTERVAL '7 days'
                    GROUP BY source
                """)
            )
            contradictions = {str(row[0]): row[1] for row in r2.fetchall()}

        # Now compute per-source trust scores with economic signal
        # Iterate all known sources from DEFAULT_TRUST_SCORES
        for source_key in DEFAULT_TRUST_SCORES:
            # Start from base
            base = DEFAULT_TRUST_SCORES.get(source_key, 0.3)
            
            # Economic signal component
            metrics = economic_metrics.get(source_key, {})
            if metrics:
                # Positive Sharpe contribution -> increase trust
                sharpe_bonus = min(0.2, max(-0.2, metrics["avg_sharpe"] * 0.1))
                # Survival rate bonus
                survival_bonus = (metrics["survival_rate"] - 0.5) * 0.2
                # Win rate bonus
                win_rate_bonus = (metrics["avg_win_rate"] - 0.5) * 0.1
                economic_bonus = sharpe_bonus + survival_bonus + win_rate_bonus
            else:
                economic_bonus = 0.0

            # Contradiction penalty
            contradiction_count = contradictions.get(source_key, 0)
            contradiction_penalty = min(0.3, contradiction_count * 0.05)

            # Time decay (base trust decays toward 0.3 over time without new data)
            # This is handled by the staleness decay in _assess_sources

            # Final trust score
            new_trust = max(0.05, min(0.95, base + economic_bonus - contradiction_penalty))
            self._trust_scores[source_key] = new_trust

            # Persist updated trust to source_performance_log
            try:
                await self.db._execute_insert(
                    """
                    INSERT INTO source_performance_log
                        (id, source, source_sub, dynamic_trust_score, historical_accuracy,
                         n_profitable_signals, n_loss_signals, updated_at)
                    VALUES
                        (:id, :source, 'economic', :trust, :acc, 0, 0, NOW())
                    ON CONFLICT (id) DO UPDATE
                        SET dynamic_trust_score = EXCLUDED.dynamic_trust_score,
                            updated_at = NOW()
                    """,
                    {
                        "id": uuid.uuid4().hex[:16],
                        "source": source_key,
                        "trust": round(new_trust, 4),
                        "acc": round(0.5 + economic_bonus, 4),
                    }
                )
            except Exception:
                pass

        logger.info(
            f"{self.name}: Phase 26C economic trust evolution complete for "
            f"{len(economic_metrics)} sources with economic data"
        )

    async def run(self):
        # Phase 26C: Add economic trust evolution to the regular assessment cycle
        logger.info(f"{self.name}: Starting dynamic source reliability tracking")
        while self.status == "running":
            try:
                await self._assess_sources()
                # Phase 26C: Economic trust scoring
                await self._compute_economic_trust_scores()
            except Exception as e:
                logger.error(f"{self.name}: Source assessment error: {e}")
            for _ in range(self._run_interval // 10):
                await asyncio.sleep(10)
                if self.status != "running":
                    return


    async def _assess_sources(self):
        """Assess reliability dynamically based on outcome attribution and decay."""
        # 1. Fetch signal counts and staleness
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT source, source_sub, COUNT(*) as signal_count,
                           MAX(timestamp) as last_signal
                    FROM external_scout_memory
                    GROUP BY source, source_sub
                """)
            )
            sources = r.fetchall()
            
            # 2. Fetch attribution outcomes
            r_attr = await conn.execute(
                text("""
                    SELECT source, source_sub, 
                           COUNT(*) FILTER (WHERE outcome_pnl > 0) as profitable,
                           COUNT(*) FILTER (WHERE outcome_pnl <= 0) as losses
                    FROM scout_signal_attribution
                    GROUP BY source, source_sub
                """)
            )
            outcomes = {(row[0], row[1]): (row[2], row[3]) for row in r_attr.fetchall()}
            
            # 3. Fetch quarantine counts
            r_quar = await conn.execute(
                text("""
                    SELECT source, source_sub, COUNT(*) as quarantine_count
                    FROM scout_poison_quarantine
                    GROUP BY source, source_sub
                """)
            )
            quarantines = {(row[0], row[1]): row[2] for row in r_quar.fetchall()}

        now = datetime.now(timezone.utc)
        
        for row in sources:
            source = str(row[0])
            sub = str(row[1]) if row[1] else "default"
            signal_count = row[2]
            
            # Handle timezone-aware vs naive dates safely
            try:
                last_sig = row[3].replace(tzinfo=timezone.utc)
            except Exception:
                last_sig = now
                
            days_since = (now - last_sig).days
            
            prof, loss = outcomes.get((source, sub), (0, 0))
            quar_count = quarantines.get((source, sub), 0)
            
            # Dynamic scoring logic
            base = DEFAULT_TRUST_SCORES.get(source, 0.3)
            
            # Outcome component (historical accuracy)
            total_outcomes = prof + loss
            accuracy = prof / total_outcomes if total_outcomes > 0 else 0.5
            accuracy_bonus = (accuracy - 0.5) * 0.4 # up to +0.2 or -0.2
            
            # Staleness decay
            decay = min(0.3, days_since * 0.05) # -5% per day inactive, max -30%
            
            # Quarantine slash
            quarantine_penalty = min(0.5, quar_count * 0.2)
            
            trust = max(0.0, min(1.0, base + accuracy_bonus - decay - quarantine_penalty))
            
            # Cache it
            self._trust_scores[f"{source}:{sub}"] = trust
            
            # Persist to source_performance_log
            await self._persist_performance(source, sub, trust, accuracy, prof, loss, quar_count)
            
        logger.info(f"{self.name}: Completed dynamic reliability assessment for {len(sources)} sources")

    async def _persist_performance(
        self, source: str, sub: str, trust: float, accuracy: float, 
        prof: int, loss: int, quar_count: int
    ):
        """Persist to new source_performance_log table."""
        await self.db._execute_insert(
            """
            INSERT INTO source_performance_log
                (id, source, source_sub, dynamic_trust_score, historical_accuracy,
                 n_profitable_signals, n_loss_signals, n_quarantined_signals, updated_at)
            VALUES
                (:id, :source, :sub, :trust, :acc, :prof, :loss, :quar, NOW())
            ON CONFLICT (id) DO NOTHING
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "source": source,
                "sub": sub,
                "trust": round(trust, 4),
                "acc": round(accuracy, 4),
                "prof": prof,
                "loss": loss,
                "quar": quar_count
            }
        )

    def get_weighted_score(self, source: str, source_sub: str, raw_score: float) -> float:
        """Adjust a score from a source by its dynamic reliability weight."""
        trust = self._trust_scores.get(f"{source}:{source_sub}", self._trust_scores.get(f"{source}:default", DEFAULT_TRUST_SCORES.get(source, 0.3)))
        return raw_score * trust
