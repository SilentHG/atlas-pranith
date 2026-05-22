"""competition_scout.py — Phase 12: External Scout Network.

Monitors trading/data science competitions for:
  - Emerging strategy patterns and techniques
  - Feature engineering innovations
  - Performance benchmarks
  - Meta-learning signals
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Optional

import numpy as np
from loguru import logger

from atlas.core.agent_base import BaseAgent


class CompetitionScout(BaseAgent):
    """External intelligence scout — competition monitoring."""

    name = "CompetitionScout"
    agent_type = "external_scout"
    layer = "L7"

    PLATFORM_RELIABILITY = {
        "kaggle_trading": 0.85,
        "numerai": 0.90,
        "quantopian_legacy": 0.70,
        "crowd_analytics": 0.65,
        "algo_competitions": 0.80,
    }

    def __init__(self, redis_client=None, db_client=None, run_interval: int = 7200):
        super().__init__(
            name=self.name,
            agent_type=self.agent_type,
            layer=self.layer,
            redis_client=redis_client,
        )
        self.db = db_client
        self.run_interval = run_interval

    async def run(self):
        logger.info(f"{self.name}: starting competition intelligence scout (every {self.run_interval}s)")
        while self.status == "running":
            try:
                signals = await self._scan_competitions()
                if signals:
                    ranked = self._rank_by_novelty(signals)
                    await self._persist_signals(ranked)
                    await self._publish_intelligence(ranked)
            except Exception as e:
                logger.error(f"{self.name}: cycle failed: {e}")
            await asyncio.sleep(self.run_interval)

    async def _scan_competitions(self) -> list[dict]:
        """Simulate scanning trading/data science competitions."""
        signals = []
        for platform, reliability in self.PLATFORM_RELIABILITY.items():
            for comp_idx in range(2):
                innovation_score = float(np.random.uniform(0.3, 1.0))
                signals.append({
                    "id": str(uuid.uuid4()),
                    "source": "competition",
                    "platform": platform,
                    "source_reliability": reliability,
                    "timestamp": datetime.now(timezone.utc),
                    "innovation_score": round(innovation_score, 4),
                    "feature_families": list(np.random.choice(
                        ["technical", "microstructure", "order_flow", "sentiment", "alternative", "correlation"],
                        int(np.random.poisson(2) + 1)
                    )),
                    "technique_category": np.random.choice(
                        ["ensemble", "deep_learning", "feature_engineering", "risk_modeling",
                         "execution_optimization", "portfolio_construction"]
                    ),
                    "top_performer_metric": round(float(np.random.uniform(0.5, 0.99)), 4),
                    "participants": int(np.random.poisson(500)),
                    "strategy_notes": f"Top strategies use {np.random.choice(['gradient_boosting', 'transformer', 'lstm', 'random_forest', 'linear_models'])}",
                })
        return signals

    def _rank_by_novelty(self, signals: list[dict]) -> list[dict]:
        for s in signals:
            s["hypothesis_score"] = round(
                s["source_reliability"] * 0.3
                + s["innovation_score"] * 0.4
                + s["top_performer_metric"] * 0.3,
                4,
            )
            s["signal_direction"] = "insight"
        signals.sort(key=lambda x: -x["hypothesis_score"])
        return signals

    async def _persist_signals(self, signals: list[dict]) -> None:
        if not self.db:
            return
        for sig in signals[:10]:
            try:
                await self.db._execute_insert(
                    """
                    INSERT INTO external_scout_memory
                        (id, source, source_sub, source_reliability,
                         timestamp, sentiment, mentioned_tickers,
                         hypothesis_score, signal_direction, metadata)
                    VALUES
                        (:id, :source, :source_sub, :source_reliability,
                         :timestamp, :sentiment, :mentioned_tickers,
                         :hypothesis_score, :signal_direction, :metadata)
                    """,
                    {
                        "id": sig["id"],
                        "source": sig["source"],
                        "source_sub": sig.get("platform", ""),
                        "source_reliability": sig["source_reliability"],
                        "timestamp": sig["timestamp"],
                        "sentiment": sig["innovation_score"],
                        "mentioned_tickers": json.dumps([]),
                        "hypothesis_score": sig["hypothesis_score"],
                        "signal_direction": sig["signal_direction"],
                        "metadata": json.dumps({k: v for k, v in sig.items()
                                                if k in ("feature_families", "technique_category",
                                                         "top_performer_metric", "participants",
                                                         "strategy_notes", "platform")}),
                    },
                )
            except Exception as e:
                logger.warning(f"{self.name}: persist failed: {e}")

    async def _publish_intelligence(self, signals: list[dict]) -> None:
        if not self._redis:
            return
        try:
            top = [{"source": "competition", "platform": s.get("platform", ""),
                    "innovation_score": s["innovation_score"], "score": s["hypothesis_score"],
                    "technique": s.get("technique_category", "")}
                   for s in signals[:3]]
            await self._redis.publish("external_scout_signals",
                                      json.dumps({"type": "competition_scout", "signals": top}))
        except Exception as e:
            logger.warning(f"{self.name}: publish failed: {e}")
