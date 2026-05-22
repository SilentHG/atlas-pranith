"""podcast_scout.py — Phase 12: External Scout Network.

Monitors financial podcast content for:
  - Episode title and description sentiment
  - Sector and ticker mentions
  - Topic trend analysis
  - Guest expert sentiment (reliable sources)
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Optional

import numpy as np
from loguru import logger

from atlas.core.agent_base import BaseAgent


class PodcastScout(BaseAgent):
    """External intelligence scout — financial podcast monitoring."""

    name = "PodcastScout"
    agent_type = "external_scout"
    layer = "L7"

    PODCAST_RELIABILITY = {
        "macro_market_analysis": 0.80,
        "quant_strategies": 0.85,
        "crypto_insights": 0.60,
        "options_education": 0.70,
        "stock_research": 0.65,
    }

    def __init__(self, redis_client=None, db_client=None, run_interval: int = 3600):
        super().__init__(
            name=self.name,
            agent_type=self.agent_type,
            layer=self.layer,
            redis_client=redis_client,
        )
        self.db = db_client
        self.run_interval = run_interval

    async def run(self):
        logger.info(f"{self.name}: starting podcast intelligence scout (every {self.run_interval}s)")
        while self.status == "running":
            try:
                episodes = await self._scan_episodes()
                if episodes:
                    ranked = self._rank_episodes(episodes)
                    await self._persist_signals(ranked)
                    await self._publish_intelligence(ranked)
            except Exception as e:
                logger.error(f"{self.name}: cycle failed: {e}")
            await asyncio.sleep(self.run_interval)

    async def _scan_episodes(self) -> list[dict]:
        """Simulate scanning podcast episodes."""
        signals = []
        for podcast, reliability in self.PODCAST_RELIABILITY.items():
            for ep_idx in range(2):
                sentiment = float(np.random.uniform(-1, 1))
                signals.append({
                    "id": str(uuid.uuid4()),
                    "source": "podcast",
                    "podcast": podcast,
                    "source_reliability": reliability,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "episode_sentiment": round(sentiment, 4),
                    "mentioned_tickers": [{"ticker": t, "sentiment": round(sentiment * np.random.uniform(0.5, 1.2), 4)}
                                          for t in np.random.choice(["SPY", "QQQ", "GLD", "TLT", "BTC", "AAPL", "MSFT", "NVDA"],
                                                                    int(np.random.poisson(2) + 1))],
                    "episode_topic": np.random.choice(["market_outlook", "strategy_deep_dive", "sector_analysis",
                                                       "interview", "economic_outlook"]),
                    "guest_credibility": float(np.random.uniform(0.3, 0.9)),
                })
        return signals

    def _rank_episodes(self, episodes: list[dict]) -> list[dict]:
        for e in episodes:
            e["hypothesis_score"] = round(
                e["source_reliability"] * 0.3
                + e["guest_credibility"] * 0.3
                + abs(e["episode_sentiment"]) * 0.2
                + min(1.0, len(e["mentioned_tickers"]) / 5) * 0.2,
                4,
            )
            e["signal_direction"] = "bullish" if e["episode_sentiment"] > 0.3 else ("bearish" if e["episode_sentiment"] < -0.3 else "neutral")
        episodes.sort(key=lambda x: -x["hypothesis_score"])
        return episodes

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
                        "source_sub": sig.get("podcast", ""),
                        "source_reliability": sig["source_reliability"],
                        "timestamp": sig["timestamp"],
                        "sentiment": sig["episode_sentiment"],
                        "mentioned_tickers": json.dumps(sig.get("mentioned_tickers", [])),
                        "hypothesis_score": sig["hypothesis_score"],
                        "signal_direction": sig["signal_direction"],
                        "metadata": json.dumps({k: v for k, v in sig.items()
                                                if k in ("episode_topic", "guest_credibility", "podcast")}),
                    },
                )
            except Exception as e:
                logger.warning(f"{self.name}: persist failed: {e}")

    async def _publish_intelligence(self, signals: list[dict]) -> None:
        if not self._redis:
            return
        try:
            top = [{"source": "podcast", "podcast": s.get("podcast", ""), "direction": s["signal_direction"],
                    "sentiment": s["episode_sentiment"], "score": s["hypothesis_score"],
                    "tickers": [t["ticker"] for t in s.get("mentioned_tickers", [])[:3]]}
                   for s in signals[:3]]
            await self._redis.publish("external_scout_signals",
                                      json.dumps({"type": "podcast_scout", "signals": top}))
        except Exception as e:
            logger.warning(f"{self.name}: publish failed: {e}")
