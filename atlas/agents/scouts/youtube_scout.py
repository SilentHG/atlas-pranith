"""youtube_scout.py — Phase 12: External Scout Network.

Monitors YouTube financial channels for:
  - Video title and description sentiment
  - Ticker/sector mentions
  - Popularity trends (views, engagement)
  - Strategy-relevant educational content
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Optional

import numpy as np
from loguru import logger

from atlas.core.agent_base import BaseAgent


class YouTubeScout(BaseAgent):
    """External intelligence scout — YouTube financial content monitoring."""

    name = "YouTubeScout"
    agent_type = "external_scout"
    layer = "L7"

    CHANNEL_RELIABILITY = {
        "financial_education": 0.75,
        "market_analysis": 0.65,
        "trading_strategy": 0.60,
        "crypto_analysis": 0.45,
        "stock_picks": 0.40,
    }

    def __init__(self, redis_client=None, db_client=None, run_interval: int = 900):
        super().__init__(
            name=self.name,
            agent_type=self.agent_type,
            layer=self.layer,
            redis_client=redis_client,
        )
        self.db = db_client
        self.run_interval = run_interval

    async def run(self):
        logger.info(f"{self.name}: starting YouTube intelligence scout (every {self.run_interval}s)")
        while self.status == "running":
            try:
                signals = await self._scan_channels()
                if signals:
                    ranked = self._rank_by_reliability(signals)
                    await self._persist_signals(ranked)
                    await self._publish_intelligence(ranked)
            except Exception as e:
                logger.error(f"{self.name}: cycle failed: {e}")
            await asyncio.sleep(self.run_interval)

    async def _scan_channels(self) -> list[dict]:
        """Simulate scanning YouTube financial channels."""
        signals = []
        for channel, reliability in self.CHANNEL_RELIABILITY.items():
            for video_idx in range(2):
                sentiment = float(np.random.uniform(-1, 1))
                signals.append({
                    "id": str(uuid.uuid4()),
                    "source": "youtube",
                    "channel": channel,
                    "source_reliability": reliability,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "title_sentiment": round(sentiment, 4),
                    "description_sentiment": round(sentiment * float(np.random.uniform(0.8, 1.2)), 4),
                    "view_count": int(np.random.poisson(50000)),
                    "engagement_rate": float(np.random.uniform(0.02, 0.15)),
                    "mentioned_tickers": [{"ticker": t, "sentiment": round(sentiment * np.random.uniform(0.5, 1.0), 4)}
                                          for t in np.random.choice(["BTC", "ETH", "AAPL", "TSLA", "NVDA", "SPY"], 2)],
                    "video_type": np.random.choice(["analysis", "education", "news", "opinion"]),
                })
        return signals

    def _rank_by_reliability(self, signals: list[dict]) -> list[dict]:
        """Rank signals by source reliability and engagement."""
        for s in signals:
            s["hypothesis_score"] = round(
                s["source_reliability"] * 0.4
                + abs(s["title_sentiment"]) * 0.2
                + min(1.0, s["engagement_rate"] * 5) * 0.2
                + min(1.0, s["view_count"] / 100000) * 0.2,
                4,
            )
            s["signal_direction"] = "bullish" if s["title_sentiment"] > 0.3 else ("bearish" if s["title_sentiment"] < -0.3 else "neutral")
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
                        "source_sub": sig.get("channel", ""),
                        "source_reliability": sig["source_reliability"],
                        "timestamp": sig["timestamp"],
                        "sentiment": sig.get("title_sentiment", 0),
                        "mentioned_tickers": json.dumps(sig.get("mentioned_tickers", [])),
                        "hypothesis_score": sig["hypothesis_score"],
                        "signal_direction": sig["signal_direction"],
                        "metadata": json.dumps({k: v for k, v in sig.items()
                                                if k in ("view_count", "engagement_rate", "video_type", "channel")}),
                    },
                )
            except Exception as e:
                logger.warning(f"{self.name}: persist failed: {e}")

    async def _publish_intelligence(self, signals: list[dict]) -> None:
        if not self._redis:
            return
        try:
            top = [{"source": "youtube", "channel": s.get("channel", ""), "direction": s["signal_direction"],
                    "sentiment": s.get("title_sentiment", 0), "score": s["hypothesis_score"],
                    "tickers": [t["ticker"] for t in s.get("mentioned_tickers", [])[:3]]}
                   for s in signals[:3]]
            await self._redis.publish("external_scout_signals",
                                      json.dumps({"type": "youtube_scout", "signals": top}))
        except Exception as e:
            logger.warning(f"{self.name}: publish failed: {e}")
