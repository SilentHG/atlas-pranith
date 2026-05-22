"""discord_scout.py — Phase 12: External Scout Network.

Monitors Discord trading communities for:
  - Message sentiment analysis
  - Ticker/strategy mentions
  - Community sentiment shifts
  - Emerging topics and discussions
"""

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Optional

import numpy as np
from loguru import logger

from atlas.core.agent_base import BaseAgent


class DiscordScout(BaseAgent):
    """External intelligence scout — Discord community monitoring."""

    name = "DiscordScout"
    agent_type = "external_scout"
    layer = "L7"

    SERVER_RELIABILITY = {
        "professional_traders": 0.75,
        "quantitative_finance": 0.80,
        "crypto_trading": 0.45,
        "options_flow": 0.65,
        "algorithmic_trading": 0.85,
    }

    def __init__(self, redis_client=None, db_client=None, run_interval: int = 300):
        super().__init__(
            name=self.name,
            agent_type=self.agent_type,
            layer=self.layer,
            redis_client=redis_client,
        )
        self.db = db_client
        self.run_interval = run_interval

    async def run(self):
        logger.info(f"{self.name}: starting Discord intelligence scout (every {self.run_interval}s)")
        while self.status == "running":
            try:
                messages = await self._process_messages()
                if messages:
                    ranked = self._rank_signals(messages)
                    await self._persist_signals(ranked)
                    await self._publish_intelligence(ranked)
            except Exception as e:
                logger.error(f"{self.name}: cycle failed: {e}")
            await asyncio.sleep(self.run_interval)

    async def _process_messages(self) -> list[dict]:
        """Simulate Discord message processing."""
        signals = []
        for server, reliability in self.SERVER_RELIABILITY.items():
            for msg_idx in range(5):
                sentiment = float(np.random.uniform(-1, 1))
                signals.append({
                    "id": str(uuid.uuid4()),
                    "source": "discord",
                    "server": server,
                    "source_reliability": reliability,
                    "timestamp": datetime.now(timezone.utc),
                    "sentiment": round(sentiment, 4),
                    "message_length": int(np.random.poisson(200)),
                    "mentioned_tickers": [{"ticker": t, "sentiment": round(sentiment * np.random.uniform(0.5, 1.2), 4)}
                                          for t in np.random.choice(["BTC", "ETH", "AAPL", "NVDA", "SPY", "TSLA"], int(np.random.poisson(1.5) + 1))],
                    "message_type": np.random.choice(["discussion", "alert", "question", "analysis"]),
                    "reactions": int(np.random.poisson(3)),
                })
        return signals

    def _rank_signals(self, messages: list[dict]) -> list[dict]:
        for m in messages:
            m["hypothesis_score"] = round(
                m["source_reliability"] * 0.5
                + abs(m["sentiment"]) * 0.2
                + min(1.0, m["reactions"] / 10) * 0.3,
                4,
            )
            m["signal_direction"] = "bullish" if m["sentiment"] > 0.3 else ("bearish" if m["sentiment"] < -0.3 else "neutral")
        messages.sort(key=lambda x: -x["hypothesis_score"])
        return messages

    async def _persist_signals(self, signals: list[dict]) -> None:
        if not self.db:
            return
        for sig in signals[:15]:
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
                        "source_sub": sig.get("server", ""),
                        "source_reliability": sig["source_reliability"],
                        "timestamp": sig["timestamp"],
                        "sentiment": sig["sentiment"],
                        "mentioned_tickers": json.dumps(sig.get("mentioned_tickers", [])),
                        "hypothesis_score": sig["hypothesis_score"],
                        "signal_direction": sig["signal_direction"],
                        "metadata": json.dumps({k: v for k, v in sig.items()
                                                if k in ("message_length", "message_type", "reactions", "server")}),
                    },
                )
            except Exception as e:
                logger.warning(f"{self.name}: persist failed: {e}")

    async def _publish_intelligence(self, signals: list[dict]) -> None:
        if not self._redis:
            return
        try:
            top = [{"source": "discord", "server": s.get("server", ""), "direction": s["signal_direction"],
                    "sentiment": s["sentiment"], "score": s["hypothesis_score"],
                    "tickers": [t["ticker"] for t in s.get("mentioned_tickers", [])[:3]]}
                   for s in signals[:5]]
            await self._redis.publish("external_scout_signals",
                                      json.dumps({"type": "discord_scout", "signals": top}))
        except Exception as e:
            logger.warning(f"{self.name}: publish failed: {e}")
