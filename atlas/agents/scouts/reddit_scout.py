"""reddit_scout.py — Phase 12: External Scout Network.

Monitors Reddit for:
  - Sentiment analysis on financial subreddits
  - Trending ticker mentions
  - Strategy-relevant discussion patterns
  - Crowd intelligence signals

Implements:
  - Source reliability scoring (subreddit trust weighting)
  - Hypothesis ranking (which signals matter most)
  - Automatic validation routing (feeds into pattern engine)
  - Scout memory persistence (avoids re-analyzing same posts)
"""

import asyncio
import json
import uuid
import re
import aiohttp
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Optional

from loguru import logger

from atlas.core.agent_base import BaseAgent


class RedditScout(BaseAgent):
    """External intelligence scout — Reddit sentiment and ticker monitoring."""

    name = "RedditScout"
    agent_type = "external_scout"
    layer = "L7"

    # Source reliability scores per subreddit
    SUBREDDIT_RELIABILITY = {
        "wallstreetbets": 0.4,
        "stocks": 0.6,
        "investing": 0.7,
        "algotrading": 0.8,
        "cryptocurrency": 0.5,
        "cryptomarkets": 0.5,
        "CryptoCurrency": 0.5,
        "options": 0.7,
        "securityanalysis": 0.75,
        "quant": 0.85,
    }

    def __init__(self, redis_client=None, db_client=None, run_interval: int = 600):
        super().__init__(
            name=self.name,
            agent_type=self.agent_type,
            layer=self.layer,
            redis_client=redis_client,
        )
        self.db = db_client
        self.run_interval = run_interval
        self._memory_cache: dict[str, datetime] = {}  # Track seen post IDs with timestamps
        self._cache_ttl = timedelta(hours=24)

    async def run(self):
        logger.info(f"{self.name}: starting Reddit intelligence scout (every {self.run_interval}s)")
        while self.status == "running":
            try:
                signals = await self._gather_signals()
                if signals:
                    ranked = self._rank_hypotheses(signals)
                    await self._persist_signals(ranked)
                    await self._publish_scout_intelligence(ranked)
            except Exception as e:
                logger.error(f"{self.name}: cycle failed: {e}")
            await asyncio.sleep(self.run_interval)

    async def _gather_signals(self) -> list[dict]:
        """Real Reddit data gathering via JSON endpoints."""
        signals = []
        headers = {"User-Agent": "ATLAS-Agent/1.0"}
        
        # Prune memory cache (Priority 1)
        now = datetime.now(timezone.utc)
        to_remove = [k for k, ts in self._memory_cache.items() if now - ts > self._cache_ttl]
        for k in to_remove:
            del self._memory_cache[k]

        async with aiohttp.ClientSession(headers=headers) as session:
            for subreddit, reliability in self.SUBREDDIT_RELIABILITY.items():
                try:
                    url = f"https://www.reddit.com/r/{subreddit}/hot.json?limit=15"
                    async with session.get(url, timeout=10) as resp:
                        if resp.status != 200:
                            logger.debug(f"{self.name}: Failed to fetch r/{subreddit}: HTTP {resp.status}")
                            continue
                        data = await resp.json()
                        posts = data.get("data", {}).get("children", [])
                        
                        for post in posts:
                            pdata = post.get("data", {})
                            post_id = pdata.get("id")
                            if not post_id or post_id in self._memory_cache:
                                continue
                                
                            self._memory_cache[post_id] = now
                            
                            title = pdata.get("title", "")
                            selftext = pdata.get("selftext", "")
                            score = pdata.get("score", 0)
                            
                            # Simple NLP extraction proxy
                            text = f"{title} {selftext}"
                            tickers = self._extract_tickers_from_text(text)
                            if not tickers:
                                continue
                                
                            sentiment = self._compute_basic_sentiment(text)
                            
                            signals.append({
                                "id": str(uuid.uuid4()),
                                "source": "reddit",
                                "subreddit": subreddit,
                                "source_reliability": reliability,
                                "timestamp": datetime.now(timezone.utc).isoformat(),
                                "sentiment": round(sentiment, 4),
                                "mentioned_tickers": tickers,
                                "post_type": "discussion" if abs(sentiment) < 0.5 else "sentiment_call",
                                "metadata": {
                                    "subreddit": subreddit,
                                    "post_id": post_id,
                                    "score": score,
                                    "author": pdata.get("author", "unknown"),
                                    "upvote_ratio": pdata.get("upvote_ratio", 0.0)
                                },
                            })
                except Exception as e:
                    logger.debug(f"{self.name}: Error fetching r/{subreddit}: {e}")
                    
                await asyncio.sleep(1) # Rate limit protection

        return signals

    def _extract_tickers_from_text(self, text: str) -> list[dict]:
        """Extract uppercase tickers starting with $ or standalone uppercase words."""
        # This is a naive regex for extraction, can be replaced by real NLP
        matches = re.findall(r'\$([A-Z]{2,6})\b|\b([A-Z]{3,5})\b', text)
        extracted = set()
        for m1, m2 in matches:
            if m1: extracted.add(m1)
            elif m2 and m2 not in ("THE", "AND", "FOR", "BUT", "HAS", "ARE"): 
                extracted.add(m2)
                
        # Heuristic filtering
        mentioned = []
        for t in list(extracted)[:5]: # Limit to 5 per post
            mentioned.append({
                "ticker": t,
                "sentiment": 0.0, # Sentiment will be applied per post later
                "mention_count": text.count(t)
            })
        return mentioned

    def _compute_basic_sentiment(self, text: str) -> float:
        """A simple local heuristic sentiment for demonstration."""
        text_lower = text.lower()
        bull_words = ["bull", "call", "calls", "moon", "buy", "long", "undervalued", "breakout"]
        bear_words = ["bear", "put", "puts", "crash", "sell", "short", "overvalued", "dump"]
        
        bull_score = sum(text_lower.count(w) for w in bull_words)
        bear_score = sum(text_lower.count(w) for w in bear_words)
        
        total = bull_score + bear_score
        if total == 0:
            return 0.0
            
        # Normalize to [-1.0, 1.0]
        return (bull_score - bear_score) / total

    def _rank_hypotheses(self, signals: list[dict]) -> list[dict]:
        """Rank gathered signals by source reliability and signal strength."""
        for signal in signals:
            reliability = signal["source_reliability"]
            abs_sentiment = abs(signal["sentiment"])
            ticker_count = len(signal["mentioned_tickers"])

            # Composite score: higher reliability + stronger sentiment = higher priority
            signal["hypothesis_score"] = round(
                reliability * 0.4 + abs_sentiment * 0.3 + min(1.0, ticker_count / 10) * 0.3,
                4,
            )

            # Signal direction
            signal["signal_direction"] = (
                "bullish" if signal["sentiment"] > 0.3
                else "bearish" if signal["sentiment"] < -0.3
                else "neutral"
            )

        signals.sort(key=lambda x: -x["hypothesis_score"])
        return signals

    async def _persist_signals(self, signals: list[dict]) -> None:
        """Persist scout intelligence to external_scout_memory."""
        if not self.db or not signals:
            return
        for sig in signals[:20]:  # Top 20
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
                        "source_sub": sig.get("subreddit", ""),
                        "source_reliability": sig["source_reliability"],
                        "timestamp": sig["timestamp"],
                        "sentiment": sig["sentiment"],
                        "mentioned_tickers": json.dumps(sig["mentioned_tickers"]),
                        "hypothesis_score": sig["hypothesis_score"],
                        "signal_direction": sig["signal_direction"],
                        "metadata": json.dumps(sig.get("metadata", {})),
                    },
                )
            except Exception as e:
                logger.warning(f"{self.name}: persist signal failed: {e}")

    async def _publish_scout_intelligence(self, ranked_signals: list[dict]) -> None:
        """Publish top signals to Redis."""
        if not self._redis:
            return
        try:
            top = [
                {
                    "source": s["source"],
                    "subreddit": s.get("subreddit", ""),
                    "reliability": s["source_reliability"],
                    "sentiment": s["sentiment"],
                    "direction": s["signal_direction"],
                    "score": s["hypothesis_score"],
                    "tickers": [t["ticker"] for t in s["mentioned_tickers"][:3]],
                }
                for s in ranked_signals[:5]
            ]
            await self._redis.publish(
                "external_scout_signals",
                json.dumps({"type": "reddit_scout", "signals": top, "n_signals": len(ranked_signals)}),
            )
        except Exception as e:
            logger.warning(f"{self.name}: publish failed: {e}")
