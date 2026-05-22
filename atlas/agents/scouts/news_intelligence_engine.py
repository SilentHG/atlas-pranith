"""
news_intelligence_engine.py — External scout for macro event and news intelligence.

Capabilities:
- Macro event extraction and scoring
- Earnings event tracking
- Geopolitical risk scoring
- Economic regime analysis
"""

from __future__ import annotations

import json
import uuid
import re
import aiohttp
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent
from atlas.config.settings import settings


class NewsIntelligenceEngine(BaseAgent):
    """
    External Scout — News and macro event intelligence for informed strategy decisions.
    """

    name = "NewsIntelligenceEngine"
    agent_type = "news_scout"
    layer = "Scout"

    def __init__(self, redis_client, db_client):
        super().__init__(self.name, self.agent_type, self.layer, redis_client)
        self.db = db_client
        self._run_interval = 1800  # Every 30 minutes
        self._cache: dict = {}

    async def run(self):
        logger.info(f"{self.name}: Starting news intelligence monitoring")

        while self.status == "running":
            try:
                await self._gather_news_intelligence()
            except Exception as e:
                logger.error(f"{self.name}: News gathering error: {e}")

            for _ in range(self._run_interval // 10):
                await self._sleep(10)
                if self.status != "running":
                    return

    async def _sleep(self, seconds: int):
        import asyncio
        await asyncio.sleep(seconds)

    async def _gather_news_intelligence(self):
        """Gather and score news intelligence signals from public RSS."""
        signals = []
        
        # Free Yahoo Finance RSS feeds for major macro/crypto tickers
        url = "https://feeds.finance.yahoo.com/rss/2.0/headline?s=SPY,QQQ,TLT,GLD,BTC-USD"
        
        try:
            headers = {"User-Agent": "ATLAS-NewsScout/1.0"}
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(url, timeout=10) as resp:
                    if resp.status == 200:
                        content = await resp.text()
                        root = ET.fromstring(content)
                        
                        for item in root.findall('.//item')[:15]:
                            title = item.findtext('title', '')
                            desc = item.findtext('description', '')
                            pub_date = item.findtext('pubDate', '')
                            link = item.findtext('link', '')
                            
                            text = f"{title} {desc}"
                            tickers = self._extract_tickers(text)
                            sentiment = self._compute_sentiment(text)
                            
                            signals.append({
                                "source": "news_rss",
                                "source_sub": "yahoo_finance",
                                "signal_type": "macro_news" if not tickers else "asset_news",
                                "sentiment": sentiment,
                                "hypothesis_score": abs(sentiment) * 0.8 + 0.2, # Baseline
                                "signal_direction": "bullish" if sentiment > 0.1 else "bearish" if sentiment < -0.1 else "neutral",
                                "tickers": json.dumps(tickers),
                                "details": {
                                    "title": title,
                                    "link": link,
                                    "pub_date": pub_date
                                }
                            })
        except Exception as e:
            logger.debug(f"{self.name}: Error fetching RSS: {e}")

        # Persist all signals
        for signal in signals:
            await self._persist_signal(signal)

        if signals:
            logger.info(f"{self.name}: Published {len(signals)} news intelligence signals")

    def _extract_tickers(self, text: str) -> list[str]:
        """Extract uppercase tickers starting with $ or standalone uppercase words."""
        matches = re.findall(r'\$([A-Z]{2,6})\b|\b([A-Z]{3,5})\b', text)
        extracted = set()
        for m1, m2 in matches:
            if m1: extracted.add(m1)
            elif m2 and m2 not in ("THE", "AND", "FOR", "BUT", "HAS", "ARE", "NEW", "NOW"): 
                extracted.add(m2)
        return list(extracted)[:5]

    def _compute_sentiment(self, text: str) -> float:
        """A simple local heuristic sentiment for demonstration."""
        text_lower = text.lower()
        bull_words = ["surge", "jump", "record", "growth", "beat", "rally", "upgrade", "higher"]
        bear_words = ["plunge", "drop", "fear", "inflation", "miss", "crash", "downgrade", "lower", "risk"]
        
        bull_score = sum(text_lower.count(w) for w in bull_words)
        bear_score = sum(text_lower.count(w) for w in bear_words)
        
        total = bull_score + bear_score
        if total == 0:
            return 0.0
            
        return (bull_score - bear_score) / total

    def _is_earnings_season(self) -> bool:
        """Simple heuristic for earnings season detection."""
        import calendar
        now = datetime.now(timezone.utc)
        # Approximate earnings seasons: mid-Jan, mid-Apr, mid-Jul, mid-Oct
        earnings_months = {1, 4, 7, 10}
        return now.month in earnings_months and now.day >= 10

    async def _persist_signal(self, signal: dict):
        """Persist a news intelligence signal to the scout memory."""
        await self.db._execute_insert(
            """
            INSERT INTO external_scout_memory
                (id, source, source_sub, timestamp, sentiment,
                 hypothesis_score, signal_direction, mentioned_tickers, details)
            VALUES
                (:id, :source, :source_sub, NOW(), :sentiment,
                 :score, :direction, :tickers, CAST(:details AS jsonb))
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "source": signal["source"],
                "source_sub": signal.get("source_sub", ""),
                "sentiment": signal["sentiment"],
                "score": signal["hypothesis_score"],
                "direction": signal["signal_direction"],
                "tickers": signal.get("tickers", ""),
                "details": json.dumps(signal.get("details", {})),
            },
        )
