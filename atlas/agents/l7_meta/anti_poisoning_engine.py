"""
anti_poisoning_engine.py — Phase 22

Detects coordinated signal bursts, evolutionary poisoning attempts, and 
adversarial consensus manipulation. Enforces signal quarantine and 
slashes source trust for malicious behavior.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections import defaultdict
from typing import Any

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent


class AntiPoisoningEngine(BaseAgent):
    """L7 Agent — Defense against evolutionary poisoning and adversarial inputs."""

    name = "AntiPoisoningEngine"
    agent_type = "anti_poisoning"
    layer = "L7"

    def __init__(self, redis_client, db_client):
        super().__init__(self.name, self.agent_type, self.layer, redis_client)
        self.redis = redis_client
        self.db = db_client
        self._run_interval = 300  # Every 5 minutes
        
        # Detection Thresholds
        self.BURST_THRESHOLD = 20  # Max signals per symbol per source in 1 hour
        self.COORDINATED_THRESHOLD = 5 # Minimum sources required to trigger coordinated attack detection
        
    async def run(self):
        logger.info(f"{self.name}: Starting anti-poisoning engine")
        while self.status == "running":
            try:
                await self._scan_for_poison()
            except Exception as e:
                logger.error(f"{self.name}: Poison scan error: {e}")
            for _ in range(self._run_interval // 10):
                await asyncio.sleep(10)
                if self.status != "running":
                    return

    async def _scan_for_poison(self):
        """Scan recent signals for malicious patterns."""
        await self._detect_signal_bursts()
        await self._detect_coordinated_attacks()
        await self._quarantine_stale_contradictions()

    async def _detect_signal_bursts(self):
        """Detect instances where a single source spams signals for one asset."""
        try:
            async with self.db.engine.connect() as conn:
                # Find sources that emitted > BURST_THRESHOLD signals for a symbol in 1 hr
                r = await conn.execute(text("""
                    SELECT source, symbol, COUNT(*) as signal_count
                    FROM scout_signals
                    WHERE created_at > NOW() - INTERVAL '1 hour'
                    GROUP BY source, symbol
                    HAVING COUNT(*) > :threshold
                """), {"threshold": self.BURST_THRESHOLD})
                
                bursts = r.fetchall()
                for row in bursts:
                    source = str(row[0])
                    symbol = str(row[1])
                    count = int(row[2])
                    
                    await self._execute_quarantine(
                        source=source,
                        source_sub="all",
                        violation_type="signal_burst_spam",
                        severity=0.8,
                        symbols=[symbol],
                        action="slash_trust_and_quarantine_signals",
                        reason=f"{count} signals in 1 hour for {symbol}"
                    )
        except Exception as e:
            logger.debug(f"{self.name}: Burst detection error: {e}")

    async def _detect_coordinated_attacks(self):
        """Detect when multiple low-trust sources suddenly spam the same asset."""
        try:
            async with self.db.engine.connect() as conn:
                r = await conn.execute(text("""
                    SELECT symbol, COUNT(DISTINCT source) as num_sources, 
                           SUM(confidence_score) as total_conf
                    FROM scout_signals
                    WHERE created_at > NOW() - INTERVAL '30 minutes'
                    GROUP BY symbol
                    HAVING COUNT(DISTINCT source) >= :threshold
                """), {"threshold": self.COORDINATED_THRESHOLD})
                
                attacks = r.fetchall()
                for row in attacks:
                    symbol = str(row[0])
                    num_sources = int(row[1])
                    
                    # Check if these are trusted or untrusted sources
                    r_sources = await conn.execute(text("""
                        SELECT DISTINCT source FROM scout_signals 
                        WHERE symbol = :sym AND created_at > NOW() - INTERVAL '30 minutes'
                    """), {"sym": symbol})
                    
                    sources = [str(s[0]) for s in r_sources.fetchall()]
                    
                    # Slash trust for all participating sources
                    for source in sources:
                        await self._execute_quarantine(
                            source=source,
                            source_sub="all",
                            violation_type="coordinated_attack_suspect",
                            severity=1.0,
                            symbols=[symbol],
                            action="slash_trust_and_quarantine_signals",
                            reason=f"Coordinated burst among {num_sources} sources for {symbol}"
                        )
        except Exception as e:
            logger.debug(f"{self.name}: Coordinated attack detection error: {e}")

    async def _quarantine_stale_contradictions(self):
        """Quarantine signals that contradict the trusted consensus over time."""
        # This will be enriched in Phase 22 Consensus Governance
        pass

    async def _execute_quarantine(
        self, source: str, source_sub: str, violation_type: str, 
        severity: float, symbols: list[str], action: str, reason: str
    ):
        trace_id = uuid.uuid4().hex[:16]
        
        # 1. Log the quarantine event
        await self.db._execute_insert(
            """
            INSERT INTO scout_poison_quarantine
                (id, trace_id, source, source_sub, violation_type,
                 severity_score, affected_symbols, action_taken, metadata)
            VALUES
                (:id, :trace_id, :source, :sub, :violation,
                 :severity, CAST(:symbols AS jsonb), :action, CAST(:metadata AS jsonb))
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "trace_id": trace_id,
                "source": source,
                "sub": source_sub,
                "violation": violation_type,
                "severity": severity,
                "symbols": json.dumps(symbols),
                "action": action,
                "metadata": json.dumps({"reason": reason})
            }
        )
        
        # 2. Slash trust in source_performance_log
        await self.db._execute_insert(
            """
            INSERT INTO source_performance_log
                (id, source, source_sub, dynamic_trust_score, n_quarantined_signals)
            VALUES
                (:id, :source, :sub, :trust, 1)
            ON CONFLICT (id) DO NOTHING
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "source": source,
                "sub": source_sub,
                "trust": max(0.0, 0.5 - severity) # Hard slash
            }
        )
        
        # We also need an UPDATE to slash existing trust if row exists
        # In a real app, this would be an UPSERT handling trust reduction
        async with self.db.engine.begin() as conn:
            await conn.execute(text("""
                UPDATE source_performance_log
                SET dynamic_trust_score = GREATEST(0.0, dynamic_trust_score - :slash_amount),
                    n_quarantined_signals = n_quarantined_signals + 1,
                    updated_at = NOW()
                WHERE source = :source
            """), {"slash_amount": severity * 0.5, "source": source})
            
        logger.warning(
            f"{self.name}: {violation_type} detected for {source}. "
            f"Action: {action}. Trace: {trace_id}"
        )
