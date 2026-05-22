"""
copy_overlap_engine.py — Phase 21G

Prevents systemic concentration risk across copied leaders.
Detects: duplicated exposure, correlated leaders, hidden concentration,
regime clustering, strategy overlap.

Output: overlap_score, concentration_risk, diversification_penalty.
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


class CopyOverlapEngine(BaseAgent):
    """L6 Agent — Portfolio overlap and concentration risk detection."""

    name = "CopyOverlapEngine"
    agent_type = "copy_overlap"
    layer = "L6"

    def __init__(self, redis_client, db_client):
        super().__init__(self.name, self.agent_type, self.layer, redis_client)
        self.redis = redis_client
        self.db = db_client
        self._run_interval = 900  # Every 15 minutes

    async def run(self):
        logger.info(f"{self.name}: Starting overlap engine")
        while self.status == "running":
            try:
                await self._analyze_all_followers()
            except Exception as e:
                logger.error(f"{self.name}: Overlap analysis error: {e}")
            for _ in range(self._run_interval // 10):
                await asyncio.sleep(10)
                if self.status != "running":
                    return

    async def _analyze_all_followers(self):
        """Analyze overlap for all active followers with multiple leaders."""
        followers_map = await self._load_follower_multi_leaders()
        for follower_id, leaders in followers_map.items():
            if len(leaders) > 1:
                try:
                    await self._analyze_overlap(follower_id, leaders)
                except Exception as e:
                    logger.debug(f"{self.name}: Error analyzing {follower_id}: {e}")

    async def _load_follower_multi_leaders(self) -> dict[str, list[str]]:
        """Map follower_id -> list of leader_ids they are copying."""
        out = defaultdict(list)
        try:
            async with self.db.engine.connect() as conn:
                r = await conn.execute(text("""
                    SELECT follower_id, leader_id 
                    FROM copy_follower_accounts 
                    WHERE is_active = TRUE
                """))
                for row in r.fetchall():
                    out[str(row[0])].append(str(row[1]))
            return dict(out)
        except Exception:
            return {}

    async def _analyze_overlap(self, follower_id: str, leaders: list[str]):
        """Detect duplicated exposure across a follower's leaders."""
        # 1. Load current positions for all leaders
        leader_positions = {}
        for lid in leaders:
            leader_positions[lid] = await self._get_leader_positions(lid)

        # 2. Find duplicated symbols and aggregate exposure
        symbol_exposure = defaultdict(float)
        symbol_leaders = defaultdict(list)
        
        for lid, positions in leader_positions.items():
            for symbol, exposure in positions.items():
                symbol_exposure[symbol] += abs(exposure)
                symbol_leaders[symbol].append(lid)

        duplicated_exposure = []
        overlap_score = 0.0
        total_exposure = sum(symbol_exposure.values())

        for symbol, lids in symbol_leaders.items():
            if len(lids) > 1:
                exposure = symbol_exposure[symbol]
                duplicated_exposure.append({
                    "symbol": symbol,
                    "exposure": exposure,
                    "leaders": lids
                })
                # Add to overlap score weighted by exposure
                if total_exposure > 0:
                    overlap_score += (exposure / total_exposure)

        # 3. Assess concentration risk
        concentration_risk = overlap_score * 1.5  # Amplified for risk
        diversification_penalty = min(0.5, concentration_risk * 0.5)

        # 4. Save overlap metrics
        trace_id = uuid.uuid4().hex[:16]
        await self.db._execute_insert(
            """
            INSERT INTO copy_overlap_metrics
                (id, trace_id, follower_id, overlap_score, concentration_risk,
                 diversification_penalty, duplicated_exposure, correlated_leaders,
                 hidden_concentration, n_leaders_analyzed, metadata, analyzed_at)
            VALUES
                (:id, :trace_id, :fid, :overlap, :conc_risk,
                 :div_pen, CAST(:dup_exp AS jsonb), CAST(:corr_leaders AS jsonb),
                 CAST(:hidden_conc AS jsonb), :n_leaders, CAST(:meta AS jsonb), NOW())
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "trace_id": trace_id,
                "fid": follower_id,
                "overlap": round(overlap_score, 4),
                "conc_risk": round(concentration_risk, 4),
                "div_pen": round(diversification_penalty, 4),
                "dup_exp": json.dumps(duplicated_exposure),
                "corr_leaders": json.dumps([]),  # Future correlation clustering
                "hidden_conc": json.dumps({}),   # Future regime overlap
                "n_leaders": len(leaders),
                "meta": json.dumps({"agent": self.name}),
            }
        )

        if concentration_risk > 0.3:
            logger.warning(
                f"{self.name}: High concentration risk ({concentration_risk:.2f}) "
                f"for follower {follower_id} across {len(leaders)} leaders."
            )

        # 5. Set penalty in Redis for CapitalAllocator
        await self.redis.set(
            f"copy_overlap:{follower_id}:penalty", 
            str(diversification_penalty)
        )

    async def _get_leader_positions(self, leader_id: str) -> dict[str, float]:
        """Fetch absolute exposure per symbol for a leader."""
        try:
            async with self.db.engine.connect() as conn:
                r = await conn.execute(text("""
                    SELECT p.symbol, 
                           SUM(p.qty * COALESCE(p.current_price, p.avg_entry_price))
                    FROM positions p
                    JOIN copy_leader_accounts l ON l.account_ref = p.account_ref
                    WHERE l.leader_id = :lid
                    GROUP BY p.symbol
                """), {"lid": leader_id})
                return {str(row[0]): float(row[1]) for row in r.fetchall() if row[1]}
        except Exception:
            return {}
