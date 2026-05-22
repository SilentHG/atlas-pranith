"""
copy_failover_manager.py — Phase 21F

Handles safe degraded operation during failures.
Watches leader states (trusted, monitored, degraded, suspended) and network/Redis health.
Applies degraded modes: frozen_follow, limited_follow, safe_unwind, observation_only.

Ensures followers stabilize safely rather than panic-trading during outages.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent


class CopyFailoverManager(BaseAgent):
    """L5 Agent — Manages degraded modes and safe failover for copy followers."""

    name = "CopyFailoverManager"
    agent_type = "copy_failover"
    layer = "L5"

    def __init__(self, redis_client, db_client):
        super().__init__(self.name, self.agent_type, self.layer, redis_client)
        self.db = db_client
        self._run_interval = 60  # Every 1 minute

    async def run(self):
        logger.info(f"{self.name}: Starting copy failover manager")
        while self.status == "running":
            try:
                await self._evaluate_system_failovers()
            except Exception as e:
                logger.error(f"{self.name}: Failover evaluation error: {e}")
            await asyncio.sleep(self._run_interval)

    async def _evaluate_system_failovers(self):
        """Evaluate and apply failover modes based on leader states and drift."""
        # Load all active followers and their leaders
        followers = await self._load_followers()
        for f in followers:
            try:
                await self._evaluate_follower(f)
            except Exception as e:
                logger.debug(f"{self.name}: Error evaluating follower {f['follower_id']}: {e}")

    async def _load_followers(self) -> list[dict]:
        try:
            async with self.db.engine.connect() as conn:
                r = await conn.execute(text("""
                    SELECT f.follower_id, f.leader_id, l.account_ref as leader_ref
                    FROM copy_follower_accounts f
                    JOIN copy_leader_accounts l ON l.leader_id = f.leader_id
                    WHERE f.is_active = TRUE
                """))
                return [
                    {"follower_id": str(row[0]), "leader_id": str(row[1]), "leader_ref": str(row[2])}
                    for row in r.fetchall()
                ]
        except Exception:
            return []

    async def _evaluate_follower(self, follower: dict):
        fid = follower["follower_id"]
        lid = follower["leader_id"]

        # 1. Check leader health from Redis (set by LeaderGovernanceEngine)
        state_bytes = await self.redis.get(f"copy_leader:{lid}:state")
        leader_state = state_bytes.decode("utf-8") if state_bytes else "trusted"

        # 2. Check recent drift severity
        drift_severity = await self._get_recent_drift(lid, fid)

        # 3. Determine new mode
        new_mode = "normal"
        trigger = None
        action = None

        if leader_state == "suspended":
            new_mode = "frozen_follow"
            trigger = "leader_suspended"
            action = "freeze_execution"
        elif leader_state == "degraded" or drift_severity == "critical_drift":
            new_mode = "safe_unwind"
            trigger = f"leader_{leader_state}_drift_{drift_severity}"
            action = "reduce_exposure"
        elif leader_state == "monitored" or drift_severity == "elevated_drift":
            new_mode = "limited_follow"
            trigger = "elevated_risk"
            action = "reduce_size"
        
        # 4. Check current mode and apply changes
        current_mode_bytes = await self.redis.get(f"copy_failover:{fid}:mode")
        current_mode = current_mode_bytes.decode("utf-8") if current_mode_bytes else "normal"

        if current_mode != new_mode:
            logger.info(
                f"{self.name}: Follower {fid} transitioning {current_mode} -> {new_mode} "
                f"({trigger})"
            )
            await self.redis.set(f"copy_failover:{fid}:mode", new_mode)
            
            trace_id = uuid.uuid4().hex[:16]
            await self.db._execute_insert(
                """
                INSERT INTO copy_failover_events
                    (id, trace_id, follower_id, leader_id, event_type,
                     previous_mode, new_mode, trigger_reason, recovery_action,
                     metadata, occurred_at)
                VALUES
                    (:id, :trace_id, :fid, :lid, 'mode_transition',
                     :prev, :new, :trigger, :action,
                     CAST(:meta AS jsonb), NOW())
                """,
                {
                    "id": uuid.uuid4().hex[:16],
                    "trace_id": trace_id,
                    "fid": fid,
                    "lid": lid,
                    "prev": current_mode,
                    "new": new_mode,
                    "trigger": trigger,
                    "action": action,
                    "meta": json.dumps({"agent": self.name})
                }
            )

    async def _get_recent_drift(self, leader_id: str, follower_id: str) -> str:
        """Get the most recent drift severity."""
        try:
            async with self.db.engine.connect() as conn:
                r = await conn.execute(text("""
                    SELECT drift_severity FROM copy_drift_log
                    WHERE leader_id = :lid AND follower_id = :fid
                    ORDER BY detected_at DESC LIMIT 1
                """), {"lid": leader_id, "fid": follower_id})
                row = r.fetchone()
                return str(row[0]) if row else "synchronized"
        except Exception:
            return "synchronized"
