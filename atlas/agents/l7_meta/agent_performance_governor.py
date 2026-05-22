"""
agent_performance_governor.py — L7 Meta Agent for agent performance governance.

Capabilities:
- Agent scoring (performance, reliability, contribution)
- Agent survivability tracking
- Agent drift detection
- Automatic throttling and restart escalation
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent


class AgentPerformanceGovernor(BaseAgent):
    """
    L7 Meta Agent — Governs agent performance across the ATLAS ecosystem.
    """

    name = "AgentPerformanceGovernor"
    agent_type = "agent_governor"
    layer = "L7"

    def __init__(self, redis_client, db_client):
        super().__init__(self.name, self.agent_type, self.layer, redis_client)
        self.db = db_client
        self._run_interval = 600  # Every 10 minutes

        # Throttle trackers
        self._throttled_agents: dict[str, float] = {}  # agent_name -> blocked_until

    async def run(self):
        logger.info(f"{self.name}: Starting agent performance governance")

        while self.status == "running":
            try:
                await self._govern_agents()
            except Exception as e:
                logger.error(f"{self.name}: Governance error: {e}")

            for _ in range(self._run_interval // 10):
                await self._sleep(10)
                if self.status != "running":
                    return

    async def _sleep(self, seconds: int):
        import asyncio
        await asyncio.sleep(seconds)

    async def _govern_agents(self):
        """Govern all registered agents."""
        agents = await self._load_agent_stats()
        if not agents:
            return

        scores = {}
        for agent in agents:
            score = self._score_agent(agent)
            scores[agent["name"]] = score

            if score < 0.3:
                await self._throttle_agent(agent["name"], "low_score")

            if agent.get("error_count", 0) > 10:
                await self._restart_agent(agent["name"], "too_many_errors")

        # Persist governance snapshot
        await self.db._execute_insert(
            """
            INSERT INTO agent_governance_state
                (id, assessed_at, n_agents_assessed,
                 agent_scores, throttled_agents)
            VALUES
                (:id, NOW(), :n_agents,
                 CAST(:scores AS jsonb), CAST(:throttled AS jsonb))
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "n_agents": len(agents),
                "scores": json.dumps(scores),
                "throttled": json.dumps(list(self._throttled_agents.keys())),
            },
        )

        logger.info(
            f"{self.name}: Governance sweep — {len(agents)} agents, "
            f"{len(self._throttled_agents)} throttled"
        )

    async def _load_agent_stats(self) -> list[dict]:
        """Load stats for all registered agents from Redis heartbeats."""
        registered = await self._load_from_registry()
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT actor, COUNT(*) as event_count,
                           COUNT(CASE WHEN status = 'error' THEN 1 END) as error_count,
                           MAX(created_at) as last_active
                    FROM lifecycle_events
                    WHERE created_at > NOW() - INTERVAL '24 hours'
                    GROUP BY actor
                    ORDER BY event_count DESC
                """)
            )
            lifecycle = {
                str(row[0]): {
                    "event_count": row[1],
                    "error_count": row[2],
                    "last_active": row[3].isoformat() if hasattr(row[3], "isoformat") else str(row[3]),
                }
                for row in r.fetchall()
            }

        agents = []
        for agent_name in registered:
            lc = lifecycle.get(agent_name, {})
            agents.append({
                "name": agent_name,
                "event_count": lc.get("event_count", 0),
                "error_count": lc.get("error_count", 0),
                "last_active": lc.get("last_active", ""),
            })
        return agents

    async def _load_from_registry(self) -> list[str]:
        """Load registered agent names from Redis."""
        keys = await self._redis.keys("agent:*")
        agents = []
        for key in keys:
            data = await self._redis.hgetall(key)
            if data:
                name = data.get(b"name", b"").decode("utf-8") if isinstance(data.get(b"name"), bytes) else str(data.get("name", ""))
                if name:
                    agents.append(name)
        return agents

    def _score_agent(self, agent: dict) -> float:
        """Score an agent on performance and reliability."""
        event_count = agent.get("event_count", 0)
        error_count = agent.get("error_count", 0)

        if event_count == 0:
            return 0.5  # Neutral for new agents

        reliability = 1.0 - (error_count / max(1, event_count))
        activity = min(1.0, event_count / 100)

        return 0.7 * reliability + 0.3 * activity

    async def _throttle_agent(self, agent_name: str, reason: str):
        """Throttle an underperforming agent."""
        now = datetime.now(timezone.utc).timestamp()
        self._throttled_agents[agent_name] = now + 3600  # 1 hour throttle
        logger.warning(f"{self.name}: Throttling {agent_name} — {reason}")

        await self._redis.hset(
            f"agent:throttle:{agent_name}",
            mapping={
                "blocked_until": str(self._throttled_agents[agent_name]),
                "reason": reason,
            },
        )

    async def _restart_agent(self, agent_name: str, reason: str):
        """Escalate to restart an agent."""
        logger.warning(f"{self.name}: Restart recommended for {agent_name} — {reason}")
        # In a production system, this would signal the supervisor to restart

    async def is_agent_throttled(self, agent_name: str) -> bool:
        """Check if an agent is currently throttled."""
        if agent_name in self._throttled_agents:
            if self._throttled_agents[agent_name] > datetime.now(timezone.utc).timestamp():
                return True
            del self._throttled_agents[agent_name]
        return False
