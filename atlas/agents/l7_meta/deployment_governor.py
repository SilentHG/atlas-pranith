"""
deployment_governor.py — L7 Meta Agent for deployment governance.

Capabilities:
- Canary deployments
- Shadow deployments
- Rollout approval gates
- Automatic rollback
- Performance regression detection
- Deployment survivability scoring

Modes:
- paper, shadow, partial-live, live
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent


class DeploymentMode(str, Enum):
    PAPER = "paper"
    SHADOW = "shadow"
    PARTIAL_LIVE = "partial_live"
    LIVE = "live"
    ROLLED_BACK = "rolled_back"
    REJECTED = "rejected"


class DeploymentGovernor(BaseAgent):
    """
    L7 Meta Agent — Governs strategy deployments with safety gates.
    """

    name = "DeploymentGovernor"
    agent_type = "deployment_governor"
    layer = "L7"

    def __init__(self, redis_client, db_client):
        super().__init__(self.name, self.agent_type, self.layer, redis_client)
        self.db = db_client
        self._run_interval = 600  # Every 10 minutes

    async def run(self):
        logger.info(f"{self.name}: Starting deployment governance")

        while self.status == "running":
            try:
                await self._sweep_pending_deployments()
                await self._check_live_regressions()
            except Exception as e:
                logger.error(f"{self.name}: Governance sweep error: {e}")

            for _ in range(self._run_interval // 10):
                await self._sleep(10)
                if self.status != "running":
                    return

    async def _sleep(self, seconds: int):
        import asyncio
        await asyncio.sleep(seconds)

    async def propose_deployment(
        self,
        strategy_id: str,
        proposed_by: str,
        mode: str = "paper",
        metadata: Optional[dict] = None,
    ) -> str:
        """Propose a new deployment. Returns deployment_id."""
        dep_id = uuid.uuid4().hex[:16]
        await self.db._execute_insert(
            """
            INSERT INTO deployment_governance
                (id, strategy_id, mode, status, proposed_by,
                 proposed_at, metadata)
            VALUES
                (:id, :strategy_id, :mode, 'pending_approval',
                 :proposed_by, NOW(), CAST(:metadata AS jsonb))
            """,
            {
                "id": dep_id,
                "strategy_id": strategy_id,
                "mode": mode,
                "proposed_by": proposed_by,
                "metadata": json.dumps(metadata or {}),
            },
        )
        logger.info(f"{self.name}: Deployment proposed {dep_id} for {strategy_id} ({mode})")
        return dep_id

    async def approve_deployment(
        self,
        deployment_id: str,
        approved_by: str,
    ) -> bool:
        """Approve a pending deployment."""
        async with self.db.engine.begin() as conn:
            r = await conn.execute(
                text("""
                    UPDATE deployment_governance
                    SET status = 'approved',
                        approved_by = :approved_by,
                        approved_at = NOW(),
                        updated_at = NOW()
                    WHERE id = :id AND status = 'pending_approval'
                """),
                {"id": deployment_id, "approved_by": approved_by},
            )
            if r.rowcount == 0:
                logger.warning(f"{self.name}: Cannot approve {deployment_id} — not pending")
                return False

        logger.info(f"{self.name}: Deployment {deployment_id} approved by {approved_by}")
        return True

    async def execute_deployment(self, deployment_id: str) -> bool:
        """Execute an approved deployment, moving it through modes."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT strategy_id, mode, metadata
                    FROM deployment_governance WHERE id = :id
                """),
                {"id": deployment_id},
            )
            row = r.fetchone()
            if not row:
                return False

            strategy_id = str(row[0])
            target_mode = str(row[1])
            meta_raw = row[2]

        # Run validation gate
        passed = await self._validate_deployment_gate(strategy_id)
        if not passed:
            await self._update_status(deployment_id, "rejected")
            logger.warning(f"{self.name}: Deployment {deployment_id} rejected — validation failed")
            return False

        # Execute
        await self._update_status(deployment_id, "deploying")
        await self._update_strategy_mode(strategy_id, target_mode)
        await self._update_status(deployment_id, target_mode)

        logger.info(f"{self.name}: Deployment {deployment_id} active ({target_mode})")
        return True

    async def rollback_deployment(self, deployment_id: str) -> bool:
        """Rollback a deployment."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT strategy_id FROM deployment_governance WHERE id = :id
                """),
                {"id": deployment_id},
            )
            row = r.fetchone()
            if not row:
                return False
            strategy_id = str(row[0])

        await self._update_status(deployment_id, "rolling_back")
        await self._update_strategy_mode(strategy_id, "paper")
        await self._update_status(deployment_id, "rolled_back")

        logger.warning(f"{self.name}: Deployment {deployment_id} rolled back")
        return True

    async def _sweep_pending_deployments(self):
        """Auto-approve paper deployments, flag others for review."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT id, strategy_id, mode, proposed_at
                    FROM deployment_governance
                    WHERE status = 'pending_approval'
                    ORDER BY proposed_at ASC
                """),
            )
            pending = r.fetchall()

        for row in pending:
            dep_id = str(row[0])
            mode = str(row[2])
            if mode == "paper":
                await self.approve_deployment(dep_id, "DeploymentGovernor(auto)")
                await self.execute_deployment(dep_id)

    async def _check_live_regressions(self):
        """Check for performance regressions in live strategies."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT d.id, d.strategy_id, d.mode, d.activated_at
                    FROM deployment_governance d
                    WHERE d.status IN ('partial_live', 'live')
                      AND d.activated_at < NOW() - INTERVAL '1 hour'
                """),
            )
            live = r.fetchall()

        for row in live:
            dep_id = str(row[0])
            strategy_id = str(row[1])
            # Check for performance degradation
            has_regression = await self._detect_regression(strategy_id)
            if has_regression:
                logger.warning(
                    f"{self.name}: Regression detected in {strategy_id}, auto-rolling back"
                )
                await self.rollback_deployment(dep_id)

    async def _validate_deployment_gate(self, strategy_id: str) -> bool:
        """Validate that a strategy is fit for deployment."""
        async with self.db.engine.connect() as conn:
            # Check for validation data
            r = await conn.execute(
                text("""
                    SELECT
                        COALESCE((
                            SELECT walk_forward_score
                            FROM walk_forward_analysis
                            WHERE strategy_id = :sid
                            ORDER BY analyzed_at DESC LIMIT 1
                        ), 0) as wf_score,
                        COALESCE((
                            SELECT overfit_probability
                            FROM overfitting_analysis
                            WHERE strategy_id = :sid
                            ORDER BY analyzed_at DESC LIMIT 1
                        ), 1.0) as overfit_prob
                """),
                {"sid": strategy_id},
            )
            row = r.fetchone()
            if not row:
                return False

            wf_score = float(row[0] or 0)
            overfit_prob = float(row[1] or 1.0)

            return wf_score >= 50.0 and overfit_prob < 0.5

    async def _detect_regression(self, strategy_id: str) -> bool:
        """Check if a strategy has regressed compared to backtest."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT short_window_score FROM backtest_results
                    WHERE strategy_id = :sid
                    ORDER BY start_date DESC LIMIT 1
                """),
                {"sid": strategy_id},
            )
            row = r.fetchone()
            if not row:
                return False

            score = float(row[0] or 0)
            return score < 30.0  # Significant degradation

    async def _update_status(self, deployment_id: str, status: str):
        await self.db._execute_insert(
            """
            UPDATE deployment_governance
            SET status = :status,
                activated_at = CASE WHEN :status IN ('paper','shadow','partial_live','live')
                                    THEN NOW() ELSE activated_at END,
                updated_at = NOW()
            WHERE id = :id
            """,
            params={"id": deployment_id, "status": status},
        )

    async def _update_strategy_mode(self, strategy_id: str, mode: str):
        await self.db._execute_insert(
            """
            UPDATE strategies
            SET deployment_mode = :mode, updated_at = NOW()
            WHERE id = :sid
            """,
            params={"sid": strategy_id, "mode": mode},
        )

    async def get_deployment_report(self) -> dict:
        """Get current deployment status overview."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT status, COUNT(*) as cnt
                    FROM deployment_governance
                    GROUP BY status ORDER BY cnt DESC
                """)
            )
            by_status = {str(row[0]): row[1] for row in r.fetchall()}

            r = await conn.execute(
                text("""
                    SELECT id, strategy_id, mode, status, proposed_by,
                           approved_by, proposed_at, activated_at
                    FROM deployment_governance
                    ORDER BY proposed_at DESC LIMIT 20
                """)
            )
            recent = []
            for row in r.fetchall():
                recent.append({
                    "id": str(row[0]),
                    "strategy_id": str(row[1]),
                    "mode": str(row[2]),
                    "status": str(row[3]),
                    "proposed_by": str(row[4]),
                    "approved_by": str(row[5]) if row[5] else None,
                    "proposed_at": row[6].isoformat() if hasattr(row[6], "isoformat") else str(row[6]),
                    "activated_at": row[7].isoformat() if row[7] and hasattr(row[7], "isoformat") else (str(row[7]) if row[7] else None),
                })

            return {
                "by_status": by_status,
                "recent_deployments": recent,
            }
