"""
capital_preservation_engine.py — L4 Risk Agent for capital preservation.

Capabilities:
- Drawdown circuit breakers
- Capital freeze
- Emergency deleveraging
- Adaptive risk throttling
- Volatility targeting
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent


class CapitalPreservationEngine(BaseAgent):
    """
    L4 Risk Agent — Capital preservation with adaptive risk controls.
    """

    name = "CapitalPreservationEngine"
    agent_type = "capital_preservation"
    layer = "L4"

    # Drawdown thresholds
    DRAWDOWN_WARNING = 0.10   # 10% drawdown → warning
    DRAWDOWN_THROTTLE = 0.15  # 15% → reduce exposure by 50%
    DRAWDOWN_FREEZE = 0.20    # 20% → freeze all new positions
    DRAWDOWN_EMERGENCY = 0.25 # 25% → emergency deleverage

    def __init__(self, redis_client, db_client):
        super().__init__(self.name, self.agent_type, self.layer, redis_client)
        self.db = db_client
        self._run_interval = 60  # Check every minute

    async def run(self):
        logger.info(f"{self.name}: Starting capital preservation monitoring")

        while self.status == "running":
            try:
                await self._check_drawdown_protection()
            except Exception as e:
                logger.error(f"{self.name}: Capital check error: {e}")

            for _ in range(self._run_interval // 10):
                await self._sleep(10)
                if self.status != "running":
                    return

    async def _sleep(self, seconds: int):
        import asyncio
        await asyncio.sleep(seconds)

    async def _check_drawdown_protection(self):
        """Monitor portfolio drawdown and activate protection mechanisms."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT COALESCE(SUM(pnl), 0),
                           COALESCE(SUM(CASE WHEN side = 'long' THEN qty * price ELSE 0 END), 0)
                    FROM paper_trades
                """)
            )
            row = r.fetchone()
            total_pnl = float(row[0] or 0)
            total_exposure = float(row[1] or 0)

        # Estimate peak value (simplified: initial capital + max PnL)
        from atlas.config.settings import settings
        initial_capital = settings.initial_capital if hasattr(settings, 'initial_capital') else 100000.0
        peak_value = initial_capital + max(total_pnl, 0)
        current_value = initial_capital + total_pnl
        drawdown = (peak_value - current_value) / peak_value if peak_value > 0 else 0

        action = "none"
        exposure_cut = 1.0

        if drawdown >= self.DRAWDOWN_EMERGENCY:
            action = "emergency_deleverage"
            exposure_cut = 0.0
            await self._trigger_emergency_deleverage(drawdown)
        elif drawdown >= self.DRAWDOWN_FREEZE:
            action = "freeze"
            exposure_cut = 0.0
            await self._trigger_freeze(drawdown)
        elif drawdown >= self.DRAWDOWN_THROTTLE:
            action = "throttle"
            exposure_cut = 0.5
            await self._trigger_throttle(drawdown)
        elif drawdown >= self.DRAWDOWN_WARNING:
            action = "warning"
            exposure_cut = 0.8

        # Persist state
        await self.db._execute_insert(
            """
            INSERT INTO capital_preservation_state
                (id, checked_at, drawdown_pct, action_taken,
                 exposure_cut_ratio, peak_value, current_value,
                 total_pnl, total_exposure)
            VALUES
                (:id, NOW(), :drawdown, :action,
                 :exposure_cut, :peak, :current,
                 :pnl, :exposure)
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "drawdown": round(drawdown, 4),
                "action": action,
                "exposure_cut": exposure_cut,
                "peak": round(peak_value, 2),
                "current": round(current_value, 2),
                "pnl": round(total_pnl, 2),
                "exposure": round(total_exposure, 2),
            },
        )

        if action != "none":
            logger.warning(
                f"{self.name}: CAPITAL PRESERVATION — "
                f"drawdown={drawdown:.2%}, action={action}, "
                f"exposure_cut={exposure_cut:.0%}"
            )

    async def _trigger_emergency_deleverage(self, drawdown: float):
        """Emergency: close all positions and halt trading."""
        await self._redis.hset(
            "kill_switch:state",
            mapping={
                "active": "1",
                "reason": f"CapitalPreservation: Emergency deleverage at {drawdown:.1%} drawdown",
            },
        )
        logger.critical(f"{self.name}: EMERGENCY DELEVERAGE — drawdown={drawdown:.2%}")

    async def _trigger_freeze(self, drawdown: float):
        """Freeze: block new positions, allow existing to run."""
        await self._redis.hset(
            "capital:freeze",
            mapping={
                "active": "1",
                "reason": f"CapitalPreservation: Freeze at {drawdown:.1%} drawdown",
            },
        )
        self._redis.expire("capital:freeze", 3600)

    async def _trigger_throttle(self, drawdown: float):
        """Throttle: reduce position sizing."""
        await self._redis.hset(
            "capital:throttle",
            mapping={
                "active": "1",
                "exposure_cut": "0.5",
                "reason": f"CapitalPreservation: Throttle at {drawdown:.1%} drawdown",
            },
        )
        self._redis.expire("capital:throttle", 3600)

    async def get_preservation_status(self) -> dict:
        """Get current capital preservation status."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT drawdown_pct, action_taken, exposure_cut_ratio,
                           peak_value, current_value, total_pnl, total_exposure,
                           checked_at
                    FROM capital_preservation_state
                    ORDER BY checked_at DESC LIMIT 1
                """)
            )
            row = r.fetchone()
            if not row:
                return {"status": "no_data"}

            return {
                "drawdown_pct": float(row[0]) if row[0] else 0,
                "action_taken": str(row[1]),
                "exposure_cut_ratio": float(row[2]) if row[2] else 1.0,
                "peak_value": float(row[3]) if row[3] else 0,
                "current_value": float(row[4]) if row[4] else 0,
                "total_pnl": float(row[5]) if row[5] else 0,
                "total_exposure": float(row[6]) if row[6] else 0,
                "checked_at": row[7].isoformat() if hasattr(row[7], "isoformat") else str(row[7]),
            }
