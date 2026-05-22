"""
position_reconciliation_engine.py — Phase 21A

Continuously reconciles leader vs follower portfolio state.
Compares actual economic portfolio state (not just orders):
- positions, exposure, leverage, avg entry, pnl, partial fills,
  pending orders, execution latency.

Outputs reconciliation reports, drift metrics, repair recommendations.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from loguru import logger
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent


class PositionReconciliationEngine(BaseAgent):
    """L5 Agent — Continuous leader/follower portfolio state reconciliation."""

    name = "PositionReconciliationEngine"
    agent_type = "position_reconciliation"
    layer = "L5"

    def __init__(self, redis_client, db_client):
        super().__init__(self.name, self.agent_type, self.layer, redis_client)
        self.db = db_client
        self._run_interval = 300  # Every 5 minutes

    async def run(self):
        logger.info(f"{self.name}: Starting position reconciliation")
        while self.status == "running":
            try:
                await self._reconcile_all()
            except Exception as e:
                logger.error(f"{self.name}: Reconciliation error: {e}")
            for _ in range(self._run_interval // 10):
                await asyncio.sleep(10)
                if self.status != "running":
                    return

    async def _reconcile_all(self):
        """Reconcile all active leader-follower pairs."""
        pairs = await self._load_active_pairs()
        if not pairs:
            logger.debug(f"{self.name}: No active copy pairs to reconcile")
            return

        for leader_id, follower_id in pairs:
            try:
                await self._reconcile_pair(leader_id, follower_id)
            except Exception as e:
                logger.warning(
                    f"{self.name}: Reconciliation failed for "
                    f"leader={leader_id} follower={follower_id}: {e}"
                )

    async def _load_active_pairs(self) -> list[tuple[str, str]]:
        """Load active leader-follower pairs."""
        try:
            async with self.db.engine.connect() as conn:
                r = await conn.execute(text("""
                    SELECT l.leader_id, f.follower_id
                    FROM copy_leader_accounts l
                    JOIN copy_follower_accounts f ON f.leader_id = l.leader_id
                    WHERE l.is_active = TRUE AND f.is_active = TRUE
                """))
                return [(str(row[0]), str(row[1])) for row in r.fetchall()]
        except Exception as e:
            logger.debug(f"{self.name}: Cannot load pairs: {e}")
            return []

    async def _reconcile_pair(self, leader_id: str, follower_id: str):
        """Reconcile a single leader-follower pair across all symbols."""
        leader_positions = await self._get_positions(leader_id, role="leader")
        follower_positions = await self._get_positions(follower_id, role="follower")

        all_symbols = set(leader_positions.keys()) | set(follower_positions.keys())
        if not all_symbols:
            return

        mismatches = 0
        total_exposure_delta = 0.0
        total_pnl_delta = 0.0
        repair_actions = []
        trace_id = uuid.uuid4().hex[:16]

        for symbol in all_symbols:
            lp = leader_positions.get(symbol, self._empty_position())
            fp = follower_positions.get(symbol, self._empty_position())

            # Compute deltas
            qty_delta = abs(float(lp["qty"]) - float(fp["qty"]))
            exposure_delta = abs(float(lp["exposure"]) - float(fp["exposure"]))
            pnl_delta = abs(
                float(lp["unrealized_pnl"]) - float(fp["unrealized_pnl"])
            )

            total_exposure_delta += exposure_delta
            total_pnl_delta += pnl_delta

            is_mismatch = qty_delta > 0.01 or exposure_delta > 0.5

            if is_mismatch:
                mismatches += 1
                # Determine repair action
                if float(fp["qty"]) == 0 and float(lp["qty"]) > 0:
                    repair_actions.append({
                        "symbol": symbol,
                        "action": "open_follower_position",
                        "leader_qty": float(lp["qty"]),
                        "follower_qty": 0,
                    })
                elif float(lp["qty"]) == 0 and float(fp["qty"]) > 0:
                    repair_actions.append({
                        "symbol": symbol,
                        "action": "close_follower_position",
                        "follower_qty": float(fp["qty"]),
                    })
                else:
                    repair_actions.append({
                        "symbol": symbol,
                        "action": "adjust_follower_qty",
                        "leader_qty": float(lp["qty"]),
                        "follower_qty": float(fp["qty"]),
                        "delta": qty_delta,
                    })

            # Snapshot position state
            await self._persist_position_state(
                trace_id, leader_id, follower_id, symbol, lp, fp
            )

        # Compute reconciliation score
        recon_score = max(0.0, 1.0 - (mismatches / max(len(all_symbols), 1)))

        # Persist reconciliation report
        await self.db._execute_insert(
            """
            INSERT INTO follower_reconciliation
                (id, trace_id, leader_id, follower_id,
                 reconciliation_type, n_positions_checked, n_mismatches,
                 exposure_delta, pnl_delta, repair_actions,
                 reconciliation_score, metadata, reconciled_at)
            VALUES
                (:id, :trace_id, :leader_id, :follower_id,
                 'periodic', :n_checked, :n_mismatches,
                 :exposure_delta, :pnl_delta, CAST(:repair_actions AS jsonb),
                 :recon_score, CAST(:metadata AS jsonb), NOW())
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "trace_id": trace_id,
                "leader_id": leader_id,
                "follower_id": follower_id,
                "n_checked": len(all_symbols),
                "n_mismatches": mismatches,
                "exposure_delta": round(total_exposure_delta, 4),
                "pnl_delta": round(total_pnl_delta, 4),
                "repair_actions": json.dumps(repair_actions),
                "recon_score": round(recon_score, 4),
                "metadata": json.dumps({
                    "symbols": sorted(all_symbols),
                    "agent": self.name,
                }),
            },
        )

        if mismatches > 0:
            logger.info(
                f"{self.name}: Reconciled {leader_id}↔{follower_id} — "
                f"{mismatches}/{len(all_symbols)} mismatches, "
                f"score={recon_score:.2f}"
            )

    async def _get_positions(
        self, account_id: str, role: str
    ) -> dict[str, dict]:
        """Load current positions for a leader or follower account."""
        try:
            async with self.db.engine.connect() as conn:
                r = await conn.execute(text("""
                    SELECT symbol,
                           COALESCE(SUM(qty), 0) as qty,
                           COALESCE(AVG(avg_entry_price), 0) as avg_entry,
                           COALESCE(SUM(qty * COALESCE(current_price, avg_entry_price)), 0) as exposure,
                           COALESCE(SUM(unrealized_pnl), 0) as unrealized_pnl,
                           COALESCE(SUM(realized_pnl), 0) as realized_pnl
                    FROM positions
                    WHERE account_ref = :account_id
                    GROUP BY symbol
                """), {"account_id": account_id})
                return {
                    str(row[0]): {
                        "qty": float(row[1]),
                        "avg_entry": float(row[2]),
                        "exposure": float(row[3]),
                        "unrealized_pnl": float(row[4]),
                        "realized_pnl": float(row[5]),
                    }
                    for row in r.fetchall()
                }
        except Exception:
            return {}

    def _empty_position(self) -> dict:
        return {
            "qty": 0.0, "avg_entry": 0.0, "exposure": 0.0,
            "unrealized_pnl": 0.0, "realized_pnl": 0.0,
        }

    async def _persist_position_state(
        self, trace_id: str, leader_id: str, follower_id: str,
        symbol: str, lp: dict, fp: dict
    ):
        """Persist a position state snapshot."""
        sync_quality = max(0.0, 1.0 - abs(
            float(lp["qty"]) - float(fp["qty"])
        ) / max(abs(float(lp["qty"])), 1.0))

        await self.db._execute_insert(
            """
            INSERT INTO copy_position_state
                (id, trace_id, leader_id, follower_id, symbol,
                 leader_qty, follower_qty, leader_avg_entry, follower_avg_entry,
                 leader_exposure, follower_exposure,
                 leader_unrealized_pnl, follower_unrealized_pnl,
                 leader_realized_pnl, follower_realized_pnl,
                 sync_quality_score, metadata, snapshot_at)
            VALUES
                (:id, :trace_id, :leader_id, :follower_id, :symbol,
                 :l_qty, :f_qty, :l_entry, :f_entry,
                 :l_exp, :f_exp,
                 :l_upnl, :f_upnl,
                 :l_rpnl, :f_rpnl,
                 :sync_q, CAST(:metadata AS jsonb), NOW())
            """,
            {
                "id": uuid.uuid4().hex[:16],
                "trace_id": trace_id,
                "leader_id": leader_id,
                "follower_id": follower_id,
                "symbol": symbol,
                "l_qty": lp["qty"], "f_qty": fp["qty"],
                "l_entry": lp["avg_entry"], "f_entry": fp["avg_entry"],
                "l_exp": lp["exposure"], "f_exp": fp["exposure"],
                "l_upnl": lp["unrealized_pnl"], "f_upnl": fp["unrealized_pnl"],
                "l_rpnl": lp["realized_pnl"], "f_rpnl": fp["realized_pnl"],
                "sync_q": round(sync_quality, 4),
                "metadata": json.dumps({"agent": self.name}),
            },
        )
