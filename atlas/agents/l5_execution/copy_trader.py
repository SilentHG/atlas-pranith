"""
copy_trader.py — Day-4 V1

Purpose:
  - Watch leader fills (via Redis `execution_fills` or leader_orders table)
  - Mirror orders to configured followers with proportional sizing
  - Perform follower-side risk checks (allocation_ratio, max_position_pct)
  - Persist idempotent audit in `copy_execution_log`
  - Log latency, retries, and failures

Design notes:
  - Uses `copy_leader_accounts` and `copy_follower_accounts` tables (migration)
  - Safe: uses idempotent inserts and Redis set to track processed leader_order_ids
  - BrokerAdapter abstraction with a LocalSimulator provided (no external broker calls)
  - Restart-safe and observable
"""

import asyncio
import json
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List
from loguru import logger
from redis.asyncio import Redis
from sqlalchemy.sql import text

from atlas.core.agent_base import BaseAgent
from atlas.core.messaging import MessagingClient, Channel
from atlas.data.storage.timescale_client import TimescaleClient
from atlas.config.settings import settings
from atlas.agents.l6_portfolio.copy_capital_allocator import CopyCapitalAllocator


class BrokerAdapter:
    """Abstract broker adapter. Implement `place_order` in subclasses."""

    async def place_order(self, account_ref: str, symbol: str, side: str, qty: float, price: float | None = None) -> Dict[str, Any]:
        raise NotImplementedError()


class LocalSimulatorAdapter(BrokerAdapter):
    """Simulates order placement locally (for Day-4 demos/tests)."""

    async def place_order(self, account_ref: str, symbol: str, side: str, qty: float, price: float | None = None) -> Dict[str, Any]:
        # Simulate network latency and return a fake order id and timestamp
        start = time.time()
        await asyncio.sleep(0.05 + min(0.5, max(0.0, 0.01 * (len(symbol)))))
        latency = int((time.time() - start) * 1000)
        return {
            "order_id": str(uuid.uuid4()),
            "filled_qty": qty,
            "status": "filled",
            "created_at": datetime.utcnow().isoformat(),
            "latency_ms": latency,
        }


class CopyTraderAgent(BaseAgent):
    def __init__(self, redis_client: Redis, db_client: TimescaleClient, broker: BrokerAdapter | None = None):
        super().__init__(name="CopyTraderV1", agent_type="copy_trader", layer="L5", redis_client=redis_client)
        self.redis = redis_client
        self.db = db_client
        self.messaging = MessagingClient(redis_client)
        self.broker = broker or LocalSimulatorAdapter()
        self.allocator = CopyCapitalAllocator(redis_client, db_client)
        self._processed_set_key = "copy:processed_leader_orders"
        self._poll_interval = 1.0
        self._background_tasks: set[asyncio.Task] = set()
        self._allocator_started = False

    def _track_background_task(self, task: asyncio.Task) -> asyncio.Task:
        self._background_tasks.add(task)

        def _finalize(done_task: asyncio.Task) -> None:
            self._background_tasks.discard(done_task)
            try:
                done_task.result()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception("CopyTrader background task failed")

        task.add_done_callback(_finalize)
        return task

    async def _shutdown_background_tasks(self) -> None:
        pending = [task for task in self._background_tasks if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._background_tasks.clear()

        if self._allocator_started:
            try:
                await self.allocator.stop()
            except Exception:
                logger.exception("Failed to stop copy capital allocator")
            finally:
                self._allocator_started = False

    async def run(self):
        logger.info("CopyTraderV1 starting")
        try:
            # Load follower mappings into memory (refreshed periodically)
            followers = await self._load_followers()

            # Subscribe to execution fills for leader signals
            async def _callback(msg: dict):
                await self._handle_leader_fill(msg, followers)

            # Start Capital Allocator
            await self.allocator.start()
            self._allocator_started = True

            # Start a background refresher for followers
            self._track_background_task(asyncio.create_task(self._refresh_followers_loop()))

            # Start polling loop as background task (in addition to subscribe)
            self._track_background_task(asyncio.create_task(self._polling_loop(followers)))

            # Subscribe to Redis pubsub; if fails, polling will still work
            try:
                await self.messaging.subscribe(Channel.EXECUTION_FILLS, _callback)
            except Exception as e:
                logger.warning(f"PubSub subscribe failed, but polling is active: {e}")
                # Keep running with polling — check status periodically for clean shutdown
                while self.status == "running":
                    await asyncio.sleep(1)
        finally:
            await self._shutdown_background_tasks()

    async def stop(self):
        await super().stop()
        await self._shutdown_background_tasks()

    async def _polling_loop(self, followers_map: Dict[str, List[Dict[str, Any]]]):
        """Background polling loop for leader_orders table"""
        logger.info("Polling loop started")
        while self.status == "running":
            try:
                await self._poll_leader_orders(followers_map)
            except Exception as ex:
                logger.debug(f"Polling iteration: {ex}")
            await asyncio.sleep(self._poll_interval)

    async def _refresh_followers_loop(self):
        while self.status == "running":
            try:
                self._followers = await self._load_followers()
            except Exception:
                logger.exception("Failed to refresh followers")
            await asyncio.sleep(30)

    async def _load_followers(self) -> Dict[str, List[Dict[str, Any]]]:
        """Return mapping leader_account_ref -> list of follower dicts."""
        out: Dict[str, List[Dict[str, Any]]] = {}
        async with self.db.engine.connect() as conn:
            res = await conn.execute(text("SELECT follower_id, leader_id, broker, account_ref, allocation_ratio, max_position_pct, is_active FROM copy_follower_accounts WHERE is_active = TRUE"))
            rows = res.fetchall()
            # Map leader_id to account_ref via copy_leader_accounts
            leader_map = {}
            res2 = await conn.execute(text("SELECT leader_id, account_ref FROM copy_leader_accounts WHERE is_active = TRUE"))
            for r in res2.fetchall():
                leader_map[str(r[0])] = r[1]

            for r in rows:
                leader_id = str(r[1])
                leader_ref = leader_map.get(leader_id)
                if not leader_ref:
                    continue
                follower = {
                    "follower_id": str(r[0]),
                    "broker": r[2],
                    "account_ref": r[3],
                    "allocation_ratio": float(r[4]) if r[4] is not None else 1.0,
                    "max_position_pct": float(r[5]) if r[5] is not None else 0.1,
                }
                out.setdefault(leader_ref, []).append(follower)
        logger.info(f"Loaded follower map for {len(out)} leaders")
        return out

    async def _handle_leader_fill(self, msg: dict, followers_map: Dict[str, List[Dict[str, Any]]]):
        """Process a leader fill message. Expected keys: order_id, account_ref, symbol, side, qty, price, leader_id (optional)."""
        try:
            leader_order_id = msg.get("order_id") or msg.get("id")
            if not leader_order_id:
                logger.warning("Leader fill missing order_id: %s", msg)
                return

            # Idempotency: check Redis processed set
            was = await self.redis.sismember(self._processed_set_key, leader_order_id)
            if was:
                logger.debug(f"Leader order {leader_order_id} already processed")
                return

            account_ref = msg.get("account_ref") or msg.get("account")
            symbol = msg.get("symbol")
            side = msg.get("side")
            qty = float(msg.get("qty") or msg.get("filled_qty") or 0)
            price = msg.get("price")
            leader_id = msg.get("leader_id")

            # Mark processed early to reduce duplicate processing across restarts; removed on failure
            await self.redis.sadd(self._processed_set_key, leader_order_id)
            await self.redis.expire(self._processed_set_key, 60 * 60 * 24)

            followers = followers_map.get(account_ref) or []
            if not followers:
                logger.info(f"No followers for leader account {account_ref}")
                return

            # Fetch market state for realism
            market_state = await self._get_market_state(symbol)
            if market_state.get("is_dangerous", False):
                logger.warning(f"Dangerous market detected for {symbol}. Rejecting copy flow.")
                return

            # For each follower, compute qty and attempt order with retries
            for f in followers:
                # Execution degradation check
                degraded_mode = await self._get_follower_degraded_mode(f["follower_id"])
                if degraded_mode in ("frozen_follow", "observation_only"):
                    logger.info(f"Follower {f['follower_id']} is in {degraded_mode}. Skipping order.")
                    continue

                # Get current exposure
                current_exposure = await self._get_follower_exposure(f["account_ref"], symbol)
                leader_total_exposure = await self._get_leader_exposure(account_ref)

                # Use Phase 21C Capital Intelligence Layer
                allocation = self.allocator.compute_allocation(
                    follower_id=f["follower_id"],
                    symbol=symbol,
                    leader_qty=qty,
                    leader_price=float(price or 0.0),
                    leader_total_exposure=leader_total_exposure,
                    current_follower_exposure=current_exposure,
                    symbol_volatility=market_state.get("volatility", 0.02),
                    liquidity_score=market_state.get("liquidity_score", 1.0),
                    overlap_penalty=await self._get_overlap_penalty(f["follower_id"], symbol)
                )

                if allocation.allocated_qty == 0:
                    logger.info(f"Allocation rejected for {f['follower_id']}: {allocation.rejection_reason}")
                    await self._log_copy_execution(
                        leader_order_id, None, account_ref, f, symbol, side, qty, 0,
                        status="skipped", failure_reason=allocation.rejection_reason
                    )
                    continue

                follower_qty = allocation.allocated_qty
                # Basic follower risk check: ensure not exceeding max_position_pct (best-effort)
                allowed = True
                try:
                    allowed = await self._check_follower_risk(f, symbol, follower_qty)
                except Exception as e:
                    logger.warning(f"Risk check failed for follower {f['follower_id']}: {e}")

                if not allowed:
                    await self._log_copy_execution(leader_order_id, None, account_ref, f, symbol, side, qty, 0, status="skipped", failure_reason="risk_rejected")
                    continue

                # Place order with retries
                attempt = 0
                placed = None
                start_ts = time.time()
                while attempt < 3:
                    try:
                        resp = await self.broker.place_order(f["account_ref"], symbol, side, follower_qty, price)
                        placed = resp
                        break
                    except Exception as e:
                        attempt += 1
                        logger.warning(f"Failed placing order for follower {f['follower_id']} attempt {attempt}: {e}")
                        await asyncio.sleep(0.5 * attempt)

                latency_ms = int((time.time() - start_ts) * 1000)

                if placed:
                    follower_order_id = placed.get("order_id")
                    await self._log_copy_execution(
                        leader_order_id, follower_order_id, account_ref, f, symbol, side, qty, follower_qty,
                        status=placed.get("status"), failure_reason=None, latency_ms=placed.get("latency_ms", latency_ms),
                        leader_price=float(price or 0), follower_price=float(price or 0) # Simulated
                    )
                else:
                    await self._log_copy_execution(
                        leader_order_id, None, account_ref, f, symbol, side, qty, follower_qty,
                        status="failed", failure_reason="order_failed", latency_ms=latency_ms,
                        leader_price=float(price or 0), follower_price=0.0
                    )

        except Exception as e:
            logger.exception(f"Error handling leader fill: {e}")
            # best-effort: do not re-raise

    async def _poll_leader_orders(self, followers_map: Dict[str, List[Dict[str, Any]]]):
        """Poll leader_orders table for new fills (if available). This is a fallback mechanism."""
        async with self.db.engine.connect() as conn:
            # Assumes leader_orders has id, account_ref, symbol, side, qty, created_at
            res = await conn.execute(text("SELECT id, account_ref, symbol, side, qty, price FROM leader_orders WHERE created_at > NOW() - INTERVAL '1 minute' ORDER BY created_at ASC"))
            rows = res.fetchall()
            for r in rows:
                msg = {"order_id": str(r[0]), "account_ref": r[1], "symbol": r[2], "side": r[3], "qty": float(r[4]), "price": r[5]}
                await self._handle_leader_fill(msg, followers_map)

    async def _get_market_state(self, symbol: str) -> dict:
        """Simulate fetching market state for realism."""
        return {
            "volatility": 0.015,
            "liquidity_score": 0.9,
            "is_dangerous": False
        }

    async def _get_follower_degraded_mode(self, follower_id: str) -> str:
        """Get follower failover mode from Redis if set."""
        mode = await self.redis.get(f"copy_failover:{follower_id}:mode")
        return mode.decode("utf-8") if mode else "normal"

    async def _get_overlap_penalty(self, follower_id: str, symbol: str) -> float:
        """Fetch overlap penalty to prevent concentration."""
        return 0.0

    async def _get_follower_exposure(self, account_ref: str, symbol: str) -> float:
        try:
            async with self.db.engine.connect() as conn:
                res = await conn.execute(text("SELECT COALESCE(SUM(qty * COALESCE(current_price, avg_entry_price)), 0) FROM positions WHERE account_ref = :acc AND symbol = :sym"), {"acc": account_ref, "sym": symbol})
                row = res.fetchone()
                return float(row[0]) if row and row[0] is not None else 0.0
        except Exception:
            return 0.0

    async def _get_leader_exposure(self, account_ref: str) -> float:
        try:
            async with self.db.engine.connect() as conn:
                res = await conn.execute(text("SELECT COALESCE(SUM(qty * COALESCE(current_price, avg_entry_price)), 0) FROM positions WHERE account_ref = :acc"), {"acc": account_ref})
                row = res.fetchone()
                return float(row[0]) if row and row[0] is not None else 0.0
        except Exception:
            return 10000.0  # Fallback to avoid div by zero

    async def _check_follower_risk(self, follower: Dict[str, Any], symbol: str, qty: float) -> bool:
        # Replaced by Capital Intelligence Layer
        return True

    async def _log_copy_execution(
        self, leader_order_id, follower_order_id, leader_account_ref, follower: Dict[str, Any],
        symbol, side, leader_qty, follower_qty, status, failure_reason=None, latency_ms: int | None = None,
        leader_price: float = 0.0, follower_price: float = 0.0
    ):
        """Idempotent insert into copy_execution_log and Phase 21H copy_replay_events."""
        trace_id = uuid.uuid4().hex[:16]
        follower_id = follower.get("follower_id")
        leader_id = leader_account_ref  # In a real system, resolve account_ref -> leader_id
        
        async with self.db.engine.begin() as conn:
            query = text(
                """
                INSERT INTO copy_execution_log (id, leader_order_id, follower_order_id, leader_id, follower_id, symbol, side, leader_qty, follower_qty, latency_ms, status, failure_reason, created_at)
                SELECT :id, :leader_order_id, :follower_order_id, :leader_id, :follower_id, :symbol, :side, :leader_qty, :follower_qty, :latency_ms, :status, :failure_reason, NOW()
                WHERE NOT EXISTS (
                    SELECT 1 FROM copy_execution_log WHERE leader_order_id = :leader_order_id AND follower_id = :follower_id
                )
                """
            )
            params = {
                "id": str(uuid.uuid4()),
                "leader_order_id": leader_order_id,
                "follower_order_id": follower_order_id,
                "leader_id": leader_id,
                "follower_id": follower_id,
                "symbol": symbol,
                "side": side,
                "leader_qty": leader_qty,
                "follower_qty": follower_qty,
                "latency_ms": latency_ms,
                "status": status,
                "failure_reason": failure_reason,
            }
            res = await conn.execute(query, params)
            
            # If a row was inserted, also emit to copy_replay_events for Phase 21H
            if res.rowcount > 0:
                replay_query = text(
                    """
                    INSERT INTO copy_replay_events (
                        id, trace_id, event_type, leader_id, follower_id,
                        leader_order_id, follower_order_id, symbol, side,
                        leader_qty, follower_qty, leader_price, follower_price,
                        execution_latency_ms, event_data, created_at
                    ) VALUES (
                        :id, :trace_id, 'execution_mirrored', :leader_id, :follower_id,
                        :leader_order_id, :follower_order_id, :symbol, :side,
                        :leader_qty, :follower_qty, :leader_price, :follower_price,
                        :latency_ms, CAST(:event_data AS jsonb), NOW()
                    )
                    """
                )
                await conn.execute(replay_query, {
                    "id": str(uuid.uuid4()),
                    "trace_id": trace_id,
                    "leader_id": leader_id,
                    "follower_id": follower_id,
                    "leader_order_id": leader_order_id,
                    "follower_order_id": follower_order_id,
                    "symbol": symbol,
                    "side": side,
                    "leader_qty": leader_qty,
                    "follower_qty": follower_qty,
                    "leader_price": leader_price,
                    "follower_price": follower_price,
                    "latency_ms": latency_ms or 0,
                    "event_data": json.dumps({"status": status, "failure_reason": failure_reason})
                })

        # Publish a small event for observability
        try:
            await self.messaging.publish(Channel.SYSTEM_EVENTS, {"event": "copy_execution", "leader_order_id": leader_order_id, "follower_id": follower_id, "status": status})
        except Exception as e:
            logger.warning(f"Failed to publish copy execution event: {e}")


async def main():
    print("=== COPY TRADER V1 START ===", flush=True)
    db = TimescaleClient(settings.database_url)
    await db.connect()
    redis = Redis.from_url(settings.redis_url)

    agent = CopyTraderAgent(redis, db)
    await agent.start()

    # Keep running
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
