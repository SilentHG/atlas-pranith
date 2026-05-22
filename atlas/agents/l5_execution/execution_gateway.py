"""
execution_gateway.py — Central Execution Orchestrator.

This is the ONLY approved execution path.
Flow:
Strategy → OrderTracker → Risk → KillSwitch → BrokerAdapter → PositionManager → Lineage
"""

import asyncio
from typing import Optional

from loguru import logger
from redis.asyncio import Redis

from atlas.core.agent_base import BaseAgent
from atlas.core.event_lineage import EventLineageClient
from atlas.agents.l4_risk.risk_controller import RiskController
from atlas.agents.l4_risk.kill_switch import KillSwitch
from atlas.data.storage.timescale_client import TimescaleClient

from .broker_adapter import BrokerAdapter
from .order_tracker import OrderTracker, OrderState
from .position_manager import PositionManager
from .recovery_manager import RecoveryManager
from .dead_letter import DeadLetterManager


class ExecutionGateway(BaseAgent):
    """
    Central execution orchestrator.
    INVARIANTS:
    - The ONLY code path that calls broker APIs.
    - Every order passes through idempotency → risk → kill switch → broker → tracker.
    - Recovery lock blocks execution until startup reconciliation completes.

    Distributed Execution Governance (Phase 12.7):
    - Redis distributed locking with lease TTL
    - Multi-instance ownership tracking via instance_id
    - Failover-safe order handling with lease renewal
    - Periodic heartbeat-based lease maintenance
    - All in-memory execution locks removed (fully distributed)
    """

    name = "ExecutionGateway"
    agent_type = "executor"
    layer = "L5"

    # Lease maintenance interval
    LEASE_RENEWAL_INTERVAL = 15  # seconds

    def __init__(
        self,
        redis_client: Redis,
        db_client: TimescaleClient,
        broker: BrokerAdapter,
        risk: RiskController,
        lineage: EventLineageClient,
        instance_id: Optional[str] = None,
    ):
        super().__init__(
            name=self.name,
            agent_type=self.agent_type,
            layer=self.layer,
            redis_client=redis_client,
        )
        self.db = db_client
        self.broker = broker
        self.risk = risk
        self.lineage = lineage
        self.instance_id = instance_id or ""

        self.tracker = OrderTracker(redis_client, db_client, instance_id=instance_id)
        self.positions = PositionManager(redis_client, db_client, broker)
        self.dead_letter = DeadLetterManager(redis_client, db_client)
        self.recovery = RecoveryManager(
            redis_client, db_client, broker, self.tracker, self.positions, self.dead_letter
        )

        self._recovery_complete = False
        self._active_lease_order_keys: set[str] = set()
        self._MAX_ACTIVE_LEASES = 1000
        self._lease_maintenance_task: Optional[asyncio.Task] = None
        self._background_tasks: set[asyncio.Task] = set()

        # Scout intelligence cache for dynamic execution adaptation
        self._scout_liquidity_regime = ""
        self._scout_execution_regime = ""
        self._scout_cache_time = 0.0
        self._scout_cache_ttl = 60.0  # refresh every minute

    async def _lease_maintenance(self):
        """
        Background task that periodically renews active execution leases.
        Ensures failover-safety: if this instance dies, leases expire and
        another instance can pick them up.
        Also prunes stale entries to prevent unbounded lease set growth.
        """
        while self.status == "running":
            try:
                stale_count = 0
                for order_key in list(self._active_lease_order_keys):
                    # Guard against unbounded growth: if set exceeds MAX, aggressively prune
                    if len(self._active_lease_order_keys) > self._MAX_ACTIVE_LEASES:
                        logger.warning(
                            f"Active lease set size {len(self._active_lease_order_keys)} "
                            f"exceeds MAX ({self._MAX_ACTIVE_LEASES}) — pruning {order_key}"
                        )
                        self._active_lease_order_keys.discard(order_key)
                        stale_count += 1
                        continue

                    renewed = await self.tracker.renew_lease(order_key)
                    if not renewed:
                        logger.warning(f"Lease lost for {order_key} — removing from active set")
                        self._active_lease_order_keys.discard(order_key)
                        stale_count += 1

                if stale_count > 0:
                    logger.info(f"Lease maintenance: pruned {stale_count} stale entries, "
                                f"{len(self._active_lease_order_keys)} active remaining")

                await asyncio.sleep(self.LEASE_RENEWAL_INTERVAL)
            except Exception as e:
                logger.debug(f"Lease maintenance cycle failed: {e}")
                await asyncio.sleep(5)

    def _track_background_task(self, task: asyncio.Task) -> asyncio.Task:
        self._background_tasks.add(task)

        def _finalize(done_task: asyncio.Task) -> None:
            self._background_tasks.discard(done_task)
            try:
                done_task.result()
            except asyncio.CancelledError:
                return
            except Exception:
                logger.exception(f"{self.name}: background task failed")

        task.add_done_callback(_finalize)
        return task

    async def _shutdown_background_tasks(self) -> None:
        pending = [task for task in self._background_tasks if not task.done()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        self._background_tasks.clear()

    async def run(self):
        logger.info(f"{self.name} starting up.")

        # 1. Recovery lock — block until reconciliation passes
        recovery_ok = await self.recovery.run_startup_reconciliation()
        self._recovery_complete = recovery_ok
        if not recovery_ok:
            logger.warning("Recovery reconciliation did not complete; execution remains locked.")

        # 1b. Start lease maintenance background task (distributed governance)
        self._lease_maintenance_task = self._track_background_task(
            asyncio.create_task(self._lease_maintenance())
        )

        # 1c. Detect and recover lost orders from this instance
        try:
            lost_orders = await self.tracker.get_lost_orders()
            if lost_orders:
                logger.warning(f"Found {len(lost_orders)} lost orders from this instance — marking for recovery")
                for lost_key in lost_orders:
                    await self.tracker.transition(
                        lost_key, OrderState.DEAD_LETTER,
                        error_message="lease_expired_failover_recovery"
                    )
        except Exception as e:
            logger.warning(f"Lost order recovery failed: {e}")

        # 2. Poll for validated strategies (in a real system this might be pub/sub driven)
        # We will subscribe to pubsub for signals
        pubsub = self._redis.pubsub()
        await pubsub.subscribe("strategy_signals")

        try:
            while self.status == "running":
                if not self._recovery_complete:
                    await asyncio.sleep(1)
                    continue

                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
                if message and message["type"] == "message":
                    try:
                        import json
                        data = json.loads(message["data"])
                        # Expecting {"type": "validated", "strategy_id": "...", ...}
                        if data.get("type") == "validated":
                            # Fetch full strategy from DB to ensure we have all specs
                            strategy_id = data.get("strategy_id")
                            if strategy_id:
                                # We need a method to get strategy by ID, or pass the dict directly
                                # Assuming data contains enough info, but ideally fetch from DB.
                                # Let's fetch it:
                                strategy = await self._get_strategy_by_id(strategy_id)
                                if strategy:
                                    # Create a task so we don't block the pubsub loop
                                    self._track_background_task(
                                        asyncio.create_task(self.execute(strategy))
                                    )
                    except Exception as e:
                        logger.error(f"Error processing signal: {e}")

                await asyncio.sleep(0.1)
        finally:
            await pubsub.unsubscribe()
            await self._shutdown_background_tasks()

    async def _get_strategy_by_id(self, strategy_id: str) -> Optional[dict]:
        from sqlalchemy.sql import text
        async with self.db.engine.connect() as conn:
            result = await conn.execute(
                text("SELECT id, name, parameters, status FROM strategies WHERE id = :id"),
                {"id": strategy_id}
            )
            row = result.fetchone()
            if row:
                return dict(row._mapping)
        return None

    def _build_trade_request(self, strategy: dict) -> dict:
        """Extract needed trade params from strategy spec."""
        params = strategy.get("parameters", {})
        if isinstance(params, str):
            import json
            try:
                params = json.loads(params)
            except Exception:
                params = {}

        symbol = params.get("symbol", "UNKNOWN")
        side = params.get("side", "buy").lower()
        # Default fallback sizing if not specified
        qty = params.get("qty")
        if qty is None:
            # We would typically ask risk controller or position manager for size,
            # but we fall back to a dummy fixed size if needed
            qty = 10.0

        return {
            "strategy_id": str(strategy.get("id")),
            "symbol": symbol,
            "side": side,
            "qty": float(qty),
            "price": 0.0, # Filled at market for now
        }

    async def execute(self, strategy: dict) -> bool:
        """
        The constitutional execution path.
        Returns True if fully executed (or already executed), False otherwise.
        """
        order_key = self.tracker.make_order_key(strategy)
        client_order_id = self.tracker.make_client_order_id(order_key)
        
        # 1. Idempotency gate
        if await self.tracker.is_processed(order_key):
            logger.info(f"Order {order_key} already processed.")
            return True

        # Acquire concurrency lock
        if not await self.tracker.acquire_lock(order_key):
            logger.warning(f"Order {order_key} currently locked by another process.")
            return False

        trade_req = self._build_trade_request(strategy)
        sid = trade_req["strategy_id"]
        sym = trade_req["symbol"]
        side = trade_req["side"]
        qty = trade_req["qty"]

        try:
            halted = await self.db.fetchval(
                """
                SELECT halted
                FROM risk_state
                WHERE scope = 'portfolio'
                LIMIT 1
                """
            )
            if halted in (True, 1, "1", "true", "TRUE"):
                raise Exception("KILL_SWITCH_ACTIVE")
        except Exception as exc:
            if str(exc) == "KILL_SWITCH_ACTIVE":
                await self.tracker.transition(order_key, OrderState.KILL_SWITCH_BLOCKED)
                logger.warning("Execution blocked by persisted kill switch.")
                return False
            raise

        # Track base state
        await self.tracker.transition(
            order_key, OrderState.SIGNAL_RECEIVED, 
            strategy_id=sid, symbol=sym, side=side, quantity=qty,
            client_order_id=client_order_id, broker_name=self.broker.broker_name
        )

        # Track lease for distributed governance
        self._active_lease_order_keys.add(order_key)

        try:
            # 2. Scout-aware dynamic adaptation (Phase 10.10)
            await self._refresh_scout_intelligence()

            # 2b. Reject execution if liquidity is dangerous
            if self._scout_liquidity_regime == "dangerous":
                await self.tracker.transition(
                    order_key, OrderState.RISK_REJECTED,
                    metadata={"reason": f"dangerous_liquidity_regime"}
                )
                logger.warning(f"Execution blocked: dangerous liquidity regime")
                return False

            # 2c. Kill switch check
            if await KillSwitch.is_active(self.db):
                await self.tracker.transition(order_key, OrderState.KILL_SWITCH_BLOCKED)
                return False

            # 3. Risk approval
            if not await self.risk.approve_trade(trade_req):
                await self.tracker.transition(
                    order_key, OrderState.RISK_REJECTED,
                    metadata={"reason": "risk_limits_breached"}
                )
                return False

            await self.tracker.transition(order_key, OrderState.RISK_APPROVED)

            # 4. Submit with retry (lease-renewal during long operations)
            order = await self._submit_with_retry(client_order_id, sym, side, qty)
            if not order:
                # Exhausted retries
                fail_reason = "submission_exhausted"
                await self.tracker.transition(
                    order_key, OrderState.DEAD_LETTER,
                    error_message=fail_reason
                )
                await self.dead_letter.record(
                    order_key, sid, sym, side, qty,
                    fail_reason, OrderState.RISK_APPROVED.value,
                    client_order_id=client_order_id,
                    severity="high"
                )
                return False

            broker_oid = order["id"]
            await self.tracker.transition(
                order_key, OrderState.BROKER_ACK,
                broker_order_id=broker_oid
            )

            # 5. Poll fill with partial awareness
            fill = await self._poll_fill(broker_oid)
            
            if fill.get("status") in ("filled", "closed"):
                fill_price = fill.get("filled_avg_price", 0.0)
                filled_qty = fill.get("filled_qty", qty)

                await self.tracker.transition(
                    order_key, OrderState.FILLED,
                    broker_order_id=broker_oid, price=fill_price, quantity=filled_qty,
                    metadata=fill
                )
                
                # 6. Open position
                await self.positions.open_position(
                    strategy_id=sid, symbol=sym, side=side,
                    qty=filled_qty, avg_price=fill_price,
                    broker_name=self.broker.broker_name
                )

                # 7. Write to paper_trades for historical backwards compat
                await self.db.save_paper_trade({
                    "strategy_id": sid,
                    "symbol": sym,
                    "side": side,
                    "quantity": filled_qty,
                    "price": fill_price,  # The expected or requested price could go here
                    "fill_price": fill_price,
                    "status": "filled",
                    "pnl": 0.0,
                })

                # 8. Record Lineage
                trace_id = await self.lineage.get_trace_by_strategy(sid)
                if trace_id:
                    await self.lineage.create_event(
                        trace_id=trace_id,
                        stage="execution",
                        status="completed",
                        actor="ExecutionGateway",
                        strategy_id=sid,
                        metadata={"broker": self.broker.broker_name, "qty": filled_qty, "price": fill_price}
                    )

                return True

            elif fill.get("status") == "partially_filled":
                await self.tracker.transition(
                    order_key, OrderState.PARTIALLY_FILLED,
                    broker_order_id=broker_oid,
                    metadata=fill
                )
                # Need human/system resolution
                await self.dead_letter.record(
                    order_key, sid, sym, side, qty,
                    "partial_fill_abandoned", OrderState.PARTIALLY_FILLED.value,
                    broker_order_id=broker_oid, client_order_id=client_order_id,
                    severity="medium", metadata=fill
                )
                return False
                
            else:
                # Handle timeout → cancel
                await self.tracker.transition(order_key, OrderState.FILL_TIMEOUT, broker_order_id=broker_oid)
                cancel_res = await self.broker.cancel_order(broker_oid)
                await self.tracker.transition(order_key, OrderState.CANCELLED, broker_order_id=broker_oid)
                return False

        except Exception as e:
            logger.error(f"Execution gateway error for {order_key}: {e}")
            await self.tracker.transition(order_key, OrderState.DEAD_LETTER, error_message=str(e))
            await self.dead_letter.record(
                order_key, sid, sym, side, qty,
                f"unhandled_exception: {e}", OrderState.DEAD_LETTER.value,
                client_order_id=client_order_id, severity="critical"
            )
            return False
        finally:
            self._active_lease_order_keys.discard(order_key)
            await self.tracker.release_lock(order_key)

    async def _refresh_scout_intelligence(self):
        """Refresh scout intelligence cache for dynamic adaptation."""
        import time
        now = time.time()
        if now - self._scout_cache_time < self._scout_cache_ttl:
            return
        try:
            scout = await self.db.get_latest_scout_intelligence()
            if not isinstance(scout, dict):
                return
            self._scout_liquidity_regime = scout.get("liquidity", {}).get("regime", "")
            self._scout_execution_regime = scout.get("execution", {}).get("regime", "")
            self._scout_cache_time = now
        except Exception:
            logger.debug(f"{self.name}: Scout refresh failed")

    def _scout_adjusted_qty(self, base_qty: float) -> float:
        """Reduce order size in stressed market conditions.
        
        Phase 26D: Also reduces sizing based on entropy when available.
        """
        adjusted = base_qty
        if self._scout_liquidity_regime == "thin":
            adjusted *= 0.5
        if self._scout_execution_regime in ("degraded", "stressed"):
            adjusted *= 0.75
        if self._scout_execution_regime == "unstable":
            adjusted *= 0.25
        # Phase 26D: Entropy-based sizing reduction (fetched from risk controller's state)
        try:
            entropy_val = self.risk._scout_entropy if hasattr(self.risk, '_scout_entropy') else 0.0
            if entropy_val > 0.7:
                adjusted *= 0.5  # 50% size reduction in high entropy
            elif entropy_val > 0.5:
                adjusted *= 0.75  # 25% size reduction in moderate entropy
        except Exception:
            pass
        return adjusted

    def _scout_adjusted_slippage(self) -> float:
        """Return widened slippage buffer in stressed conditions."""
        base_slippage = 10.0  # default 10 bps
        if self._scout_liquidity_regime == "thin":
            return base_slippage * 3.0
        if self._scout_execution_regime in ("degraded", "stressed"):
            return base_slippage * 2.0
        if self._scout_execution_regime == "unstable":
            return base_slippage * 5.0
        return base_slippage

    async def _submit_with_retry(self, client_order_id: str, symbol: str, side: str, qty: float, max_retries=3) -> Optional[dict]:
        for attempt in range(max_retries):
            try:
                # By passing client_order_id, the broker handles idempotency on its end
                order = await self.broker.submit_order(
                    client_order_id=client_order_id,
                    symbol=symbol,
                    side=side,
                    qty=qty
                )
                return order
            except Exception as e:
                logger.warning(f"Submit attempt {attempt+1} failed: {e}")
                await asyncio.sleep(2 ** attempt)
        return None

    async def _poll_fill(self, broker_order_id: str, timeout: int = 30) -> dict:
        start = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start < timeout:
            try:
                status = await self.broker.get_order_status(broker_order_id)
                if status.get("status") in ("filled", "partially_filled", "closed", "cancelled", "rejected"):
                    return status
            except Exception as e:
                logger.debug(f"Poll error: {e}")
            await asyncio.sleep(2)
        return {"id": broker_order_id, "status": "timeout"}
