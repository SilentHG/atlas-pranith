import asyncio
from loguru import logger
from redis.asyncio import Redis

from atlas.core.agent_base import BaseAgent
from atlas.core.messaging import MessagingClient, Channel
from atlas.data.storage.timescale_client import TimescaleClient

class RiskController(BaseAgent):
    name = "RiskController"
    agent_type = "risk"
    layer = "L4"

    LIMITS = {
        "max_portfolio_drawdown": -0.15,
        "max_single_position_pct": 0.10,
        "max_daily_loss_pct": -0.02,      # 2% daily halt (per Shakir spec)
        "max_weekly_loss_pct": -0.04,     # 4% weekly halt
        "max_open_positions": 10,
        "min_cash_reserve_pct": 0.20,
    }
    
    # We assume a fixed virtual portfolio value for % calculations since paper_trades 
    # doesn't store full portfolio equity curve yet.
    PORTFOLIO_VALUE = 100_000.0

    def __init__(self, redis_client: Redis, db_client: TimescaleClient):
        super().__init__(
            name=self.name,
            agent_type=self.agent_type,
            layer=self.layer,
            redis_client=redis_client
        )
        self.db_client = db_client
        self.messaging = MessagingClient(redis_client)
        self.RUN_INTERVAL = 30
        self._scout_entropy: float = 0.0
        self._scout_disagreement: float = 0.0
        self._entropy_cache_time: float = 0.0
        self._ENTROPY_CACHE_TTL: float = 300.0  # refresh every 5 min
        self._entropy_leverage_cap: float = 1.0  # normal leverage



    async def _refresh_entropy_context(self):
        """Phase 26D: Refresh scout entropy from influence log."""
        import time
        now = time.time()
        if now - self._entropy_cache_time < self._ENTROPY_CACHE_TTL:
            return
        try:
            summary = await self.db_client.get_scout_influence_summary(hours=24)
            if summary and len(summary) > 0:
                entropy_vals = [row.get('entropy_context', 0) or 0 for row in summary]
                self._scout_entropy = sum(entropy_vals) / max(1, len(entropy_vals))
                # Disagreement = standard deviation of entropy values
                if len(entropy_vals) > 1:
                    mean = self._scout_entropy
                    variance = sum((v - mean)**2 for v in entropy_vals) / len(entropy_vals)
                    self._scout_disagreement = min(1.0, variance * 4)  # normalize
            self._entropy_cache_time = now
        except Exception as e:
            logger.debug(f"{self.name}: Entropy refresh failed: {e}")
    
    async def run(self):
        while self.status == "running":
            try:
                # 1. Fetch all open paper_trades from DB
                open_trades = await self.db_client.get_open_paper_trades()
                
                # 2. Calculate stats
                daily_pnl = await self.db_client.get_daily_pnl()
                weekly_pnl = await self.db_client.get_weekly_pnl()
                
                open_positions_count = len(open_trades)
                
                # Simplistic drawdown and loss percent calculations
                daily_loss_pct = daily_pnl / self.PORTFOLIO_VALUE
                weekly_loss_pct = weekly_pnl / self.PORTFOLIO_VALUE
                
                # Calculate current_drawdown (just using daily loss as a proxy for now)
                current_drawdown = daily_loss_pct
                
                # 3. Check all limits
                # 4. If daily_loss breaches -2%: publish risk_alert + trigger kill switch
                if daily_loss_pct <= self.LIMITS["max_daily_loss_pct"]:
                    reason = f"Daily loss breached limit: {daily_loss_pct:.2%}"
                    await self._trigger_kill_switch(reason)                # 5. If weekly_loss breaches -4%: publish risk_alert + trigger kill switch
                if weekly_loss_pct <= self.LIMITS["max_weekly_loss_pct"]:
                    reason = f"Weekly loss breached limit: {weekly_loss_pct:.2%}"
                    await self._trigger_kill_switch(reason)

                # Phase 26D: Entropy-governed leverage and exposure limits
                await self._refresh_entropy_context()
                entropy = self._scout_entropy

                # High entropy -> reduce leverage cap
                if entropy > 0.7:
                    self._entropy_leverage_cap = 0.5  # 50% leverage cut
                    logger.info(f"{self.name}: High entropy ({entropy:.2f}) -> leverage cap 0.5")
                elif entropy > 0.5:
                    self._entropy_leverage_cap = 0.75  # 25% leverage cut
                else:
                    self._entropy_leverage_cap = 1.0

                # High disagreement -> wider diversification forced
                if self._scout_disagreement > 0.6:
                    self.LIMITS["max_single_position_pct"] = 0.05  # 5% max position
                else:
                    self.LIMITS["max_single_position_pct"] = 0.10  # 10% normal

                # 6. Log all checks
                await self.db_client.log(
                    self.agent_id,
                    "INFO",
                    "Risk checks completed",
                    {
                        "daily_pnl": daily_pnl,
                        "weekly_pnl": weekly_pnl,
                        "open_positions": open_positions_count
                    }
                )
            except Exception as e:
                logger.error(f"RiskController loop error: {e}")
            
            await asyncio.sleep(self.RUN_INTERVAL)

    async def _trigger_kill_switch(self, reason: str):
        logger.critical(f"Triggering KILL SWITCH: {reason}")
        await self.messaging.publish(Channel.RISK_ALERTS, {
            "type": "limit_breach",
            "reason": reason
        })
        await self.db_client.log(
            self.agent_id,
            "CRITICAL",
            "KILL SWITCH TRIGGERED",
            {"reason": reason}
        )

    async def approve_trade(self, trade_request: dict) -> bool:
        """
        Check position size, cash reserve, open positions count.
        Return True/False. Log every decision.
        """
        try:
            open_trades = await self.db_client.get_open_paper_trades()
            
            if len(open_trades) >= self.LIMITS["max_open_positions"]:
                logger.warning("Trade rejected: max_open_positions reached")
                return False
                
            # Assume trade_request has 'size' and 'price'
            trade_value = trade_request.get('size', 0) * trade_request.get('price', 0)
            if trade_value / self.PORTFOLIO_VALUE > self.LIMITS["max_single_position_pct"]:
                logger.warning("Trade rejected: max_single_position_pct breached")
                return False
                
            # Simplistic cash reserve check (assuming total open trade value)
            total_open_value = sum(float(t.get('quantity', 0)) * float(t.get('price', 0)) for t in open_trades)
            cash_reserve_pct = (self.PORTFOLIO_VALUE - total_open_value - trade_value) / self.PORTFOLIO_VALUE
            
            if cash_reserve_pct < self.LIMITS["min_cash_reserve_pct"]:
                logger.warning(f"Trade rejected: min_cash_reserve_pct breached (Current: {cash_reserve_pct:.2%})")
                return False
                
            return True
        except Exception as e:
            logger.error(f"Trade approval error: {e}")
            return False
