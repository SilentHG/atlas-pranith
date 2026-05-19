import asyncio
import json
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from statistics import mean
from typing import Dict, Any, List, Optional

from atlas.core.agent_base import BaseAgent


class SelfImprovementAgent(BaseAgent):
    """
    L7 Meta-Learner
    ---------------------------------------------------
    PURPOSE:
    Empirically analyze recent strategy + paper/live performance,
    discover winning/losing strategic patterns,
    rank best environments,
    and feed intelligence back into:
        - system_logs
        - strategy_signals (Redis)
        - future Ideator guidance

    CORE QUESTIONS:
    1. What categories are winning?
    2. What categories are failing?
    3. Which timeframe is strongest?
    4. Which asset class is strongest?
    5. Which market regime is strongest?
    6. Which categories are cost traps?

    OUTPUT:
    Macro strategic intelligence, NOT structural motif intelligence.
    """

    name = "SelfImprovementAgent"
    agent_type = "meta_learner"
    layer = "L7"

    # Every 6 hours
    RUN_INTERVAL = 21600

    # Governance thresholds
    MIN_SAMPLE_SIZE = 5
    WIN_RATE_THRESHOLD = 0.55
    LOSING_WIN_RATE_THRESHOLD = 0.40
    COST_TRAP_PROFIT_FACTOR = 0.85

    def __init__(self, redis_client, db_client=None):
        super().__init__(
            name=self.name,
            agent_type=self.agent_type,
            layer=self.layer,
            redis_client=redis_client,
        )
        self.db_client = db_client

    # =========================================================
    # MAIN LOOP
    # =========================================================
    async def run(self):
        self.status = "running"

        while self.status == "running":
            try:
                await self._analyze_and_feedback()
            except Exception as e:
                await self._safe_log(
                    level="ERROR",
                    message=f"SelfImprovement cycle failed: {str(e)}",
                    metadata={"type": "self_improvement_error"},
                )

            await asyncio.sleep(self.RUN_INTERVAL)

    # =========================================================
    # MASTER ANALYSIS PIPELINE
    # =========================================================
    async def _analyze_and_feedback(self):
        if not self.db_client:
            await self._safe_log(
                level="WARNING",
                message="SelfImprovementAgent skipped — no db_client",
                metadata={"type": "configuration_warning"},
            )
            return

        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=7)

        # -----------------------------------------------------
        # PHASE 1 — FETCH PERFORMANCE DATA
        # -----------------------------------------------------
        performance_rows = await self._fetch_recent_strategy_performance(
            start_dt=start_dt,
            end_dt=end_dt,
        )

        if not performance_rows:
            await self._safe_log(
                level="INFO",
                message="No recent performance rows found",
                metadata={"type": "empty_analysis_window"},
            )
            return

        # -----------------------------------------------------
        # PHASE 2 — GROUP + SCORE
        # -----------------------------------------------------
        grouped = self._group_performance(performance_rows)

        insights = self._derive_insights(grouped)

        # -----------------------------------------------------
        # PHASE 3 — PERSIST
        # -----------------------------------------------------
        await self._persist_insights(insights)

        # -----------------------------------------------------
        # PHASE 4 — REDIS FEEDBACK
        # -----------------------------------------------------
        await self._publish_signals(insights)

    # =========================================================
    # DB FETCH
    # =========================================================
    async def _fetch_recent_strategy_performance(
        self,
        start_dt: datetime,
        end_dt: datetime,
    ) -> List[Dict[str, Any]]:
        """
        Pulls strategy + backtest + trade intelligence.
        Prefer short_window_score for drift resistance.
        """

        query = """
        SELECT
            s.id,
            s.strategy_name,
            s.tags,
            s.timeframe,
            s.asset_class,
            COALESCE(b.market_regime, 'unknown') AS market_regime,
            COALESCE(b.short_window_score, 0) AS score,
            COALESCE(b.win_rate, 0) AS win_rate,
            COALESCE(b.net_profit, 0) AS net_profit,
            COALESCE(b.sharpe_ratio, 0) AS sharpe_ratio,
            COALESCE(b.profit_factor, 0) AS profit_factor,
            COALESCE(b.total_trades, 0) AS total_trades,
            b.created_at
        FROM strategies s
        JOIN backtest_results b
            ON s.id = b.strategy_id
        WHERE b.created_at >= $1
          AND b.created_at <= $2
        """

        try:
            rows = await self.db_client.fetch(query, start_dt, end_dt)
            return [dict(r) for r in rows]
        except Exception as e:
            await self._safe_log(
                level="ERROR",
                message=f"Performance fetch failed: {str(e)}",
                metadata={"type": "db_fetch_error"},
            )
            return []

    # =========================================================
    # GROUPING ENGINE
    # =========================================================
    def _group_performance(
        self,
        rows: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """
        Groups by:
        - tags
        - timeframe
        - asset_class
        - regime
        """

        grouped = {
            "tags": defaultdict(list),
            "timeframe": defaultdict(list),
            "asset_class": defaultdict(list),
            "regime": defaultdict(list),
        }

        for row in rows:
            tags = row.get("tags") or []
            if isinstance(tags, str):
                tags = [t.strip() for t in tags.split(",") if t.strip()]

            for tag in tags:
                grouped["tags"][tag].append(row)

            grouped["timeframe"][row.get("timeframe", "unknown")].append(row)
            grouped["asset_class"][row.get("asset_class", "unknown")].append(row)
            grouped["regime"][row.get("market_regime", "unknown")].append(row)

        return grouped

    # =========================================================
    # INSIGHT DERIVATION
    # =========================================================
    def _derive_insights(
        self,
        grouped: Dict[str, Dict[str, List[Dict[str, Any]]]],
    ) -> Dict[str, Any]:

        winning_patterns = []
        losing_patterns = []
        cost_traps = []

        # -----------------------------
        # TAG LEVEL ANALYSIS
        # -----------------------------
        for tag, rows in grouped["tags"].items():
            if len(rows) < self.MIN_SAMPLE_SIZE:
                continue

            avg_win = mean(r["win_rate"] for r in rows)
            avg_score = mean(r["score"] for r in rows)
            avg_pf = mean(r["profit_factor"] for r in rows)

            if avg_win >= self.WIN_RATE_THRESHOLD and avg_score > 0:
                winning_patterns.append(
                    {
                        "tag": tag,
                        "sample": len(rows),
                        "avg_win_rate": round(avg_win, 4),
                        "avg_score": round(avg_score, 4),
                    }
                )

            elif avg_win <= self.LOSING_WIN_RATE_THRESHOLD:
                losing_patterns.append(
                    {
                        "tag": tag,
                        "sample": len(rows),
                        "avg_win_rate": round(avg_win, 4),
                        "avg_score": round(avg_score, 4),
                    }
                )

            if avg_pf < self.COST_TRAP_PROFIT_FACTOR:
                cost_traps.append(
                    {
                        "tag": tag,
                        "sample": len(rows),
                        "profit_factor": round(avg_pf, 4),
                    }
                )

        # -----------------------------
        # BEST ENVIRONMENT
        # -----------------------------
        best_timeframe = self._best_group(grouped["timeframe"])
        best_asset_class = self._best_group(grouped["asset_class"])
        best_regime = self._best_group(grouped["regime"])

        return {
            "analysis_window_days": 7,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "winning_patterns": sorted(
                winning_patterns,
                key=lambda x: x["avg_score"],
                reverse=True,
            )[:10],
            "losing_patterns": sorted(
                losing_patterns,
                key=lambda x: x["avg_score"],
            )[:10],
            "cost_traps": sorted(
                cost_traps,
                key=lambda x: x["profit_factor"],
            )[:10],
            "best_timeframe": best_timeframe,
            "best_asset_class": best_asset_class,
            "best_regime": best_regime,
        }

    # =========================================================
    # BEST GROUP PICKER
    # =========================================================
    def _best_group(self, group_map: Dict[str, List[Dict[str, Any]]]) -> str:
        best_key = "unknown"
        best_score = float("-inf")

        for key, rows in group_map.items():
            if not rows:
                continue

            avg_score = mean(r["score"] for r in rows)

            if avg_score > best_score:
                best_score = avg_score
                best_key = key

        return best_key

    # =========================================================
    # DB LOGGING
    # =========================================================
    async def _persist_insights(self, insights: Dict[str, Any]):
        await self._safe_log(
            level="INFO",
            message="improvement_insight generated",
            metadata={
                "type": "improvement_insight",
                **insights,
            },
        )

    # =========================================================
    # REDIS SIGNAL FEEDBACK
    # =========================================================
    async def _publish_signals(self, insights: Dict[str, Any]):
        signal = {
            "type": "improvement_insights",
            "generated_at": insights["generated_at"],
            "winning_patterns": [
                p["tag"] for p in insights["winning_patterns"]
            ],
            "losing_patterns": [
                p["tag"] for p in insights["losing_patterns"]
            ],
            "cost_traps": [
                p["tag"] for p in insights["cost_traps"]
            ],
            "best_timeframe": insights["best_timeframe"],
            "best_asset_class": insights["best_asset_class"],
            "best_regime": insights["best_regime"],
            "recommended_focus": self._build_recommended_focus(insights),
        }

        try:
            await self._redis.publish(
                "strategy_signals",
                json.dumps(signal),
            )
        except Exception as e:
            await self._safe_log(
                level="ERROR",
                message=f"Redis publish failed: {str(e)}",
                metadata={"type": "redis_publish_error"},
            )

    # =========================================================
    # RECOMMENDATION ENGINE
    # =========================================================
    def _build_recommended_focus(self, insights: Dict[str, Any]) -> str:
        winners = [p["tag"] for p in insights["winning_patterns"][:3]]

        if not winners:
            return "insufficient_data"

        return "|".join(winners)

    # =========================================================
    # SAFE LOGGER
    # =========================================================
    async def _safe_log(
        self,
        level: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        if not self.db_client:
            return

        try:
            await self.db_client.log(
                agent_id=self.agent_id,
                level=level,
                message=message,
                metadata=metadata or {},
            )
        except Exception:
            # Avoid recursive logging failure
            pass