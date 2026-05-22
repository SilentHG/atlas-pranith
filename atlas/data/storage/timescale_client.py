import asyncio
import json
import re
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.sql import text
import pandas as pd

from atlas.core.scout_validation import validate_scout_payload
from atlas.core.serialization import normalize_db_params, safe_json_dumps


def _r4(v: float) -> float:
    return round(float(v), 4)


def _r6(v: float) -> float:
    return round(float(v), 6)


def _r8(v: float) -> float:
    return round(float(v), 8)


def _extract_table_name_from_insert(query: str) -> str:
    """Heuristic to extract the target table name from an INSERT statement."""
    m = re.search(r"insert\s+into\s+([a-zA-Z0-9_\.]+)", query, flags=re.IGNORECASE)
    if m:
        return m.group(1).split(".")[-1]
    return "unknown"

# Scout table -> scout_signals mirror configuration.
# Maps scout-specific insert targets to the columns needed by scout_signals.
_SCOUT_TABLE_MIRROR_MAP: dict[str, dict[str, Any]] = {
    "market_regime_memory": {
        "source": "regime_scout",
        "symbol_key": "symbol",
        "signal_type": "regime",
        "confidence_key": "confidence_score",
        "signal_data_keys": [
            "volatility_regime", "trend_regime", "liquidity_regime",
            "correlation_regime", "realized_volatility", "relative_volume",
            "atr_percentile", "compression_detected", "expansion_detected",
            "vwap_deviation_pct",
        ],
    },
    "liquidity_intelligence": {
        "source": "liquidity_scout",
        "symbol_key": "symbol",
        "signal_type": "liquidity",
        "confidence_key": "liquidity_score",
        "signal_data_keys": [
            "avg_spread_bps", "depth_imbalance", "slippage_risk",
            "market_impact_estimate", "liquidity_regime",
        ],
    },
    "correlation_memory": {
        "source": "correlation_scout",
        "symbol_key": None,
        "signal_type": "correlation",
        "confidence_key": "avg_pairwise_corr",
        "signal_data_keys": [
            "cluster_name", "dominant_factor", "risk_state",
            "symbols_analyzed", "correlation_spike_detected",
        ],
    },
    "execution_intelligence": {
        "source": "execution_scout",
        "symbol_key": "symbol",
        "signal_type": "execution",
        "confidence_key": "fill_quality_score",
        "signal_data_keys": [
            "avg_slippage_bps", "fill_latency_ms", "rejection_rate",
            "execution_regime", "sample_size",
        ],
    },
    "external_scout_memory": {
        "source": "source",
        "symbol_key": None,
        "signal_type": "external",
        "confidence_key": "hypothesis_score",
        "signal_data_keys": [
            "source_sub", "sentiment", "source_reliability",
            "signal_direction", "mentioned_tickers",
        ],
    },
}


class BarData(BaseModel):
    time: datetime
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    source: str
    interval: str
    asset_class: str = "crypto"


class OrderbookData(BaseModel):
    time: datetime
    symbol: str
    bids: Dict[str, Any]
    asks: Dict[str, Any]
    spread: float
    mid_price: float


class FeatureData(BaseModel):
    time: datetime
    symbol: str
    feature_name: str
    value: float


class AgentData(BaseModel):
    id: str
    name: str
    type: str
    layer: str
    status: str
    pid: Optional[int] = None
    last_heartbeat: datetime
    created_at: datetime
    metadata: Optional[Dict[str, Any]] = None


class LogData(BaseModel):
    time: datetime
    agent_id: str
    level: str
    message: str
    metadata: Optional[Dict[str, Any]] = None


class QuoteData(BaseModel):
    """Market data for quotes (Q.* stream)"""

    time: datetime
    symbol: str
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    bid_exchange: str
    ask_exchange: str
    source: str = "polygon"


class TradeData(BaseModel):
    """Market data for trades (T.* stream)"""

    time: datetime
    symbol: str
    price: float
    size: float
    side: str  # "buy", "sell", or "unknown"
    exchange: str
    source: str = "polygon"


class AggregateData(BaseModel):
    """Market data for aggregates (A.* stream) - used as L1 bars"""

    time: datetime
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float
    source: str = "polygon"
    interval: str = "1m"


class BinanceTradeData(BaseModel):
    """Crypto trade data from Binance @trade stream"""

    time: datetime
    symbol: str
    price: float
    quantity: float
    buyer_maker: bool  # True if buyer was the maker
    trade_id: int
    source: str = "binance"


class BinanceDepthData(BaseModel):
    """Crypto depth/orderbook data from Binance @depth20@100ms stream"""

    time: datetime
    symbol: str
    bids: Dict[str, Any]  # price: quantity mapping
    asks: Dict[str, Any]  # price: quantity mapping
    source: str = "binance"
    last_update_id: int = 0


class TimescaleClient:
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.engine = create_async_engine(self.db_url, echo=False)

    async def close(self) -> None:
        """Dispose the SQLAlchemy engine and release pooled connections."""
        await self.engine.dispose()

    async def get_mutation_leaderboard(self) -> list[dict]:
        """
        Retrieves the aggregated mutation intelligence (ranked by conversion rate and average delta).
        Provides IdeatorAgent with empirical priors of what mutations are currently working.
        """
        query = """
            SELECT
                mutation_type,
                COUNT(*)                                                AS total,
                COUNT(*) FILTER (WHERE improved = TRUE)                 AS improved_count,
                COUNT(*) FILTER (WHERE improved = FALSE)                AS failed_count,
                ROUND(AVG(parent_composite_score)::numeric, 2)          AS avg_parent_score,
                ROUND(AVG(child_composite_score)::numeric, 2)           AS avg_child_score,
                ROUND(AVG(score_delta)::numeric, 2)                     AS avg_score_delta,
                ROUND(
                    100.0 * COUNT(*) FILTER (WHERE improved = TRUE)
                    / NULLIF(COUNT(*) FILTER (WHERE improved IS NOT NULL), 0),
                    1
                )                                                       AS conversion_rate_pct
            FROM mutation_memory
            GROUP BY mutation_type
            ORDER BY conversion_rate_pct DESC NULLS LAST, avg_score_delta DESC NULLS LAST
        """
        try:
            async with self.engine.connect() as conn:
                res = await conn.execute(text(query))
                rows = res.fetchall()
                results = []
                for r in rows:
                    results.append({
                        "mutation_type":      r[0],
                        "total":              int(r[1]),
                        "improved":           int(r[2]),
                        "failed":             int(r[3]),
                        "avg_parent_score":   float(r[4]) if r[4] is not None else None,
                        "avg_child_score":    float(r[5]) if r[5] is not None else None,
                        "avg_score_delta":    float(r[6]) if r[6] is not None else None,
                        "conversion_rate":    float(r[7]) if r[7] is not None else None,
                    })
                return results
        except Exception as e:
            from loguru import logger
            logger.error(f"Error fetching mutation leaderboard: {e}")
            return []

    async def connect(self) -> None:
        """Verify the database connection and apply missing migrations."""
        async with self.engine.begin() as conn:
            await conn.execute(text("SELECT 1"))

            # Auto-migration: ensure schema additions from schema.sql are applied
            # backtest_trades
            await conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS backtest_trades (
                    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                    strategy_id UUID NOT NULL,
                    symbol TEXT NOT NULL,
                    entry_time TIMESTAMPTZ,
                    exit_time TIMESTAMPTZ,
                    entry_price NUMERIC,
                    exit_price NUMERIC,
                    side TEXT,
                    pnl NUMERIC,
                    pnl_pct NUMERIC,
                    bars_held INT,
                    exit_reason TEXT
                )
            """)
            )
            await conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_backtest_trades_strategy ON backtest_trades (strategy_id)"
                )
            )
            await conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_backtest_trades_entry ON backtest_trades (entry_time)"
                )
            )
            await conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_backtest_trades_symbol ON backtest_trades (symbol)"
                )
            )
            # market_data_l1 new columns
            await conn.execute(
                text(
                    "ALTER TABLE market_data_l1 ADD COLUMN IF NOT EXISTS asset_class TEXT NOT NULL DEFAULT 'crypto'"
                )
            )
            await conn.execute(
                text(
                    "ALTER TABLE market_data_l1 ADD COLUMN IF NOT EXISTS ingestion_time TIMESTAMPTZ DEFAULT NOW()"
                )
            )
            # strategies new columns
            await conn.execute(
                text("ALTER TABLE strategies ADD COLUMN IF NOT EXISTS prompt TEXT")
            )
            await conn.execute(
                text(
                    "ALTER TABLE strategies ADD COLUMN IF NOT EXISTS raw_response TEXT"
                )
            )
            await conn.execute(
                text(
                    "ALTER TABLE strategies ADD COLUMN IF NOT EXISTS normalized_strategy JSONB"
                )
            )
            await conn.execute(
                text(
                    "ALTER TABLE strategies ADD COLUMN IF NOT EXISTS compile_error TEXT"
                )
            )
            await conn.execute(
                text(
                    "ALTER TABLE strategies ADD COLUMN IF NOT EXISTS strategy_signature TEXT"
                )
            )
            # features_wide materialized view — auto-migrate if schema outdated
            has_price_vs_vwap = await conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'features_wide' AND column_name = 'price_vs_vwap_pct'"
                )
            )
            if has_price_vs_vwap.fetchone() is None:
                await conn.execute(
                    text("DROP MATERIALIZED VIEW IF EXISTS features_wide CASCADE")
                )
                await conn.execute(
                    text("""
                    CREATE MATERIALIZED VIEW features_wide AS
                    SELECT
                        time,
                        symbol,
                        MAX(CASE WHEN feature_name='returns'               THEN value END) AS returns,
                        MAX(CASE WHEN feature_name='log_returns'           THEN value END) AS log_returns,
                        MAX(CASE WHEN feature_name='rsi_14'                THEN value END) AS rsi_14,
                        MAX(CASE WHEN feature_name='macd'                  THEN value END) AS macd,
                        MAX(CASE WHEN feature_name='macd_signal'           THEN value END) AS macd_signal,
                        MAX(CASE WHEN feature_name='vwap'                  THEN value END) AS vwap,
                        MAX(CASE WHEN feature_name='sma_5'                 THEN value END) AS sma_5,
                        MAX(CASE WHEN feature_name='sma_20'                THEN value END) AS sma_20,
                        MAX(CASE WHEN feature_name='ema_5'                 THEN value END) AS ema_5,
                        MAX(CASE WHEN feature_name='ema_12'                THEN value END) AS ema_12,
                        MAX(CASE WHEN feature_name='ema_26'                THEN value END) AS ema_26,
                        MAX(CASE WHEN feature_name='bollinger_lower'       THEN value END) AS bollinger_lower,
                        MAX(CASE WHEN feature_name='bollinger_upper'       THEN value END) AS bollinger_upper,
                        MAX(CASE WHEN feature_name='rolling_volatility'    THEN value END) AS rolling_volatility,
                        MAX(CASE WHEN feature_name='price_vs_vwap_pct'     THEN value END) AS price_vs_vwap_pct,
                        MAX(CASE WHEN feature_name='ema_spread_pct'        THEN value END) AS ema_spread_pct,
                        MAX(CASE WHEN feature_name='relative_volume'       THEN value END) AS relative_volume,
                        MAX(CASE WHEN feature_name='bollinger_band_position' THEN value END) AS bollinger_band_position,
                        MAX(CASE WHEN feature_name='volatility_regime'     THEN value END) AS volatility_regime,
                        MAX(CASE WHEN feature_name='trend_strength'        THEN value END) AS trend_strength
                    FROM features
                    GROUP BY time, symbol
                """)
                )
            await conn.execute(
                text(
                    "CREATE UNIQUE INDEX IF NOT EXISTS idx_features_wide_time_symbol ON features_wide (time, symbol)"
                )
            )
            # pattern_memory — PatternAgent storage
            await conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS pattern_memory (
                    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
                    pattern_type TEXT NOT NULL,
                    archetype TEXT,
                    feature_family TEXT[],
                    asset_class TEXT,
                    timeframe TEXT,
                    regime TEXT,
                    composite_score_avg NUMERIC,
                    short_window_score_avg NUMERIC,
                    sharpe_avg NUMERIC,
                    win_rate_avg NUMERIC,
                    total_trades_avg NUMERIC,
                    cost_burden_avg NUMERIC,
                    sample_size INT DEFAULT 0,
                    confidence_score NUMERIC DEFAULT 0.0,
                    recommendation TEXT,
                    motif_details JSONB,
                    detected_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            )
            await conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_pattern_memory_type ON pattern_memory (pattern_type)"
                )
            )
            await conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_pattern_memory_archetype ON pattern_memory (archetype)"
                )
            )
            # lifecycle_events — Event Lineage Layer (Day 7b)
            await conn.execute(
                text("""
                CREATE TABLE IF NOT EXISTS lifecycle_events (
                    id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    strategy_id TEXT,
                    stage TEXT NOT NULL,
                    status TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    parent_event_id TEXT,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            )
            await conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_lifecycle_trace ON lifecycle_events (trace_id)"
                )
            )
            await conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_lifecycle_strategy ON lifecycle_events (strategy_id)"
                )
            )
            await conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_lifecycle_stage ON lifecycle_events (stage)"
                )
            )
            await conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS risk_state (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        scope TEXT NOT NULL,
                        strategy_id UUID NULL,
                        halted BOOLEAN NOT NULL DEFAULT FALSE,
                        reason TEXT,
                        triggered_by TEXT,
                        activated_at TIMESTAMPTZ,
                        released_at TIMESTAMPTZ,
                        metadata JSONB DEFAULT '{}'::jsonb,
                        created_at TIMESTAMPTZ DEFAULT NOW(),
                        updated_at TIMESTAMPTZ DEFAULT NOW()
                    )
                    """
                )
            )
            await conn.execute(
                text("CREATE UNIQUE INDEX IF NOT EXISTS idx_risk_state_scope ON risk_state (scope)")
            )
            await conn.execute(
                text(
                    """
                    INSERT INTO risk_state (scope, halted, reason)
                    VALUES ('portfolio', FALSE, 'initial_state')
                    ON CONFLICT DO NOTHING
                    """
                )
            )
            await conn.execute(
                text(
                    """
                    CREATE TABLE IF NOT EXISTS scout_quarantine (
                        id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                        source TEXT,
                        source_sub TEXT,
                        reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
                        raw_payload JSONB NOT NULL,
                        quarantined_at TIMESTAMPTZ DEFAULT NOW()
                    )
                    """
                )
            )
            await conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_scout_quarantine_source ON scout_quarantine (source)"
                )
            )
            await conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_scout_quarantine_time ON scout_quarantine (quarantined_at DESC)"
                )
            )
            # Fix system_logs.agent_id type (agents pass names like 'CoderAgent', not UUIDs)
            try:
                await conn.execute(text("""
                    ALTER TABLE system_logs ALTER COLUMN agent_id TYPE TEXT
                """))
            except Exception:
                pass  # Column may already be TEXT

            # Failed inserts dead-letter queue
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS failed_inserts (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    table_name TEXT,
                    query TEXT,
                    params JSONB,
                    reason TEXT,
                    inserted_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_failed_inserts_time ON failed_inserts (inserted_at DESC)"))
            # Add created_at column to backtest_results (needed by system_health, retirement, drift engines)
            await conn.execute(
                text("ALTER TABLE backtest_results ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_backtest_results_created ON backtest_results (created_at DESC)")
            )

            # trace_id column on strategies
            await conn.execute(
                text("ALTER TABLE strategies ADD COLUMN IF NOT EXISTS trace_id TEXT")
            )
            await conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS idx_strategies_trace ON strategies (trace_id)"
                )
            )

            # ================================================================
            # SCOUT NETWORK TABLES — Phase 10: Internal Scout Intelligence
            # ================================================================
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS market_regime_memory (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    symbol TEXT,
                    asset_class TEXT,
                    timeframe TEXT,
                    timestamp TIMESTAMPTZ NOT NULL,
                    volatility_regime TEXT,
                    trend_regime TEXT,
                    liquidity_regime TEXT,
                    correlation_regime TEXT,
                    atr_percentile NUMERIC,
                    realized_volatility NUMERIC,
                    relative_volume NUMERIC,
                    spread_bps NUMERIC,
                    compression_detected BOOLEAN,
                    expansion_detected BOOLEAN,
                    vwap_deviation_pct NUMERIC,
                    confidence_score NUMERIC DEFAULT 0.0,
                    metadata JSONB DEFAULT '{}'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_regime_memory_symbol ON market_regime_memory (symbol)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_regime_memory_timestamp ON market_regime_memory (timestamp DESC)"))

            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS liquidity_intelligence (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    symbol TEXT,
                    timestamp TIMESTAMPTZ NOT NULL,
                    avg_spread_bps NUMERIC,
                    depth_imbalance NUMERIC,
                    liquidity_score NUMERIC,
                    slippage_risk NUMERIC,
                    market_impact_estimate NUMERIC,
                    liquidity_regime TEXT,
                    metadata JSONB DEFAULT '{}'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_liquidity_symbol ON liquidity_intelligence (symbol)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_liquidity_timestamp ON liquidity_intelligence (timestamp DESC)"))

            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS correlation_memory (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    timestamp TIMESTAMPTZ NOT NULL,
                    cluster_name TEXT,
                    avg_pairwise_corr NUMERIC,
                    dominant_factor TEXT,
                    risk_state TEXT,
                    symbols_analyzed TEXT[],
                    top_correlated_pairs JSONB,
                    correlation_spike_detected BOOLEAN DEFAULT FALSE,
                    metadata JSONB DEFAULT '{}'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_correlation_timestamp ON correlation_memory (timestamp DESC)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_correlation_risk_state ON correlation_memory (risk_state)"))

            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS execution_intelligence (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    timestamp TIMESTAMPTZ NOT NULL,
                    symbol TEXT,
                    broker TEXT,
                    avg_slippage_bps NUMERIC,
                    fill_latency_ms NUMERIC,
                    rejection_rate NUMERIC,
                    fill_quality_score NUMERIC,
                    execution_regime TEXT,
                    sample_size INT DEFAULT 0,
                    metadata JSONB DEFAULT '{}'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_execution_symbol ON execution_intelligence (symbol)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_execution_timestamp ON execution_intelligence (timestamp DESC)"))

            # ================================================================
            # VALIDATION TABLES — Phase 11: Advanced Validation & Pattern Intelligence
            # ================================================================
            # walk_forward_analysis
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS walk_forward_analysis (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    strategy_id UUID NOT NULL,
                    walk_forward_score NUMERIC,
                    temporal_consistency NUMERIC,
                    regime_survival_score NUMERIC,
                    n_windows_survived INT,
                    n_windows_total INT,
                    per_window_metrics JSONB DEFAULT '[]'::jsonb,
                    analyzed_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (strategy_id)
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_walkforward_strategy ON walk_forward_analysis (strategy_id)"))

            # monte_carlo_analysis
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS monte_carlo_analysis (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    strategy_id UUID NOT NULL,
                    monte_carlo_survival_score NUMERIC,
                    expected_tail_drawdown NUMERIC,
                    probabilistic_sharpe NUMERIC,
                    ci_low_90pct NUMERIC,
                    ci_high_90pct NUMERIC,
                    n_simulations INT DEFAULT 0,
                    n_trades_input INT DEFAULT 0,
                    simulated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (strategy_id)
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_mc_strategy ON monte_carlo_analysis (strategy_id)"))

            # overfitting_analysis
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS overfitting_analysis (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    strategy_id UUID NOT NULL,
                    overfit_probability NUMERIC,
                    robustness_score NUMERIC,
                    parameter_stability_score NUMERIC,
                    shuffle_test_p_value NUMERIC,
                    noise_degradation_pct NUMERIC,
                    analyzed_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (strategy_id)
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_overfit_strategy ON overfitting_analysis (strategy_id)"))

            # regime_validation
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS regime_validation (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    strategy_id UUID NOT NULL,
                    regime_survival_map JSONB DEFAULT '{}'::jsonb,
                    regime_dependency_score NUMERIC,
                    regime_survival_score NUMERIC,
                    n_regimes_survived INT,
                    passes_min_regimes BOOLEAN DEFAULT FALSE,
                    over_specialized BOOLEAN DEFAULT FALSE,
                    validated_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (strategy_id)
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_regimeval_strategy ON regime_validation (strategy_id)"))

            # cost_stress_analysis
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS cost_stress_analysis (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    strategy_id UUID NOT NULL,
                    cost_survival_score NUMERIC,
                    max_survivable_multiplier NUMERIC,
                    profit_factor_degradation NUMERIC,
                    expectancy_degradation NUMERIC,
                    passes_min_survival BOOLEAN DEFAULT FALSE,
                    fragile_scalper_detected BOOLEAN DEFAULT FALSE,
                    tested_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (strategy_id)
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_coststress_strategy ON cost_stress_analysis (strategy_id)"))

            # feature_importance
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS feature_importance (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    feature_name TEXT NOT NULL,
                    feature_importance_score NUMERIC,
                    avg_composite_score NUMERIC,
                    std_composite_score NUMERIC,
                    n_uses INT DEFAULT 0,
                    survival_rate NUMERIC,
                    decay_score NUMERIC,
                    dominant_archetype TEXT,
                    archetype_focus_pct NUMERIC,
                    top_archetypes JSONB DEFAULT '{}'::jsonb,
                    computed_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (feature_name)
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_feature_importance_score ON feature_importance (feature_importance_score DESC)"))

            # ================================================================
            # PORTFOLIO TABLES — Phase 12: Portfolio Intelligence & Capital Realism
            # ================================================================
            # portfolio_intelligence
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS portfolio_intelligence (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    computed_at TIMESTAMPTZ NOT NULL,
                    n_strategies INT DEFAULT 0,
                    strategy_ids JSONB DEFAULT '[]'::jsonb,
                    correlation_matrix JSONB DEFAULT '[]'::jsonb,
                    covariance_matrix JSONB DEFAULT '[]'::jsonb,
                    cluster_map JSONB DEFAULT '{}'::jsonb,
                    efficiency_scores JSONB DEFAULT '[]'::jsonb,
                    optimal_allocations JSONB DEFAULT '[]'::jsonb,
                    regime_conditioned_weights JSONB DEFAULT '{}'::jsonb,
                    ensemble_survivability_score NUMERIC,
                    concentration_risk NUMERIC,
                    diversification_score NUMERIC,
                    metadata JSONB DEFAULT '{}'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_portfolio_computed ON portfolio_intelligence (computed_at DESC)"))

            # capital_allocation
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS capital_allocation (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    computed_at TIMESTAMPTZ NOT NULL,
                    n_strategies INT DEFAULT 0,
                    method TEXT,
                    final_allocations JSONB DEFAULT '[]'::jsonb,
                    total_exposure NUMERIC,
                    kelly_weights JSONB DEFAULT '[]'::jsonb,
                    vol_target_weights JSONB DEFAULT '[]'::jsonb,
                    risk_parity_weights JSONB DEFAULT '[]'::jsonb,
                    redistribution_signals JSONB DEFAULT '[]'::jsonb,
                    regime_applied JSONB DEFAULT '{}'::jsonb,
                    leverage_cap_applied NUMERIC DEFAULT 1.0,
                    metadata JSONB DEFAULT '{}'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_capital_allocation_computed ON capital_allocation (computed_at DESC)"))

            # ensemble_execution
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS ensemble_execution (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    executed_at TIMESTAMPTZ NOT NULL,
                    n_signals_processed INT DEFAULT 0,
                    n_trades_generated INT DEFAULT 0,
                    consensus_trades JSONB DEFAULT '[]'::jsonb,
                    strategy_weights_used JSONB DEFAULT '{}'::jsonb,
                    regime_context JSONB DEFAULT '{}'::jsonb,
                    metadata JSONB DEFAULT '{}'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ensemble_executed ON ensemble_execution (executed_at DESC)"))

            # execution_realism
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS execution_realism (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    simulated_at TIMESTAMPTZ NOT NULL,
                    n_trades_simulated INT DEFAULT 0,
                    avg_fill_probability NUMERIC,
                    avg_expected_slippage_bps NUMERIC,
                    avg_expected_partial_pct NUMERIC,
                    avg_simulated_latency_ms NUMERIC,
                    avg_market_impact_bps NUMERIC,
                    exhaustion_scenario JSONB DEFAULT '{}'::jsonb,
                    execution_degradation_score NUMERIC,
                    liquidity_state JSONB DEFAULT '{}'::jsonb,
                    simulated_fills JSONB DEFAULT '[]'::jsonb,
                    metadata JSONB DEFAULT '{}'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_exec_realism_simulated ON execution_realism (simulated_at DESC)"))

            # drift_detection
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS drift_detection (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    detected_at TIMESTAMPTZ NOT NULL,
                    feature_drift_score NUMERIC,
                    strategy_drift_score NUMERIC,
                    regime_drift_score NUMERIC,
                    execution_drift_score NUMERIC,
                    composite_severity NUMERIC,
                    n_strategies_monitored INT DEFAULT 0,
                    retirement_candidates JSONB DEFAULT '[]'::jsonb,
                    retrain_recommendations JSONB DEFAULT '[]'::jsonb,
                    metadata JSONB DEFAULT '{}'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_drift_detected ON drift_detection (detected_at DESC)"))

            # strategy_retirement
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS strategy_retirement (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    analyzed_at TIMESTAMPTZ NOT NULL,
                    n_strategies_analyzed INT DEFAULT 0,
                    n_active INT DEFAULT 0,
                    n_monitor INT DEFAULT 0,
                    n_retirement_pending INT DEFAULT 0,
                    n_retired INT DEFAULT 0,
                    lifecycle_states JSONB DEFAULT '{}'::jsonb,
                    retirement_recommendations JSONB DEFAULT '[]'::jsonb,
                    capital_withdrawal_signals JSONB DEFAULT '[]'::jsonb,
                    metadata JSONB DEFAULT '{}'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_retirement_analyzed ON strategy_retirement (analyzed_at DESC)"))

            # external_scout_memory — External Scout Network (Phase 12.9)
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS external_scout_memory (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    source TEXT NOT NULL,
                    source_sub TEXT,
                    source_reliability NUMERIC,
                    timestamp TIMESTAMPTZ NOT NULL,
                    sentiment NUMERIC,
                    mentioned_tickers JSONB DEFAULT '[]'::jsonb,
                    hypothesis_score NUMERIC,
                    signal_direction TEXT,
                    metadata JSONB DEFAULT '{}'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ext_scout_source ON external_scout_memory (source)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ext_scout_timestamp ON external_scout_memory (timestamp DESC)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ext_scout_score ON external_scout_memory (hypothesis_score DESC)"))

            # ================================================================
            # PHASE 13 — PRODUCTION GOVERNANCE & RELIABILITY TABLES
            # ================================================================
            # Event Store (append-only immutable event log)
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS event_store (
                    id TEXT PRIMARY KEY,
                    aggregate_id TEXT NOT NULL,
                    aggregate_type TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    event_version INT DEFAULT 1,
                    data JSONB DEFAULT '{}'::jsonb,
                    trace_id TEXT,
                    parent_event_id TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_aggregate ON event_store (aggregate_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_type ON event_store (event_type)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_trace ON event_store (trace_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_events_created ON event_store (created_at DESC)"))

            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS event_snapshots (
                    id TEXT PRIMARY KEY,
                    aggregate_id TEXT NOT NULL,
                    version INT NOT NULL,
                    state JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshots_agg_version ON event_snapshots (aggregate_id, version)"))

            # Audit Ledger (tamper-resistant, cryptographic hash chaining)
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS audit_ledger (
                    id TEXT PRIMARY KEY,
                    event_type TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    target_id TEXT,
                    action TEXT NOT NULL,
                    data_hash TEXT,
                    previous_hash TEXT,
                    trace_id TEXT,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_audit_type ON audit_ledger (event_type)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_audit_actor ON audit_ledger (actor)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_ledger (created_at DESC)"))

            # Deployment Governance
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS deployment_governance (
                    id TEXT PRIMARY KEY,
                    strategy_id TEXT NOT NULL,
                    mode TEXT NOT NULL DEFAULT 'paper',
                    status TEXT NOT NULL DEFAULT 'pending_approval',
                    proposed_by TEXT,
                    approved_by TEXT,
                    proposed_at TIMESTAMPTZ DEFAULT NOW(),
                    approved_at TIMESTAMPTZ,
                    activated_at TIMESTAMPTZ,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_deploy_strategy ON deployment_governance (strategy_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_deploy_status ON deployment_governance (status)"))

            # System Health
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS system_health (
                    id TEXT PRIMARY KEY,
                    checked_at TIMESTAMPTZ NOT NULL,
                    composite_score NUMERIC,
                    system_mode TEXT,
                    subsystem_scores JSONB DEFAULT '{}'::jsonb,
                    degraded_subsystems JSONB DEFAULT '[]'::jsonb,
                    n_degraded INT DEFAULT 0,
                    n_total INT DEFAULT 0
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_health_checked ON system_health (checked_at DESC)"))

            # Replay Integrity
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS replay_integrity (
                    id TEXT PRIMARY KEY,
                    checked_at TIMESTAMPTZ NOT NULL,
                    n_aggregates_checked INT DEFAULT 0,
                    n_events_checked INT DEFAULT 0,
                    integrity_score NUMERIC,
                    n_violations INT DEFAULT 0,
                    details JSONB DEFAULT '{}'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_replay_checked ON replay_integrity (checked_at DESC)"))

            # Systemic Risk
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS systemic_risk (
                    id TEXT PRIMARY KEY,
                    assessed_at TIMESTAMPTZ NOT NULL,
                    systemic_risk_score NUMERIC,
                    contagion_probability NUMERIC,
                    portfolio_fragility NUMERIC,
                    correlation_regime NUMERIC,
                    concentration_risk NUMERIC,
                    n_strategies_analyzed INT DEFAULT 0,
                    details JSONB DEFAULT '{}'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_sysrisk_assessed ON systemic_risk (assessed_at DESC)"))

            # Stress Test Results
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS stress_test_results (
                    id TEXT PRIMARY KEY,
                    tested_at TIMESTAMPTZ NOT NULL,
                    n_scenarios INT DEFAULT 0,
                    n_positions INT DEFAULT 0,
                    worst_scenario TEXT,
                    min_survival_probability NUMERIC,
                    max_drawdown NUMERIC,
                    avg_recovery_days NUMERIC,
                    scenario_results JSONB DEFAULT '[]'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_stress_tested ON stress_test_results (tested_at DESC)"))

            # Capital Preservation State
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS capital_preservation_state (
                    id TEXT PRIMARY KEY,
                    checked_at TIMESTAMPTZ NOT NULL,
                    drawdown_pct NUMERIC,
                    action_taken TEXT,
                    exposure_cut_ratio NUMERIC,
                    peak_value NUMERIC,
                    current_value NUMERIC,
                    total_pnl NUMERIC,
                    total_exposure NUMERIC
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_cap_pres_checked ON capital_preservation_state (checked_at DESC)"))

            # ================================================================
            # PHASE 14 — PORTFOLIO DURABILITY TABLES
            # ================================================================
            # Advanced Portfolio Optimization
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS advanced_portfolio_optimization (
                    id TEXT PRIMARY KEY,
                    optimized_at TIMESTAMPTZ NOT NULL,
                    method_used TEXT,
                    n_strategies INT DEFAULT 0,
                    final_allocations JSONB DEFAULT '[]'::jsonb,
                    method_scores JSONB DEFAULT '{}'::jsonb,
                    details JSONB DEFAULT '{}'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_adv_portfolio_opt ON advanced_portfolio_optimization (optimized_at DESC)"))

            # ================================================================
            # PHASE 15 — TRUE META-LEARNING TABLES
            # ================================================================
            # Prompt Templates
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS prompt_templates (
                    id TEXT PRIMARY KEY,
                    prompt_type TEXT NOT NULL DEFAULT 'ideator',
                    prompt_text TEXT NOT NULL,
                    archetype TEXT,
                    status TEXT DEFAULT 'active',
                    parent_prompt_id TEXT,
                    generation_count INT DEFAULT 0,
                    success_count INT DEFAULT 0,
                    effectiveness_score NUMERIC DEFAULT 0.0,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_prompt_type ON prompt_templates (prompt_type)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_prompt_effectiveness ON prompt_templates (effectiveness_score DESC)"))

            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS prompt_generation_log (
                    id TEXT PRIMARY KEY,
                    prompt_id TEXT,
                    strategy_id TEXT,
                    success BOOLEAN DEFAULT FALSE,
                    generation_score NUMERIC,
                    generated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_prompt_log_prompt ON prompt_generation_log (prompt_id)"))

            # Mutation Policy State
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS mutation_policy_state (
                    id TEXT PRIMARY KEY,
                    learned_at TIMESTAMPTZ NOT NULL,
                    mutation_weights JSONB DEFAULT '{}'::jsonb,
                    per_type_success_rates JSONB DEFAULT '{}'::jsonb,
                    n_observations INT DEFAULT 0,
                    details JSONB DEFAULT '{}'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_policy_learned ON mutation_policy_state (learned_at DESC)"))

            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS mutation_outcome_log (
                    id TEXT PRIMARY KEY,
                    mutation_type TEXT,
                    parent_strategy_id TEXT,
                    child_strategy_id TEXT,
                    outcome_score NUMERIC,
                    recorded_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_outcome_type ON mutation_outcome_log (mutation_type)"))

            # Agent Governance State
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS agent_governance_state (
                    id TEXT PRIMARY KEY,
                    assessed_at TIMESTAMPTZ NOT NULL,
                    n_agents_assessed INT DEFAULT 0,
                    agent_scores JSONB DEFAULT '{}'::jsonb,
                    throttled_agents JSONB DEFAULT '[]'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_gov_assessed ON agent_governance_state (assessed_at DESC)"))

            # ================================================================
            # PHASE 17 — OBSERVABILITY TABLES
            # ================================================================
            # Monitoring Metrics
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS monitoring_metrics (
                    id TEXT PRIMARY KEY,
                    recorded_at TIMESTAMPTZ NOT NULL,
                    counters JSONB DEFAULT '{}'::jsonb,
                    latencies JSONB DEFAULT '{}'::jsonb
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_metrics_recorded ON monitoring_metrics (recorded_at DESC)"))

            # Anomaly Observations
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS anomaly_observations (
                    id TEXT PRIMARY KEY,
                    observed_at TIMESTAMPTZ NOT NULL,
                    n_anomalies INT DEFAULT 0,
                    anomalies JSONB DEFAULT '[]'::jsonb,
                    severity NUMERIC DEFAULT 0
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_anomaly_observed ON anomaly_observations (observed_at DESC)"))

            # Feature evolution metadata column on feature_importance
            await conn.execute(text("ALTER TABLE feature_importance ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::jsonb"))

            # ================================================================
            # PHASE 19 — META-INTELLIGENCE ADVISORY TABLES
            # ================================================================
            # Meta Reasoning Log — MetaReasoningAgent outputs
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS meta_reasoning_log (
                    id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    advisory_type TEXT NOT NULL,
                    confidence NUMERIC DEFAULT 0.0,
                    reasoning_text TEXT,
                    system_state_snapshot JSONB DEFAULT '{}'::jsonb,
                    recommendations JSONB DEFAULT '[]'::jsonb,
                    advisory_only BOOLEAN DEFAULT TRUE,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_meta_reasoning_trace ON meta_reasoning_log (trace_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_meta_reasoning_type ON meta_reasoning_log (advisory_type)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_meta_reasoning_created ON meta_reasoning_log (created_at DESC)"))

            # Hypothesis Registry — structured research hypotheses
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS hypothesis_registry (
                    id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    statement TEXT NOT NULL,
                    observation_source TEXT,
                    testable_prediction TEXT,
                    confidence NUMERIC DEFAULT 0.5,
                    evidence_count INT DEFAULT 0,
                    contradiction_count INT DEFAULT 0,
                    regime_scope TEXT,
                    replay_score NUMERIC DEFAULT 0.0,
                    decay_rate NUMERIC DEFAULT 0.01,
                    status TEXT DEFAULT 'active',
                    evidence JSONB DEFAULT '[]'::jsonb,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    last_confirmed_at TIMESTAMPTZ,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_hypothesis_trace ON hypothesis_registry (trace_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_hypothesis_status ON hypothesis_registry (status)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_hypothesis_confidence ON hypothesis_registry (confidence DESC)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_hypothesis_created ON hypothesis_registry (created_at DESC)"))

            # Failure Analysis — postmortem reasoning on systemic failures
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS failure_analysis (
                    id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    analysis_type TEXT NOT NULL,
                    confidence NUMERIC DEFAULT 0.0,
                    root_causes JSONB DEFAULT '[]'::jsonb,
                    systemic_patterns JSONB DEFAULT '[]'::jsonb,
                    governance_recommendations JSONB DEFAULT '[]'::jsonb,
                    mutation_collapse_warnings JSONB DEFAULT '[]'::jsonb,
                    feature_saturation_alerts JSONB DEFAULT '[]'::jsonb,
                    n_failures_analyzed INT DEFAULT 0,
                    advisory_only BOOLEAN DEFAULT TRUE,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_failure_trace ON failure_analysis (trace_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_failure_type ON failure_analysis (analysis_type)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_failure_created ON failure_analysis (created_at DESC)"))

            # Mutation Policy Log — advisory history for mutation directions
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS mutation_policy_log (
                    id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    confidence NUMERIC DEFAULT 0.0,
                    advisory TEXT,
                    exploration_vs_exploitation TEXT,
                    entropy_metric NUMERIC DEFAULT 0.0,
                    diversification_advisory TEXT,
                    priority_weights JSONB DEFAULT '{}'::jsonb,
                    leaderboard_snapshot JSONB DEFAULT '{}'::jsonb,
                    advisory_only BOOLEAN DEFAULT TRUE,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_mutation_policy_trace ON mutation_policy_log (trace_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_mutation_policy_created ON mutation_policy_log (created_at DESC)"))

            # Scout Synthesis Log — synthesized scout intelligence
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS scout_synthesis_log (
                    id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    confidence NUMERIC DEFAULT 0.0,
                    contextual_summary TEXT,
                    scout_agreement_score NUMERIC DEFAULT 0.0,
                    scout_disagreement_areas JSONB DEFAULT '[]'::jsonb,
                    market_state_interpretation TEXT,
                    confidence_weights JSONB DEFAULT '{}'::jsonb,
                    source_signals JSONB DEFAULT '{}'::jsonb,
                    advisory_only BOOLEAN DEFAULT TRUE,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_scout_synth_trace ON scout_synthesis_log (trace_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_scout_synth_created ON scout_synthesis_log (created_at DESC)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_scout_synth_agreement ON scout_synthesis_log (scout_agreement_score DESC)"))

            # ================================================================
            # PHASE 21 — INSTITUTIONAL COPY TRADING TABLES
            # ================================================================
            # Copy Position State — leader vs follower portfolio snapshots
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS copy_position_state (
                    id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    leader_id TEXT NOT NULL,
                    follower_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    leader_qty NUMERIC DEFAULT 0,
                    follower_qty NUMERIC DEFAULT 0,
                    leader_avg_entry NUMERIC,
                    follower_avg_entry NUMERIC,
                    leader_exposure NUMERIC DEFAULT 0,
                    follower_exposure NUMERIC DEFAULT 0,
                    leader_unrealized_pnl NUMERIC DEFAULT 0,
                    follower_unrealized_pnl NUMERIC DEFAULT 0,
                    leader_realized_pnl NUMERIC DEFAULT 0,
                    follower_realized_pnl NUMERIC DEFAULT 0,
                    execution_latency_ms INT DEFAULT 0,
                    sync_quality_score NUMERIC DEFAULT 1.0,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    snapshot_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_copy_pos_leader ON copy_position_state (leader_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_copy_pos_follower ON copy_position_state (follower_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_copy_pos_snapshot ON copy_position_state (snapshot_at DESC)"))

            # Copy Drift Log — follower divergence tracking
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS copy_drift_log (
                    id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    leader_id TEXT NOT NULL,
                    follower_id TEXT NOT NULL,
                    drift_score NUMERIC DEFAULT 0.0,
                    drift_severity TEXT DEFAULT 'synchronized',
                    exposure_drift NUMERIC DEFAULT 0.0,
                    pnl_drift NUMERIC DEFAULT 0.0,
                    leverage_drift NUMERIC DEFAULT 0.0,
                    symbol_allocation_drift NUMERIC DEFAULT 0.0,
                    execution_timing_drift_ms INT DEFAULT 0,
                    slippage_amplification NUMERIC DEFAULT 0.0,
                    partial_fill_divergence NUMERIC DEFAULT 0.0,
                    sync_quality_score NUMERIC DEFAULT 1.0,
                    repair_recommendation TEXT,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    detected_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_copy_drift_leader ON copy_drift_log (leader_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_copy_drift_severity ON copy_drift_log (drift_severity)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_copy_drift_detected ON copy_drift_log (detected_at DESC)"))

            # Leader Health Metrics — leader governance scores
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS leader_health_metrics (
                    id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    leader_id TEXT NOT NULL,
                    health_score NUMERIC DEFAULT 1.0,
                    leader_state TEXT DEFAULT 'trusted',
                    drawdown_pct NUMERIC DEFAULT 0.0,
                    survivability_score NUMERIC DEFAULT 1.0,
                    execution_quality NUMERIC DEFAULT 1.0,
                    replay_consistency NUMERIC DEFAULT 1.0,
                    drift_stability NUMERIC DEFAULT 1.0,
                    portfolio_concentration NUMERIC DEFAULT 0.0,
                    slippage_amplification NUMERIC DEFAULT 0.0,
                    strategy_mortality_rate NUMERIC DEFAULT 0.0,
                    vol_adjusted_return NUMERIC DEFAULT 0.0,
                    n_followers INT DEFAULT 0,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    assessed_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_leader_health_leader ON leader_health_metrics (leader_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_leader_health_state ON leader_health_metrics (leader_state)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_leader_health_assessed ON leader_health_metrics (assessed_at DESC)"))

            # Follower Reconciliation — reconciliation reports
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS follower_reconciliation (
                    id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    leader_id TEXT NOT NULL,
                    follower_id TEXT NOT NULL,
                    reconciliation_type TEXT DEFAULT 'periodic',
                    n_positions_checked INT DEFAULT 0,
                    n_mismatches INT DEFAULT 0,
                    exposure_delta NUMERIC DEFAULT 0.0,
                    pnl_delta NUMERIC DEFAULT 0.0,
                    repair_actions JSONB DEFAULT '[]'::jsonb,
                    reconciliation_score NUMERIC DEFAULT 1.0,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    reconciled_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_recon_leader ON follower_reconciliation (leader_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_recon_follower ON follower_reconciliation (follower_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_recon_at ON follower_reconciliation (reconciled_at DESC)"))

            # Copy Overlap Metrics — portfolio overlap detection
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS copy_overlap_metrics (
                    id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    follower_id TEXT NOT NULL,
                    overlap_score NUMERIC DEFAULT 0.0,
                    concentration_risk NUMERIC DEFAULT 0.0,
                    diversification_penalty NUMERIC DEFAULT 0.0,
                    duplicated_exposure JSONB DEFAULT '[]'::jsonb,
                    correlated_leaders JSONB DEFAULT '[]'::jsonb,
                    hidden_concentration JSONB DEFAULT '{}'::jsonb,
                    n_leaders_analyzed INT DEFAULT 0,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    analyzed_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_overlap_follower ON copy_overlap_metrics (follower_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_overlap_analyzed ON copy_overlap_metrics (analyzed_at DESC)"))

            # Copy Failover Events — degradation mode tracking
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS copy_failover_events (
                    id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    follower_id TEXT NOT NULL,
                    leader_id TEXT,
                    event_type TEXT NOT NULL,
                    previous_mode TEXT,
                    new_mode TEXT NOT NULL,
                    trigger_reason TEXT,
                    recovery_action TEXT,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    occurred_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_failover_follower ON copy_failover_events (follower_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_failover_type ON copy_failover_events (event_type)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_failover_at ON copy_failover_events (occurred_at DESC)"))

            # Copy Replay Events — replayable copy execution log
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS copy_replay_events (
                    id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    leader_id TEXT,
                    follower_id TEXT,
                    leader_order_id TEXT,
                    follower_order_id TEXT,
                    symbol TEXT,
                    side TEXT,
                    leader_qty NUMERIC,
                    follower_qty NUMERIC,
                    leader_price NUMERIC,
                    follower_price NUMERIC,
                    slippage_bps NUMERIC DEFAULT 0,
                    execution_latency_ms INT DEFAULT 0,
                    drift_at_execution NUMERIC DEFAULT 0,
                    event_data JSONB DEFAULT '{}'::jsonb,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_copy_replay_trace ON copy_replay_events (trace_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_copy_replay_leader ON copy_replay_events (leader_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_copy_replay_created ON copy_replay_events (created_at DESC)"))

            # Copy Quality Metrics — institutional replication quality
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS copy_quality_metrics (
                    id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    leader_id TEXT,
                    follower_id TEXT,
                    replication_latency_ms NUMERIC DEFAULT 0,
                    sync_quality_score NUMERIC DEFAULT 1.0,
                    slippage_amplification NUMERIC DEFAULT 0.0,
                    execution_divergence NUMERIC DEFAULT 0.0,
                    pnl_divergence NUMERIC DEFAULT 0.0,
                    replay_integrity NUMERIC DEFAULT 1.0,
                    drift_accumulation NUMERIC DEFAULT 0.0,
                    follower_survivability NUMERIC DEFAULT 1.0,
                    n_events_analyzed INT DEFAULT 0,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    measured_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_copy_quality_leader ON copy_quality_metrics (leader_id)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_copy_quality_measured ON copy_quality_metrics (measured_at DESC)"))

            # ================================================================
            # PHASE 22 — SCOUT NETWORK HARDENING & OUTCOME ATTRIBUTION
            # ================================================================

            # Scout Signal Attribution — Links signals to execution/pnl outcomes
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS scout_signal_attribution (
                    id TEXT PRIMARY KEY,
                    signal_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    source_sub TEXT,
                    symbol TEXT,
                    executed_order_id TEXT,
                    hypothesis_id TEXT,
                    outcome_pnl NUMERIC DEFAULT 0.0,
                    attribution_score NUMERIC DEFAULT 0.0,
                    predictive_survivability NUMERIC DEFAULT 0.0,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    attributed_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_scout_attr_source ON scout_signal_attribution (source)"))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_scout_attr_signal ON scout_signal_attribution (signal_id)"))

            # Source Performance Log — Dynamic trust tracking over time
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS source_performance_log (
                    id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    source_sub TEXT,
                    dynamic_trust_score NUMERIC DEFAULT 0.5,
                    historical_accuracy NUMERIC DEFAULT 0.5,
                    n_profitable_signals INT DEFAULT 0,
                    n_loss_signals INT DEFAULT 0,
                    n_quarantined_signals INT DEFAULT 0,
                    recent_contradiction_rate NUMERIC DEFAULT 0.0,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    updated_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_source_perf_source ON source_performance_log (source, source_sub)"))
            
            # Scout Poison Quarantine — Anti-poisoning detection logs
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS scout_poison_quarantine (
                    id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    source TEXT NOT NULL,
                    source_sub TEXT,
                    violation_type TEXT NOT NULL,
                    severity_score NUMERIC DEFAULT 1.0,
                    affected_symbols JSONB DEFAULT '[]'::jsonb,
                    action_taken TEXT NOT NULL,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    detected_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_scout_poison_source ON scout_poison_quarantine (source)"))
            # ---------------------------------------------------------------
            # SCOUT_SIGNALS TABLE — anti_poisoning_engine dependency
            # ---------------------------------------------------------------
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS scout_signals (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    source TEXT,
                    symbol TEXT,
                    signal_type TEXT,
                    confidence_score NUMERIC DEFAULT 0.0,
                    signal_data JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_scout_signals_source ON scout_signals (source)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_scout_signals_symbol ON scout_signals (symbol)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_scout_signals_created ON scout_signals (created_at DESC)")
            )

            # ================================================================
            # PHASE 25 — SCOUT MIRROR DEBUG LOG (signal pipeline observability)
            # ================================================================
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS scout_mirror_debug_log (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    table_name TEXT,
                    source TEXT,
                    symbol TEXT,
                    signal_type TEXT,
                    confidence_score NUMERIC DEFAULT 0.0,
                    success BOOLEAN DEFAULT FALSE,
                    error_message TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_debug_log_created ON scout_mirror_debug_log (created_at DESC)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_debug_log_table ON scout_mirror_debug_log (table_name)")
            )

            # ================================================================
            # PHASE 26 -- SCOUT-INFLUENCE TRACKING TABLES
            # ================================================================

            # scout_influence_log -- records every scout influence event on agent behavior
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS scout_influence_log (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    trace_id TEXT,
                    source_scout TEXT NOT NULL,
                    target_agent TEXT NOT NULL,
                    influence_type TEXT NOT NULL,
                    influence_metric TEXT NOT NULL,
                    before_value NUMERIC,
                    after_value NUMERIC,
                    delta NUMERIC,
                    confidence NUMERIC DEFAULT 0.0,
                    regime_context TEXT,
                    entropy_context NUMERIC,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_scout_influence_source ON scout_influence_log (source_scout)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_scout_influence_target ON scout_influence_log (target_agent)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_scout_influence_created ON scout_influence_log (created_at DESC)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_scout_influence_type ON scout_influence_log (influence_type)")
            )

            # scout_economic_attribution -- full causal chain tracking
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS scout_economic_attribution (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    trace_id TEXT NOT NULL,
                    source_scout TEXT NOT NULL,
                    influence_type TEXT NOT NULL,
                    target_agent TEXT NOT NULL,
                    strategy_id TEXT,
                    strategy_name TEXT,
                    sharpe_contribution NUMERIC DEFAULT 0.0,
                    drawdown_contribution NUMERIC DEFAULT 0.0,
                    pnl_contribution NUMERIC DEFAULT 0.0,
                    win_rate_contribution NUMERIC DEFAULT 0.0,
                    attribution_weight NUMERIC DEFAULT 0.0,
                    survived_validation BOOLEAN DEFAULT FALSE,
                    regime_at_time TEXT,
                    entropy_at_time NUMERIC,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_economic_attr_source ON scout_economic_attribution (source_scout)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_economic_attr_trace ON scout_economic_attribution (trace_id)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_economic_attr_strategy ON scout_economic_attribution (strategy_id)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_economic_attr_created ON scout_economic_attribution (created_at DESC)")
            )

            # Record schema version
            await conn.execute(
                text("INSERT INTO schema_version (version, description) VALUES ('v26.0', 'Phase 26: Scout influence tracking, economic attribution, entropy governance') ON CONFLICT DO NOTHING")
            )


            # ================================================================
            # PHASE 24 — SCHEMA DRIFT FIXES (post-soak audit remediation)
            # ================================================================

            # ---------------------------------------------------------------
            # EVENT_STORE: The CREATE TABLE below defines a minimal set of
            # columns, but EventStore.append_event() writes a DIFFERENT set
            # of columns.  We must add ALL columns that the code expects.
            # ---------------------------------------------------------------

            # event_store.sequence — required by EventStore for ordering
            await conn.execute(
                text("ALTER TABLE event_store ADD COLUMN IF NOT EXISTS sequence INT DEFAULT 0")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_events_sequence ON event_store (sequence)")
            )

            # event_store.version — used by EventStore.append_event() (inserted as :version)
            await conn.execute(
                text("ALTER TABLE event_store ADD COLUMN IF NOT EXISTS version TEXT DEFAULT '1.0'")
            )

            # event_store.metadata — JSONB metadata column
            await conn.execute(
                text("ALTER TABLE event_store ADD COLUMN IF NOT EXISTS metadata JSONB DEFAULT '{}'::jsonb")
            )

            # event_store.hash_prev — previous event hash for chain integrity
            await conn.execute(
                text("ALTER TABLE event_store ADD COLUMN IF NOT EXISTS hash_prev TEXT")
            )

            # event_store.hash_self — self-hash for tamper detection
            await conn.execute(
                text("ALTER TABLE event_store ADD COLUMN IF NOT EXISTS hash_self TEXT")
            )

            # ---------------------------------------------------------------
            # AUDIT_LEDGER: The CREATE TABLE uses different column names than
            # AuditLedger.record().  We add ALL columns the code writes.
            # ---------------------------------------------------------------

            # audit_ledger.resource_type — used by AuditLedger.record()
            await conn.execute(
                text("ALTER TABLE audit_ledger ADD COLUMN IF NOT EXISTS resource_type TEXT")
            )

            # audit_ledger.resource_id — used by AuditLedger.record()
            await conn.execute(
                text("ALTER TABLE audit_ledger ADD COLUMN IF NOT EXISTS resource_id TEXT")
            )

            # audit_ledger.details — JSONB details column
            await conn.execute(
                text("ALTER TABLE audit_ledger ADD COLUMN IF NOT EXISTS details JSONB DEFAULT '{}'::jsonb")
            )

            # audit_ledger.severity — severity level column
            await conn.execute(
                text("ALTER TABLE audit_ledger ADD COLUMN IF NOT EXISTS severity TEXT DEFAULT 'info'")
            )

            # audit_ledger.hash_prev — previous entry hash
            await conn.execute(
                text("ALTER TABLE audit_ledger ADD COLUMN IF NOT EXISTS hash_prev TEXT")
            )

            # audit_ledger.hash_self — self-hash
            await conn.execute(
                text("ALTER TABLE audit_ledger ADD COLUMN IF NOT EXISTS hash_self TEXT")
            )

            # audit_ledger.sequence — deterministic ordering for per-aggregate hash chain verification
            await conn.execute(
                text("ALTER TABLE audit_ledger ADD COLUMN IF NOT EXISTS sequence INT DEFAULT 1")
            )
            await conn.execute(
                text("DROP INDEX IF EXISTS idx_audit_sequence")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_audit_trace_sequence ON audit_ledger (trace_id, sequence)")
            )

            # paper_trades.id — missing UUID primary key
            await conn.execute(
                text("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS id UUID DEFAULT gen_random_uuid()")
            )
            # paper_trades.qty — generated column backed by quantity so both column names work
            await conn.execute(
                text("ALTER TABLE paper_trades ADD COLUMN IF NOT EXISTS qty NUMERIC GENERATED ALWAYS AS (quantity) STORED")
            )

            # strategies.mutation_type — used by mutation analysis queries
            await conn.execute(
                text("ALTER TABLE strategies ADD COLUMN IF NOT EXISTS mutation_type TEXT")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_strategies_mutation_type ON strategies (mutation_type)")
            )

            # lifecycle_events.agent_name — alias for actor, used by metrics queries
            await conn.execute(
                text("ALTER TABLE lifecycle_events ADD COLUMN IF NOT EXISTS agent_name TEXT")
            )

            # external_scout_memory.details — used by scout performance tracking
            await conn.execute(
                text("ALTER TABLE external_scout_memory ADD COLUMN IF NOT EXISTS details TEXT")
            )

            # strategies.generation_batch — missing column used by IdeatorV2
            await conn.execute(
                text("ALTER TABLE strategies ADD COLUMN IF NOT EXISTS generation_batch TEXT")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_strategies_batch ON strategies (generation_batch)")
            )

            # ================================================================
            # SCHEMA VERSIONING — Phase 24: Post-migration validation
            # ================================================================
            from loguru import logger
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS schema_version (
                    version TEXT PRIMARY KEY,
                    applied_at TIMESTAMPTZ DEFAULT NOW(),
                    description TEXT,
                    checksum TEXT
                )
            """))
            await conn.execute(
                text("INSERT INTO schema_version (version, description) VALUES ('v24.0', 'Phase 24: Schema drift remediation, column alignment, start-up validation') ON CONFLICT DO NOTHING")
            )

            # Verify critical columns that are required by the code
            # Runs AFTER all migrations to avoid false-positive warnings
            _required_columns = [
                ("event_store", "version", "EventStore.append_event"),
                ("event_store", "metadata", "EventStore.append_event"),
                ("event_store", "hash_prev", "EventStore hash chain"),
                ("event_store", "hash_self", "EventStore hash chain"),
                ("event_store", "sequence", "EventStore ordering"),
                ("audit_ledger", "resource_type", "AuditLedger.record"),
                ("audit_ledger", "resource_id", "AuditLedger.record"),
                ("audit_ledger", "details", "AuditLedger.record"),
                ("audit_ledger", "severity", "AuditLedger.record"),
                ("audit_ledger", "hash_prev", "AuditLedger hash chain"),
                ("audit_ledger", "hash_self", "AuditLedger hash chain"),
                ("audit_ledger", "sequence", "AuditLedger hash chain ordering"),
                ("external_scout_memory", "details", "Scout performance tracking"),
                ("strategies", "mutation_type", "Mutation analysis"),
                ("strategies", "generation_batch", "IdeatorV2"),
                ("lifecycle_events", "agent_name", "Metrics queries"),
                ("correlation_memory", "correlation_value", "SystemicRiskEngine"),
                ("paper_trades", "id", "Primary key"),
                ("backtest_results", "created_at", "System health, retirement"),
            ]
            _missing = []
            for _table, _col, _usage in _required_columns:
                r = await conn.execute(
                    text("SELECT column_name FROM information_schema.columns WHERE table_name = :t AND column_name = :c"),
                    {"t": _table, "c": _col},
                )
                if r.fetchone() is None:
                    _missing.append(f"{_table}.{_col} ({_usage})")
            if _missing:
                logger.warning(f"Schema validation: {len(_missing)} critical columns still missing — {', '.join(_missing[:5])}...")
            else:
                logger.info("Schema validation: All critical columns present ✅")

    @staticmethod
    def _strip_pg_casts(query: str) -> str:
        return re.sub(r':([a-zA-Z_][a-zA-Z0-9_]*)::[a-zA-Z_]+', r':', query)

    async def _execute_insert(self, query: str, params: Dict[str, Any]) -> None:
        query = self._strip_pg_casts(query)
        normalized_params = normalize_db_params(params)
        query_l = query.strip().lower()
        if query_l.startswith("insert into external_scout_memory"):
            validation = validate_scout_payload(normalized_params)
            if not validation.valid:
                await self._quarantine_scout_payload(validation.normalized_payload, validation.reasons)
                return
            normalized_params = validation.normalized_payload
        async with self.engine.begin() as conn:
            try:
                res = await conn.execute(text(query), normalized_params)
                # rowcount may be None depending on driver; treat None as unknown
                rc = getattr(res, "rowcount", None)
                if rc == 0:
                    # Insert silently affected 0 rows — capture dead-letter for investigation
                    table_name = _extract_table_name_from_insert(query)
                    await conn.execute(
                        text("""
                        INSERT INTO failed_inserts (table_name, query, params, reason)
                        VALUES (:table_name, :query, CAST(:params AS jsonb), :reason)
                        """),
                        {
                            "table_name": table_name,
                            "query": query,
                            "params": safe_json_dumps(normalized_params),
                            "reason": "zero_rowcount",
                        },
                    )

                else:
                    # Successful insert -- mirror to scout_signals if applicable
                    table_name = _extract_table_name_from_insert(query)
                    await self._mirror_to_scout_signals(table_name, normalized_params)

            except Exception as e:
                # On exception, persist the failed insert for offline debugging and continue
                try:
                    table_name = _extract_table_name_from_insert(query)
                    await conn.execute(
                        text("""
                        INSERT INTO failed_inserts (table_name, query, params, reason)
                        VALUES (:table_name, :query, CAST(:params AS jsonb), :reason)
                        """),
                        {
                            "table_name": table_name,
                            "query": query,
                            "params": safe_json_dumps(normalized_params),
                            "reason": str(e),
                        },
                    )
                except Exception:
                    # Last resort: log and move on
                    from loguru import logger

                    logger.error(f"Failed to record failed_insert for query: {e}")
                from loguru import logger

                logger.warning(f"DB insert failed: {e}")

    async def _quarantine_scout_payload(self, payload: dict[str, Any], reasons: list[str]) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    """
                    INSERT INTO scout_quarantine (source, source_sub, reasons, raw_payload)
                    VALUES (:source, :source_sub, CAST(:reasons AS jsonb), CAST(:raw_payload AS jsonb))
                    """
                ),
                {
                    "source": payload.get("source"),
                    "source_sub": payload.get("source_sub") or payload.get("signal_type"),
                    "reasons": safe_json_dumps(reasons),
                    "raw_payload": safe_json_dumps(payload),
                },
            )

    async def _mirror_to_scout_signals(self, table_name: str, params: dict[str, Any]) -> None:
        """Auto-mirror scout inserts to scout_signals for pipeline consumption."""
        config = _SCOUT_TABLE_MIRROR_MAP.get(table_name)
        if config is None:
            return

        # Use config source value as a literal static name, unless the param key
        # actually exists in the insert params (e.g., external_scout_memory has a
        # dynamic "source" field like "youtube" / "discord").
        if config["source"] in params:
            raw_source = params[config["source"]]
        else:
            raw_source = config["source"]
        symbol = None
        if config.get("symbol_key"):
            symbol = params.get(config["symbol_key"])

        confidence = None
        if config.get("confidence_key"):
            try:
                confidence = float(params.get(config["confidence_key"], 0) or 0)
            except (TypeError, ValueError):
                confidence = 0.0

        signal_data = {}
        for key in config.get("signal_data_keys", []):
            if key in params:
                val = params[key]
                if isinstance(val, (list, dict)):
                    import json
                    signal_data[key] = json.loads(json.dumps(val, default=str))
                else:
                    signal_data[key] = val

        insert_query = """
            INSERT INTO scout_signals (source, symbol, signal_type, confidence_score, signal_data)
            VALUES (:source, :symbol, :signal_type, :confidence_score, CAST(:signal_data AS jsonb))
        """
        async with self.engine.begin() as conn:
            success = False
            error_msg = None
            try:
                await conn.execute(
                    text(insert_query),
                    {
                        "source": str(raw_source),
                        "symbol": str(symbol) if symbol else None,
                        "signal_type": config["signal_type"],
                        "confidence_score": confidence,
                        "signal_data": safe_json_dumps(signal_data),
                    },
                )
                success = True
            except Exception as e:
                error_msg = str(e)[:500]
                # Mirror failures are non-fatal -- do not poison the primary insert

            # Phase 25 Step 3: Debug mode — log every mirror attempt
            try:
                await conn.execute(
                    text("""
                        INSERT INTO scout_mirror_debug_log
                            (table_name, source, symbol, signal_type, confidence_score, success, error_message)
                        VALUES (:tn, :src, :sym, :st, :conf, :ok, :err)
                    """),
                    {
                        "tn": table_name,
                        "src": str(raw_source),
                        "sym": str(symbol) if symbol else None,
                        "st": config["signal_type"],
                        "conf": confidence,
                        "ok": success,
                        "err": error_msg,
                    },
                )
            except Exception as log_e:
                from loguru import logger
                logger.debug(f"Mirror debug log insertion failed: {log_e}")

    async def fetchval(self, query: str, params: Optional[Dict[str, Any]] = None):
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), params or {})
            return result.scalar()

    # ================================================================
    # PHASE 26 -- SCOUT INFLUENCE TRACKING HELPERS
    # ================================================================

    async def log_scout_influence(
        self,
        source_scout: str,
        target_agent: str,
        influence_type: str,
        influence_metric: str,
        before_value: float | None = None,
        after_value: float | None = None,
        delta: float | None = None,
        confidence: float = 0.0,
        regime_context: str | None = None,
        entropy_context: float | None = None,
        metadata: dict | None = None,
        trace_id: str | None = None,
    ) -> None:
        """Log a scout influence event for Phase 26 coupling analysis."""
        import uuid
        try:
            await self._execute_insert(
                """
                INSERT INTO scout_influence_log
                    (trace_id, source_scout, target_agent, influence_type, influence_metric,
                     before_value, after_value, delta, confidence, regime_context,
                     entropy_context, metadata)
                VALUES
                    (:trace_id, :source, :target, :itype, :imetric,
                     :before, :after, :delta, :conf, :regime,
                     :entropy, CAST(:meta AS jsonb))
                """,
                {
                    "trace_id": trace_id or uuid.uuid4().hex[:16],
                    "source": source_scout,
                    "target": target_agent,
                    "itype": influence_type,
                    "imetric": influence_metric,
                    "before": before_value,
                    "after": after_value,
                    "delta": delta,
                    "conf": confidence,
                    "regime": regime_context,
                    "entropy": entropy_context,
                    "meta": safe_json_dumps(metadata or {}),
                },
            )
        except Exception as e:
            from loguru import logger
            logger.debug(f"log_scout_influence failed: {e}")

    async def log_economic_attribution(
        self,
        source_scout: str,
        influence_type: str,
        target_agent: str,
        strategy_id: str | None = None,
        strategy_name: str | None = None,
        sharpe_contribution: float = 0.0,
        drawdown_contribution: float = 0.0,
        pnl_contribution: float = 0.0,
        win_rate_contribution: float = 0.0,
        attribution_weight: float = 0.0,
        survived_validation: bool = False,
        regime_at_time: str | None = None,
        entropy_at_time: float | None = None,
        metadata: dict | None = None,
    ) -> None:
        """Record full economic attribution for a scout-influenced decision."""
        import uuid
        trace_id = uuid.uuid4().hex[:16]
        try:
            await self._execute_insert(
                """
                INSERT INTO scout_economic_attribution
                    (trace_id, source_scout, influence_type, target_agent,
                     strategy_id, strategy_name,
                     sharpe_contribution, drawdown_contribution, pnl_contribution,
                     win_rate_contribution, attribution_weight,
                     survived_validation, regime_at_time, entropy_at_time, metadata)
                VALUES
                    (:trace_id, :source, :itype, :target,
                     :sid, :sname,
                     :sharpe, :dd, :pnl,
                     :wr, :weight,
                     :survived, :regime, :entropy, CAST(:meta AS jsonb))
                """,
                {
                    "trace_id": trace_id,
                    "source": source_scout,
                    "itype": influence_type,
                    "target": target_agent,
                    "sid": strategy_id,
                    "sname": strategy_name,
                    "sharpe": sharpe_contribution,
                    "dd": drawdown_contribution,
                    "pnl": pnl_contribution,
                    "wr": win_rate_contribution,
                    "weight": attribution_weight,
                    "survived": survived_validation,
                    "regime": regime_at_time,
                    "entropy": entropy_at_time,
                    "meta": safe_json_dumps(metadata or {}),
                },
            )
        except Exception as e:
            from loguru import logger
            logger.debug(f"log_economic_attribution failed: {e}")

    async def get_scout_influence_summary(self, source_scout: str | None = None) -> list[dict]:
        """Get summary of scout influence events."""
        import json
        async with self.engine.connect() as conn:
            if source_scout:
                r = await conn.execute(
                    text("""
                        SELECT source_scout, target_agent, influence_type, influence_metric,
                               COUNT(*) as event_count,
                               AVG(ABS(COALESCE(delta, 0))) as avg_abs_delta,
                               AVG(COALESCE(confidence, 0)) as avg_confidence,
                               MIN(created_at) as first_event,
                               MAX(created_at) as last_event
                        FROM scout_influence_log
                        WHERE source_scout = :src
                        GROUP BY source_scout, target_agent, influence_type, influence_metric
                        ORDER BY event_count DESC
                    """),
                    {"src": source_scout},
                )
            else:
                r = await conn.execute(
                    text("""
                        SELECT source_scout, target_agent, influence_type, influence_metric,
                               COUNT(*) as event_count,
                               AVG(ABS(COALESCE(delta, 0))) as avg_abs_delta,
                               AVG(COALESCE(confidence, 0)) as avg_confidence
                        FROM scout_influence_log
                        GROUP BY source_scout, target_agent, influence_type, influence_metric
                        ORDER BY event_count DESC
                    """),
                )
            results = []
            for row in r.fetchall():
                results.append({
                    "source_scout": row[0],
                    "target_agent": row[1],
                    "influence_type": row[2],
                    "influence_metric": row[3],
                    "event_count": row[4],
                    "avg_abs_delta": float(row[5] or 0),
                    "avg_confidence": float(row[6] or 0),
                })
            return results

    async def get_economic_attribution(
        self, source_scout: str | None = None, strategy_id: str | None = None
    ) -> list[dict]:
        """Get economic attribution records."""
        import json
        async with self.engine.connect() as conn:
            conditions = []
            params = {}
            if source_scout:
                conditions.append("source_scout = :src")
                params["src"] = source_scout
            if strategy_id:
                conditions.append("strategy_id = :sid")
                params["sid"] = strategy_id
            where_clause = " AND ".join(conditions) if conditions else "TRUE"
            r = await conn.execute(
                text(f"""
                    SELECT source_scout, influence_type, target_agent,
                           strategy_name, sharpe_contribution, drawdown_contribution,
                           pnl_contribution, win_rate_contribution, attribution_weight,
                           survived_validation, regime_at_time, entropy_at_time, created_at
                    FROM scout_economic_attribution
                    WHERE {where_clause}
                    ORDER BY created_at DESC
                    LIMIT 100
                """),
                params,
            )
            results = []
            for row in r.fetchall():
                results.append({
                    "source_scout": row[0],
                    "influence_type": row[1],
                    "target_agent": row[2],
                    "strategy_name": row[3],
                    "sharpe_contribution": float(row[4] or 0),
                    "drawdown_contribution": float(row[5] or 0),
                    "pnl_contribution": float(row[6] or 0),
                    "win_rate_contribution": float(row[7] or 0),
                    "attribution_weight": float(row[8] or 0),
                    "survived_validation": bool(row[9]),
                    "regime_at_time": row[10],
                    "entropy_at_time": float(row[11] or 0) if row[11] else None,
                })
            return results



    async def write_bars(self, symbol: str, data: BarData) -> None:
        """Insert to market_data_l1 (idempotent — skips duplicates)"""
        is_crypto = data.asset_class == "crypto"
        query = """
            INSERT INTO market_data_l1 (time, symbol, open, high, low, close, volume, source, interval, asset_class, ingestion_time)
            VALUES (:time, :symbol, :open, :high, :low, :close, :volume, :source, :interval, :asset_class, NOW())
            ON CONFLICT (time, symbol) DO NOTHING
        """
        params = data.model_dump()
        params["symbol"] = symbol
        params["open"] = _r4(params["open"])
        params["high"] = _r4(params["high"])
        params["low"] = _r4(params["low"])
        params["close"] = _r4(params["close"])
        params["volume"] = _r6(params["volume"]) if is_crypto else _r4(params["volume"])
        await self._execute_insert(query, params)

    async def write_quote(self, data: QuoteData) -> None:
        """Insert quote data to market_data_l2 as orderbook snapshot"""
        query = """
            INSERT INTO market_data_l2 (time, symbol, bids, asks, spread, mid_price)
            VALUES (:time, :symbol, :bids, :asks, :spread, :mid_price)
        """
        spread = _r6(data.ask - data.bid)
        mid_price = _r6((data.bid + data.ask) / 2)

        params = {
            "time": data.time,
            "symbol": data.symbol,
            "bids": json.dumps({str(_r6(data.bid)): data.bid_size}),
            "asks": json.dumps({str(_r6(data.ask)): data.ask_size}),
            "spread": spread,
            "mid_price": mid_price,
        }
        await self._execute_insert(query, params)

    async def write_trade(self, data: TradeData) -> None:
        """Insert trade data to order_flow table"""
        query = """
            INSERT INTO order_flow (time, symbol, price, size, side, aggressor)
            VALUES (:time, :symbol, :price, :size, :side, :aggressor)
        """
        params = {
            "time": data.time,
            "symbol": data.symbol,
            "price": _r6(data.price),
            "size": _r4(data.size),
            "side": data.side,
            "aggressor": data.exchange,
        }
        await self._execute_insert(query, params)

    async def write_aggregate(self, data: AggregateData) -> None:
        """Insert aggregate/OHLCV data to market_data_l1"""
        asset_class = "crypto" if data.source == "binance" else "equity"
        is_crypto = asset_class == "crypto"
        query = """
            INSERT INTO market_data_l1 (time, symbol, open, high, low, close, volume, source, interval, asset_class, ingestion_time)
            VALUES (:time, :symbol, :open, :high, :low, :close, :volume, :source, :interval, :asset_class, NOW())
        """
        params = {
            "time": data.time,
            "symbol": data.symbol,
            "open": _r4(data.open),
            "high": _r4(data.high),
            "low": _r4(data.low),
            "close": _r4(data.close),
            "volume": _r6(data.volume) if is_crypto else _r4(data.volume),
            "source": data.source,
            "interval": data.interval,
            "asset_class": asset_class,
        }
        await self._execute_insert(query, params)

    async def write_orderbook(self, symbol: str, data: OrderbookData) -> None:
        """Insert to market_data_l2"""
        query = """
            INSERT INTO market_data_l2 (time, symbol, bids, asks, spread, mid_price)
            VALUES (:time, :symbol, :bids, :asks, :spread, :mid_price)
        """
        params = data.model_dump()
        params["symbol"] = symbol
        params["bids"] = json.dumps(params["bids"])
        params["asks"] = json.dumps(params["asks"])
        params["spread"] = _r6(params["spread"])
        params["mid_price"] = _r6(params["mid_price"])
        await self._execute_insert(query, params)

    async def write_binance_trade(self, data: BinanceTradeData) -> None:
        """Insert Binance trade data to order_flow table"""
        query = """
            INSERT INTO order_flow (time, symbol, price, size, side, aggressor)
            VALUES (:time, :symbol, :price, :size, :side, :aggressor)
        """
        # Determine side: if buyer was maker, then buyer is passive (sell side pressure)
        # Otherwise buyer is aggressive (buy side pressure)
        side = "sell" if data.buyer_maker else "buy"

        params = {
            "time": data.time,
            "symbol": data.symbol,
            "price": _r6(data.price),
            "size": _r6(data.quantity),
            "side": side,
            "aggressor": str(data.trade_id),  # Use trade ID as aggressor identifier
        }
        await self._execute_insert(query, params)

    async def write_binance_depth(self, data: BinanceDepthData) -> None:
        """Insert Binance depth data to market_data_l2 as orderbook snapshot"""
        query = """
            INSERT INTO market_data_l2 (time, symbol, bids, asks, spread, mid_price)
            VALUES (:time, :symbol, :bids, :asks, :spread, :mid_price)
        """

        # Calculate spread and mid price from top level
        bids_list = sorted(
            [(float(price), float(qty)) for price, qty in data.bids.items()],
            key=lambda x: x[0],
            reverse=True,
        )
        asks_list = sorted(
            [(float(price), float(qty)) for price, qty in data.asks.items()],
            key=lambda x: x[0],
        )

        best_bid = bids_list[0][0] if bids_list else 0
        best_ask = asks_list[0][0] if asks_list else 0
        spread = _r6(best_ask - best_bid) if best_bid > 0 and best_ask > 0 else 0
        mid_price = (
            _r6((best_bid + best_ask) / 2) if best_bid > 0 and best_ask > 0 else 0
        )

        params = {
            "time": data.time,
            "symbol": data.symbol,
            "bids": json.dumps(data.bids),
            "asks": json.dumps(data.asks),
            "spread": spread,
            "mid_price": mid_price,
        }
        await self._execute_insert(query, params)

    async def write_features(
        self, symbol: str, features_dict: Dict[str, float]
    ) -> None:
        """Insert to features"""
        query = """
            INSERT INTO features (time, symbol, feature_name, value)
            VALUES (:time, :symbol, :feature_name, :value)
        """
        time_now = datetime.now(timezone.utc)
        ratio_features = {
            "returns",
            "log_returns",
            "price_vs_vwap_pct",
            "ema_spread_pct",
            "trend_strength",
        }
        async with self.engine.begin() as conn:
            for feature_name, value in features_dict.items():
                rnd = _r8 if feature_name in ratio_features else _r6
                params = {
                    "time": time_now,
                    "symbol": symbol,
                    "feature_name": feature_name,
                    "value": rnd(value),
                }
                await conn.execute(text(query), params)

    async def get_bars(
        self, symbol: str, start: datetime, end: datetime, interval: str
    ) -> pd.DataFrame:
        """Returns DataFrame of market_data_l1"""
        query = """
            SELECT time, symbol, open, high, low, close, volume, source, interval
            FROM market_data_l1
            WHERE symbol = :symbol AND interval = :interval AND time >= :start AND time <= :end
            ORDER BY time ASC
        """
        params = {"symbol": symbol, "start": start, "end": end, "interval": interval}
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), params)
            rows = result.fetchall()
            if not rows:
                return pd.DataFrame()

            # Convert to DataFrame
            df = pd.DataFrame(
                rows,
                columns=[
                    "time",
                    "symbol",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "source",
                    "interval",
                ],
            )
            return df

    async def fetch_recent_bars(self, symbol: str, limit: int = 5000) -> pd.DataFrame:
        query = """
            SELECT time, open, high, low, close, volume
            FROM market_data_l1
            WHERE symbol = :symbol
            ORDER BY time ASC
            LIMIT :limit
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), {"symbol": symbol, "limit": limit})
            rows = result.fetchall()
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(
                rows, columns=["time", "open", "high", "low", "close", "volume"]
            )
            numeric_cols = ["open", "high", "low", "close", "volume"]
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df["time"] = pd.to_datetime(df["time"], utc=True)
            df = df.dropna()
            return df

    async def write_agent(self, agent_data: AgentData) -> None:
        """Upserts to agent_registry"""
        query = """
            INSERT INTO agent_registry (id, name, type, layer, status, pid, last_heartbeat, created_at, metadata)
            VALUES (:id, :name, :type, :layer, :status, :pid, :last_heartbeat, :created_at, :metadata)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                type = EXCLUDED.type,
                layer = EXCLUDED.layer,
                status = EXCLUDED.status,
                pid = EXCLUDED.pid,
                last_heartbeat = EXCLUDED.last_heartbeat,
                metadata = EXCLUDED.metadata
        """
        params = agent_data.model_dump()
        params["id"] = str(params["id"])  # ensure uuid string
        params["metadata"] = (
            json.dumps(params["metadata"]) if params["metadata"] else None
        )
        await self._execute_insert(query, params)

    async def log(
        self,
        agent_id: str,
        level: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Inserts to system_logs"""
        query = """
            INSERT INTO system_logs (time, agent_id, level, message, metadata)
            VALUES (:time, :agent_id, :level, :message, :metadata)
        """
        params = {
            "time": datetime.now(timezone.utc),
            "agent_id": str(agent_id),
            "level": level,
            "message": message,
            "metadata": json.dumps(metadata) if metadata else None,
        }
        await self._execute_insert(query, params)

    async def get_latest_features(self, symbols: list[str], limit: int = 5) -> dict:
        """Returns: {symbol: {feature_name: value}} for latest features per symbol"""
        query = """
            SELECT symbol, feature_name, value 
            FROM (
                SELECT symbol, feature_name, value,
                       ROW_NUMBER() OVER(PARTITION BY symbol, feature_name ORDER BY time DESC) as rn
                FROM features
                WHERE symbol = ANY(:symbols)
            ) sub
            WHERE rn = 1
        """
        result_dict = {sym: {} for sym in symbols}
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), {"symbols": symbols})
            rows = result.fetchall()
            for row in rows:
                result_dict[row[0]][row[1]] = float(row[2])
        return result_dict

    async def get_recent_backtest_results(self, limit: int = 5) -> list[dict]:
        """Returns: last N backtest results ordered by created_at DESC"""
        query = """
            SELECT b.strategy_id, b.start_date, b.end_date, b.sharpe, b.cagr, 
                   b.max_drawdown, b.win_rate, b.total_trades, b.passed_validation, b.results,
                   s.created_at
            FROM backtest_results b
            JOIN strategies s ON b.strategy_id = s.id
            ORDER BY s.created_at DESC
            LIMIT :limit
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), {"limit": limit})
            rows = result.fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "strategy_id": str(r[0]),
                        "start_date": r[1],
                        "end_date": r[2],
                        "sharpe": float(r[3]) if r[3] is not None else None,
                        "cagr": float(r[4]) if r[4] is not None else None,
                        "max_drawdown": float(r[5]) if r[5] is not None else None,
                        "win_rate": float(r[6]) if r[6] is not None else None,
                        "total_trades": int(r[7]) if r[7] is not None else None,
                        "passed_validation": bool(r[8]),
                        "results": r[9],
                        "created_at": r[10],
                    }
                )
            return out

    async def get_top_patterns(
        self,
        min_confidence: float = 0.0,
        limit: int = 10,
    ) -> dict[str, list[dict]]:
        """Returns pattern intelligence grouped by type.
        Returns: {winning: [...], losing: [...], cost_traps: [...], neutral: [...]}
        """
        query = """
            SELECT pattern_type, archetype, feature_family, asset_class,
                   composite_score_avg, short_window_score_avg, sharpe_avg,
                   win_rate_avg, total_trades_avg, cost_burden_avg,
                   sample_size, confidence_score, recommendation, motif_details
            FROM pattern_memory
            WHERE confidence_score >= :min_conf
            ORDER BY composite_score_avg DESC
            LIMIT :lim
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(
                text(query), {"min_conf": min_confidence, "lim": limit}
            )
            rows = result.fetchall()

        grouped: dict[str, list[dict]] = {
            "winning": [],
            "losing": [],
            "cost_traps": [],
            "neutral": [],
        }
        for r in rows:
            motif = {
                "pattern_type": r[0],
                "archetype": r[1],
                "feature_family": r[2]
                if isinstance(r[2], list)
                else (list(r[2]) if r[2] else []),
                "asset_class": r[3],
                "composite_score_avg": float(r[4]) if r[4] is not None else 0,
                "short_window_score_avg": float(r[5]) if r[5] is not None else 0,
                "sharpe_avg": float(r[6]) if r[6] is not None else 0,
                "win_rate_avg": float(r[7]) if r[7] is not None else 0,
                "total_trades_avg": float(r[8]) if r[8] is not None else 0,
                "cost_burden_avg": float(r[9]) if r[9] is not None else 0,
                "sample_size": int(r[10]) if r[10] is not None else 0,
                "confidence_score": float(r[11]) if r[11] is not None else 0,
                "recommendation": r[12] or "",
                "motif_details": r[13] if isinstance(r[13], dict) else {},
            }
            ptype = r[0]
            if ptype == "winning_motif":
                grouped["winning"].append(motif)
            elif ptype == "losing_motif":
                grouped["losing"].append(motif)
            elif ptype == "cost_trap":
                grouped["cost_traps"].append(motif)
            else:
                grouped["neutral"].append(motif)

        return grouped

    async def get_recent_strategy_names(self, limit: int = 10) -> list[str]:
        """Returns: last N strategy names to avoid duplicates"""
        query = """
            SELECT name FROM strategies
            ORDER BY created_at DESC
            LIMIT :limit
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), {"limit": limit})
            return [row[0] for row in result.fetchall()]

    async def get_strategy_signatures(self, limit: int = 100) -> set[str]:
        """Returns: set of recent strategy signatures for dedup."""
        query = """
            SELECT strategy_signature FROM strategies
            WHERE strategy_signature IS NOT NULL
            ORDER BY created_at DESC
            LIMIT :limit
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), {"limit": limit})
            return {row[0] for row in result.fetchall() if row[0]}

    async def get_recent_feature_combos(
        self, limit: int = 50
    ) -> list[tuple[set[str], str]]:
        """Returns: list of (feature_set, archetype) from recent strategies."""
        query = """
            SELECT normalized_strategy FROM strategies
            WHERE normalized_strategy IS NOT NULL
            ORDER BY created_at DESC
            LIMIT :limit
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), {"limit": limit})
            combos = []
            for row in result.fetchall():
                raw = row[0]
                if isinstance(raw, str):
                    try:
                        raw = json.loads(raw)
                    except Exception:
                        continue
                if not isinstance(raw, dict):
                    continue
                features = set()
                for key in ("entry_conditions", "exit_conditions"):
                    for cond in raw.get(key) or []:
                        if isinstance(cond, str):
                            for feat in re.findall(r"\b[a-z_][a-z_0-9]+\b", cond):
                                features.add(feat)
                archetype = (raw.get("tags") or ["unknown"])[0]
                if features:
                    combos.append((features, archetype))
            return combos

    async def save_strategy(
        self,
        spec: Any,
        status: str,
        author_agent: str,
        prompt: Optional[str] = None,
        raw_response: Optional[str] = None,
        strategy_signature: Optional[str] = None,
        generation_batch: Optional[str] = None,
    ) -> str:
        """Inserts to strategies table, returns strategy_id (uuid)."""
        import uuid

        # Defensive normalization
        if isinstance(spec, str):
            try:
                spec = json.loads(spec)
            except json.JSONDecodeError:
                raise ValueError(
                    f"save_strategy received non-JSON string: {spec[:200]}"
                )
        if isinstance(spec, tuple):
            spec = spec[0] if spec else {}
        if not isinstance(spec, dict):
            raise TypeError(
                f"save_strategy expected dict, got {type(spec).__name__}: {spec!r}"
            )

        strategy_name = spec.get("strategy_name") or spec.get("name")
        if not strategy_name:
            raise KeyError(
                f"Missing strategy_name in spec. Keys: {list(spec.keys())} | spec: {spec}"
            )

        strategy_id = str(uuid.uuid4())
        trace_id = uuid.uuid4().hex[:16]
        query = """
            INSERT INTO strategies (id, name, code, parameters, status, created_at, author_agent, prompt, raw_response, normalized_strategy, strategy_signature, trace_id, generation_batch)
            VALUES (:id, :name, :code, :parameters, :status, :created_at, :author_agent, :prompt, :raw_response, :normalized_strategy, :strategy_signature, :trace_id, :generation_batch)
        """
        params = {
            "id": strategy_id,
            "name": strategy_name,
            "code": "",
            "parameters": json.dumps(spec),
            "status": status,
            "created_at": datetime.now(timezone.utc),
            "author_agent": author_agent,
            "prompt": prompt,
            "raw_response": raw_response,
            "normalized_strategy": json.dumps(spec),
            "strategy_signature": strategy_signature,
            "trace_id": trace_id,
            "generation_batch": generation_batch,
        }
        await self._execute_insert(query, params)

        # Log first lifecycle event
        try:
            from atlas.core.event_lineage import EventLineageClient

            lineage = EventLineageClient(self)
            await lineage.create_event(
                trace_id=trace_id,
                stage="ideator",
                status="completed",
                actor=author_agent,
                strategy_id=strategy_id,
                metadata={
                    "strategy_name": strategy_name,
                    "status": status,
                    "mode": spec.get("mode", "unknown"),
                },
            )
        except Exception as exc:
            logger.warning(f"Failed to log lifecycle event: {exc}")

        return strategy_id

    async def update_strategy_status(
        self, strategy_id: str, status: str, message: Optional[str] = None
    ) -> None:
        """Updates status in strategies table. Optional message is logged."""
        query = """
            UPDATE strategies
            SET status = :status
            WHERE id = :id
        """
        params = {"id": strategy_id, "status": status}
        await self._execute_insert(query, params)
        if message:
            await self.log(strategy_id, "INFO", message)

    async def update_strategy_code(
        self, strategy_id: str, code: str, status: str
    ) -> None:
        """Updates code + status in strategies table"""
        query = """
            UPDATE strategies 
            SET code = :code, status = :status
            WHERE id = :id
        """
        params = {"id": strategy_id, "code": code, "status": status}
        await self._execute_insert(query, params)

    async def get_backtest_result(self, strategy_id: str) -> dict | None:
        """Fetch latest backtest result for a strategy"""
        async with self.engine.connect() as conn:
            result = await conn.execute(
                text("""
                    SELECT *, results::jsonb as results_json
                    FROM backtest_results
                    WHERE strategy_id = :sid
                """),
                {"sid": str(strategy_id)},
            )
            row = result.fetchone()
            if not row:
                return None
            d = dict(row._mapping)
            # Merge results JSONB into top level for easy access
            if d.get("results_json"):
                try:
                    extra = (
                        json.loads(d["results_json"])
                        if isinstance(d["results_json"], str)
                        else d["results_json"]
                    )
                    d.update(extra)
                except:
                    pass
            return d

    async def update_strategy_status(
        self, strategy_id: str, status: str, notes: str = ""
    ) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                text("""
                    UPDATE strategies
                    SET status = :status,
                        parameters = jsonb_set(
                            COALESCE(parameters::jsonb, '{}'::jsonb),
                            '{validation_notes}',
                            cast(:notes_json as jsonb)
                        )
                    WHERE id = :sid
                """),
                {
                    "status": status,
                    "notes_json": json.dumps(notes),
                    "sid": str(strategy_id),
                },
            )

    async def get_strategies_by_status(self, status: str) -> list[dict]:
        """Returns all strategies matching status"""
        query = """
            SELECT id, name, code, parameters, status, created_at, author_agent, trace_id
            FROM strategies
            WHERE status = :status
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), {"status": status})
            rows = result.fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "id": str(r[0]),
                        "name": r[1],
                        "code": r[2],
                        "parameters": r[3]
                        if isinstance(r[3], dict)
                        else json.loads(r[3]),
                        "status": r[4],
                        "created_at": r[5],
                        "author_agent": r[6],
                        "trace_id": str(r[7]) if r[7] else None,
                    }
                )
            return out

    async def get_strategies_with_backtest(
        self, statuses: list[str] | None = None, limit: int = 10
    ) -> list[dict]:
        """Returns strategies with backtest results joined, ordered by short_window_score.
        Only queries verified top-level columns; profit_factor/composite_score/gross_edge/cost_burden
        are extracted from the results JSONB.
        Used by the ideator to learn from past failures and successes.
        Defaults to failed_validation + repair_candidate if no statuses given.
        """
        if statuses is None:
            statuses = ["failed_validation", "repair_candidate"]
        query = """
            SELECT s.id, s.name, s.code, s.parameters, s.status, s.created_at,
                   s.author_agent, b.total_trades,
                   b.max_drawdown, b.win_rate, b.short_window_score,
                   b.results
            FROM strategies s
            JOIN backtest_results b ON s.id = b.strategy_id
            WHERE s.status = ANY(:statuses)
            ORDER BY b.short_window_score DESC NULLS LAST
            LIMIT :limit
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(
                text(query), {"statuses": statuses, "limit": limit}
            )
            rows = result.fetchall()
            out = []
            for r in rows:
                params = r[3] if isinstance(r[3], dict) else json.loads(r[3])
                results_raw = r[11] if r[11] else {}
                results = (
                    json.loads(results_raw)
                    if isinstance(results_raw, str)
                    else (results_raw if isinstance(results_raw, dict) else {})
                )
                out.append(
                    {
                        "id": str(r[0]),
                        "name": r[1],
                        "code": r[2],
                        "parameters": params,
                        "status": r[4],
                        "created_at": r[5],
                        "author_agent": r[6],
                        "total_trades": r[7] or 0,
                        "max_drawdown": r[8] or 0,
                        "win_rate": r[9] or 0,
                        "short_window_score": r[10] or 0.0,
                        "profit_factor": float(results.get("profit_factor", 1.0)),
                        "composite_score": float(results.get("composite_score", 0.0)),
                        "gross_edge": float(results.get("gross_edge", 0.0)),
                        "cost_burden": float(results.get("cost_burden", 0.0)),
                    }
                )
            return out

    async def get_latest_scout_intelligence(self) -> dict:
        """
        Fetch latest scout intelligence payloads for Ideator/Mutator/Validator consumption.
        Returns: {regime: {...}, liquidity: {...}, correlation: {...}, execution: {...}}
        """
        results = {}
        async with self.engine.connect() as conn:
            try:
                result = await conn.execute(text("""
                    SELECT volatility_regime, trend_regime, liquidity_regime, correlation_regime,
                           realized_volatility, relative_volume, confidence_score
                    FROM market_regime_memory
                    ORDER BY timestamp DESC
                    LIMIT 1
                """))
                row = result.fetchone()
                if row:
                    results["regime"] = {
                        "volatility": row[0],
                        "trend": row[1],
                        "liquidity": row[2],
                        "correlation": row[3],
                        "realized_vol": float(row[4]) if row[4] else 0,
                        "relative_volume": float(row[5]) if row[5] else 0,
                        "confidence": float(row[6]) if row[6] else 0,
                    }
            except Exception:
                pass

            try:
                result = await conn.execute(text("""
                    SELECT liquidity_regime, liquidity_score, slippage_risk, avg_spread_bps
                    FROM liquidity_intelligence
                    ORDER BY timestamp DESC
                    LIMIT 1
                """))
                row = result.fetchone()
                if row:
                    results["liquidity"] = {
                        "regime": row[0],
                        "score": float(row[1]) if row[1] else 0,
                        "risk": float(row[2]) if row[2] else 0,
                        "spread_bps": float(row[3]) if row[3] else 0,
                    }
            except Exception:
                pass

            try:
                result = await conn.execute(text("""
                    SELECT cluster_name, avg_pairwise_corr, risk_state, correlation_spike_detected
                    FROM correlation_memory
                    ORDER BY timestamp DESC
                    LIMIT 1
                """))
                row = result.fetchone()
                if row:
                    results["correlation"] = {
                        "cluster": row[0],
                        "avg_corr": float(row[1]) if row[1] else 0,
                        "risk_state": row[2],
                        "spike": bool(row[3]),
                    }
            except Exception:
                pass

            try:
                result = await conn.execute(text("""
                    SELECT execution_regime, fill_quality_score, avg_slippage_bps, rejection_rate
                    FROM execution_intelligence
                    ORDER BY timestamp DESC
                    LIMIT 1
                """))
                row = result.fetchone()
                if row:
                    results["execution"] = {
                        "regime": row[0],
                        "fill_score": float(row[1]) if row[1] else 0,
                        "slippage_bps": float(row[2]) if row[2] else 0,
                        "rejection_rate": float(row[3]) if row[3] else 0,
                    }
            except Exception:
                pass

        return results

    async def get_scout_signal_summary(self) -> dict:
        """
        Fetch compressed summary of recent scout_signals grouped by source and signal_type.
        Phase 25D: Used by IdeatorAgentV2 to inject structured scout data into ideation prompts.
        Returns: {by_source: {...}, recent: [...], total_signals: int}
        """
        async with self.engine.connect() as conn:
            try:
                result = await conn.execute(text("""
                    SELECT source, signal_type, COUNT(*) as cnt,
                           ROUND(AVG(confidence_score)::numeric, 2) as avg_conf
                    FROM scout_signals
                    GROUP BY source, signal_type
                    ORDER BY cnt DESC
                """))
                by_source = {}
                for row in result.fetchall():
                    key = (row[0], row[1])  # (source, signal_type)
                    by_source[key] = {
                        "count": row[2],
                        "avg_confidence": float(row[3]) if row[3] else 0.0,
                    }
            except Exception:
                by_source = {}

            try:
                result = await conn.execute(text("""
                    SELECT source, signal_type, symbol, confidence_score, created_at
                    FROM scout_signals
                    ORDER BY created_at DESC
                    LIMIT 5
                """))
                recent = []
                for row in result.fetchall():
                    recent.append({
                        "source": row[0],
                        "type": row[1],
                        "symbol": row[2],
                        "confidence": float(row[3]) if row[3] else 0.0,
                        "created_at": str(row[4]) if row[4] else None,
                    })
            except Exception:
                recent = []

        total = sum(info["count"] for info in by_source.values())
        return {"by_source": by_source, "recent": recent, "total_signals": total}

    async def get_validation_intelligence(self) -> dict:
        """
        Fetch latest Phase 11 validation intelligence for Ideator/Mutator consumption.
        Returns: {walk_forward: {...}, monte_carlo: {...}, overfitting: {...},
                  regime: {...}, cost_stress: {...}}
        """
        results = {}
        # Each table has its own timestamp column for ORDER BY
        queries = [
            ("walk_forward_analysis", "walk_forward",
             ["walk_forward_score", "temporal_consistency", "regime_survival_score"],
             "analyzed_at"),
            ("monte_carlo_analysis", "monte_carlo",
             ["monte_carlo_survival_score", "expected_tail_drawdown", "probabilistic_sharpe"],
             "simulated_at"),
            ("overfitting_analysis", "overfitting",
             ["overfit_probability", "robustness_score", "parameter_stability_score"],
             "analyzed_at"),
            ("regime_validation", "regime",
             ["regime_dependency_score", "regime_survival_score", "over_specialized"],
             "validated_at"),
            ("cost_stress_analysis", "cost_stress",
             ["cost_survival_score", "passes_min_survival", "fragile_scalper_detected"],
             "tested_at"),
        ]
        async with self.engine.connect() as conn:
            for tbl, key, cols, order_by in queries:
                try:
                    col_list = ", ".join(cols)
                    q = text(f"SELECT {col_list} FROM {tbl} ORDER BY {order_by} DESC NULLS LAST LIMIT 1")
                    result = await conn.execute(q)
                    row = result.fetchone()
                    if row:
                        results[key] = {}
                        for i, c in enumerate(cols):
                            val = row[i]
                            if isinstance(val, bool):
                                results[key][c] = bool(val)
                            else:
                                results[key][c] = float(val) if val is not None else 0.0
                except Exception:
                    pass
        return results

    async def get_top_strategies_by_sharpe(
        self, min_sharpe: float, max_sharpe: float, limit: int
    ) -> list[dict]:
        """Returns validated strategies filtered by sharpe range"""
        query = """
            SELECT s.id, s.name, s.code, s.parameters, s.status, s.created_at, s.author_agent, b.sharpe
            FROM strategies s
            JOIN backtest_results b ON s.id = b.strategy_id
            WHERE s.status = 'validated' 
              AND b.sharpe >= :min_sharpe 
              AND b.sharpe <= :max_sharpe
            ORDER BY b.sharpe DESC
            LIMIT :limit
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(
                text(query),
                {"min_sharpe": min_sharpe, "max_sharpe": max_sharpe, "limit": limit},
            )
            rows = result.fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "id": str(r[0]),
                        "name": r[1],
                        "code": r[2],
                        "parameters": r[3]
                        if isinstance(r[3], dict)
                        else json.loads(r[3]),
                        "status": r[4],
                        "created_at": r[5],
                        "sharpe": float(r[7]),
                    }
                )
            return out

    async def get_top_strategies_by_composite_score(
        self, min_score: float, max_score: float, limit: int
    ) -> list[dict]:
        """Returns strategies sorted by short_window_score (temporal fitness).
        Includes pending_validation and benchmark strategies that have been backtested.
        """
        query = """
            SELECT s.id, s.name, s.code, s.parameters, s.status, s.created_at,
                   s.author_agent, b.short_window_score
            FROM strategies s
            JOIN backtest_results b ON s.id = b.strategy_id
            WHERE b.short_window_score IS NOT NULL
              AND b.short_window_score >= :min_score
              AND b.short_window_score <= :max_score
              AND s.status IN ('pending_validation', 'benchmark')
            ORDER BY b.short_window_score DESC
            LIMIT :limit
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(
                text(query),
                {"min_score": min_score, "max_score": max_score, "limit": limit},
            )
            rows = result.fetchall()
            out = []
            for r in rows:
                out.append(
                    {
                        "id": str(r[0]),
                        "name": r[1],
                        "code": r[2],
                        "parameters": r[3]
                        if isinstance(r[3], dict)
                        else json.loads(r[3]),
                        "status": r[4],
                        "created_at": r[5],
                        "short_window_score": float(r[7]) if r[7] is not None else 0.0,
                    }
                )
            return out

    async def check_combination_exists(self, parent_a: str, parent_b: str) -> bool:
        """Check if a combination of these two parents already exists (unordered)."""
        query = """
            SELECT 1 FROM combination_memory
            WHERE (parent_a = :a AND parent_b = :b)
               OR (parent_a = :b AND parent_b = :a)
            LIMIT 1
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), {"a": parent_a, "b": parent_b})
            return result.fetchone() is not None

    async def save_combination_record(
        self,
        parent_a: str,
        parent_b: str,
        child_id: str,
        combination_type: str = "claude_hybrid",
        parent_a_score: float = 0.0,
        parent_b_score: float = 0.0,
        child_score: float = 0.0,
    ) -> None:
        """Record a combination in combination_memory with score delta."""
        # Normalize ordering to avoid (a,b) vs (b,a) duplicates
        if parent_a > parent_b:
            parent_a, parent_b = parent_b, parent_a
            parent_a_score, parent_b_score = parent_b_score, parent_a_score
        query = """
            INSERT INTO combination_memory
                (parent_a, parent_b, child_id, combination_type,
                 parent_a_sharpe, parent_b_sharpe, child_sharpe, sharpe_delta)
            VALUES
                (:parent_a, :parent_b, :child_id, :combination_type,
                 :parent_a_score, :parent_b_score, :child_score, :sharpe_delta)
            ON CONFLICT (parent_a, parent_b, combination_type) DO NOTHING
        """
        params = {
            "parent_a": parent_a,
            "parent_b": parent_b,
            "child_id": child_id,
            "combination_type": combination_type,
            "parent_a_score": parent_a_score,
            "parent_b_score": parent_b_score,
            "child_score": child_score,
            "sharpe_delta": child_score - max(parent_a_score, parent_b_score),
        }
        await self._execute_insert(query, params)

    async def save_backtest_results(
        self, strategy_id: str, results: dict, start_date: str, end_date: str
    ) -> None:
        """Saves backtest results including short_window temporal scores"""
        query = """
            INSERT INTO backtest_results (strategy_id, start_date, end_date, sharpe, cagr, max_drawdown, win_rate, total_trades, passed_validation, results, entry_count, exit_count, bars_processed, short_window_score, score_7d, score_14d, score_30d)
            VALUES (:strategy_id, :start_date, :end_date, :sharpe, :cagr, :max_drawdown, :win_rate, :total_trades, :passed_validation, :results, :entry_count, :exit_count, :bars_processed, :short_window_score, :score_7d, :score_14d, :score_30d)
            ON CONFLICT (strategy_id, start_date, end_date) DO UPDATE SET
                sharpe = EXCLUDED.sharpe,
                cagr = EXCLUDED.cagr,
                max_drawdown = EXCLUDED.max_drawdown,
                win_rate = EXCLUDED.win_rate,
                total_trades = EXCLUDED.total_trades,
                passed_validation = EXCLUDED.passed_validation,
                results = EXCLUDED.results,
                entry_count = EXCLUDED.entry_count,
                exit_count = EXCLUDED.exit_count,
                bars_processed = EXCLUDED.bars_processed,
                short_window_score = EXCLUDED.short_window_score,
                score_7d = EXCLUDED.score_7d,
                score_14d = EXCLUDED.score_14d,
                score_30d = EXCLUDED.score_30d
        """
        params = {
            "strategy_id": strategy_id,
            "start_date": datetime.fromisoformat(start_date)
            if isinstance(start_date, str)
            else start_date,
            "end_date": datetime.fromisoformat(end_date)
            if isinstance(end_date, str)
            else end_date,
            "sharpe": results.get("holdout_sharpe", results.get("sharpe_ratio", 0.0)),
            "cagr": results.get("cagr", 0.0),
            "max_drawdown": results.get("max_drawdown", 0.0),
            "win_rate": results.get("win_rate", 0.0),
            "total_trades": results.get("total_trades", 0),
            "passed_validation": results.get("passed_validation", False),
            "results": json.dumps(results),
            "entry_count": results.get("entry_count", 0),
            "exit_count": results.get("exit_count", 0),
            "bars_processed": results.get("bars_processed", 0),
            "short_window_score": results.get("short_window_score"),
            "score_7d": results.get("score_7d"),
            "score_14d": results.get("score_14d"),
            "score_30d": results.get("score_30d"),
        }
        await self._execute_insert(query, params)

    async def get_open_paper_trades(self) -> list[dict]:
        query = "SELECT * FROM paper_trades WHERE status = 'open'"
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query))
            rows = result.fetchall()
            cols = result.keys()
            return [dict(zip(cols, row)) for row in rows]

    async def get_daily_pnl(self) -> float:
        query = (
            "SELECT SUM(pnl) FROM paper_trades WHERE time >= NOW() - INTERVAL '1 day'"
        )
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query))
            val = result.scalar()
            return float(val) if val else 0.0

    async def get_weekly_pnl(self) -> float:
        query = (
            "SELECT SUM(pnl) FROM paper_trades WHERE time >= NOW() - INTERVAL '7 days'"
        )
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query))
            val = result.scalar()
            return float(val) if val else 0.0

    async def get_agent_metadata(self, name: str) -> dict:
        query = "SELECT metadata FROM agent_registry WHERE name = :name"
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), {"name": name})
            val = result.scalar()
            return val if val else {}

    async def update_agent_metadata_active(self, name: str, active: bool) -> None:
        # Simplistic approach to handle jsonb update since we don't have jsonb_set working cleanly in all mock cases.
        # We will fetch, update, and save.
        meta = await self.get_agent_metadata(name)
        if not meta:
            meta = {}
        meta["active"] = active
        query = "UPDATE agent_registry SET metadata = :meta WHERE name = :name"
        params = {"meta": json.dumps(meta), "name": name}
        await self._execute_insert(query, params)

    async def save_paper_trade(self, trade: dict) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                text(
                    """
                INSERT INTO paper_trades
                (time, strategy_id, symbol, side, quantity, price, fill_price, status, pnl)
                VALUES (NOW(), :strategy_id, :symbol, :side, :quantity,
                        :price, :fill_price, :status, :pnl)
                ON CONFLICT DO NOTHING
            """
                ),
                trade,
            )

    async def save_backtest_trade(self, strategy_id: str, trade: dict) -> None:
        """Insert a single backtest trade into backtest_trades"""
        query = """
            INSERT INTO backtest_trades (strategy_id, symbol, entry_time, exit_time, entry_price, exit_price, side, pnl, pnl_pct, bars_held, exit_reason)
            VALUES (:strategy_id, :symbol, :entry_time, :exit_time, :entry_price, :exit_price, :side, :pnl, :pnl_pct, :bars_held, :exit_reason)
        """
        params = {
            "strategy_id": strategy_id,
            "symbol": trade.get("symbol", ""),
            "entry_time": trade.get("entry_time"),
            "exit_time": trade.get("exit_time"),
            "entry_price": trade.get("entry_price"),
            "exit_price": trade.get("exit_price"),
            "side": trade.get("side"),
            "pnl": trade.get("pnl"),
            "pnl_pct": trade.get("pnl_pct"),
            "bars_held": trade.get("bars_held", 0),
            "exit_reason": trade.get("exit_reason", "signal"),
        }
        await self._execute_insert(query, params)

    async def update_strategy_fields(self, strategy_id: str, **kwargs) -> None:
        """Generic update for arbitrary strategy columns. Usage:
        await db.update_strategy_fields(sid, compile_error="...", normalized_strategy={...})
        """
        if not kwargs:
            return
        sets = ", ".join(f"{k} = :{k}" for k in kwargs)
        params = {"id": strategy_id, **kwargs}
        query = f"UPDATE strategies SET {sets} WHERE id = :id"
        await self._execute_insert(query, params)

    async def get_paper_trades_for_strategy(self, strategy_id: str) -> list:
        async with self.engine.connect() as conn:
            result = await conn.execute(
                text("SELECT * FROM paper_trades WHERE strategy_id=:sid"),
                {"sid": str(strategy_id)},
            )
            return [dict(r._mapping) for r in result.fetchall()]

    async def get_repair_candidates(self, limit: int = 10) -> list[dict]:
        """
        Fetch strategies suitable for mutation repair.
        Targets: repair_candidate and research_candidate only.
        Excludes failed_validation — directed evolution, not random garbage mutation.
        """
        query = """
            SELECT s.id, s.name, s.code, s.parameters, s.normalized_strategy,
                   s.status, s.created_at, s.author_agent,
                   b.sharpe, b.entry_count, b.total_trades, b.max_drawdown,
                   b.win_rate, b.results
            FROM strategies s
            JOIN backtest_results b ON s.id = b.strategy_id
            WHERE s.status IN ('repair_candidate', 'research_candidate')
            ORDER BY
                CASE s.status
                    WHEN 'research_candidate' THEN 1
                    WHEN 'repair_candidate' THEN 2
                END,
                b.sharpe DESC
            LIMIT :limit
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(text(query), {"limit": limit})
            rows = result.fetchall()
            import decimal

            out = []
            for r in rows:
                d = dict(r._mapping)
                # Convert Decimal values to native float/int for JSON safety
                for k, v in d.items():
                    if isinstance(v, decimal.Decimal):
                        d[k] = float(v) if "." in str(v) else int(v)
                if isinstance(d.get("parameters"), str):
                    d["parameters"] = json.loads(d["parameters"])
                if isinstance(d.get("normalized_strategy"), str):
                    d["normalized_strategy"] = json.loads(d["normalized_strategy"])
                # Merge results JSONB for diagnostics
                results_raw = d.get("results")
                if isinstance(results_raw, dict):
                    d.update(results_raw)
                out.append(d)
            return out

    async def save_mutation_record(
        self,
        parent_id: str,
        child_id: str,
        mutation_type: str,
        changed_fields: list[str],
        parent_metrics: dict,
        child_metrics: dict,
    ) -> None:
        """Record a parent-child mutation lineage."""
        query = """
            INSERT INTO mutation_memory
                (parent_strategy_id, child_strategy_id, mutation_type, changed_fields,
                 parent_sharpe, child_sharpe, sharpe_delta,
                 parent_entry_count, child_entry_count,
                 parent_trades, child_trades,
                 parent_composite_score, child_composite_score,
                 score_delta, improved)
            VALUES
                (:parent_id, :child_id, :mutation_type, :changed_fields,
                 :parent_sharpe, :child_sharpe, :sharpe_delta,
                 :parent_entry_count, :child_entry_count,
                 :parent_trades, :child_trades,
                 :parent_composite_score, :child_composite_score,
                 :score_delta, :improved)
        """
        p_sharpe = float(
            parent_metrics.get("sharpe", parent_metrics.get("holdout_sharpe", 0))
        )
        c_sharpe = float(
            child_metrics.get("sharpe", child_metrics.get("holdout_sharpe", 0))
        )
        import json
        if isinstance(changed_fields, (dict, list)):
            changed_fields = json.dumps(changed_fields)

        p_composite = parent_metrics.get("composite_score") or parent_metrics.get("composite_score_avg")
        c_composite = child_metrics.get("composite_score") or child_metrics.get("composite_score_avg")
        p_composite = float(p_composite) if p_composite is not None else None
        c_composite = float(c_composite) if c_composite is not None else None
        score_delta = (c_composite - p_composite) if (p_composite is not None and c_composite is not None) else None
        improved = (score_delta > 0) if score_delta is not None else None

        params = {
            "parent_id": parent_id,
            "child_id": child_id,
            "mutation_type": mutation_type,
            "changed_fields": changed_fields,
            "parent_sharpe": p_sharpe,
            "child_sharpe": c_sharpe,
            "sharpe_delta": c_sharpe - p_sharpe,
            "parent_entry_count": int(parent_metrics.get("entry_count", 0)),
            "child_entry_count": int(child_metrics.get("entry_count", 0)),
            "parent_trades": int(parent_metrics.get("total_trades", 0)),
            "child_trades": int(child_metrics.get("total_trades", 0)),
            "parent_composite_score": p_composite,
            "child_composite_score": c_composite,
            "score_delta": score_delta,
            "improved": improved,
        }
        await self._execute_insert(query, params)

    async def get_mutation_history(
        self, strategy_id: str, max_depth: int = 3
    ) -> list[dict]:
        """Fetch mutation lineage for a strategy (both as parent and child)."""
        query = """
            SELECT * FROM mutation_memory
            WHERE parent_strategy_id = :sid OR child_strategy_id = :sid
            ORDER BY created_at DESC
            LIMIT :limit
        """
        async with self.engine.connect() as conn:
            result = await conn.execute(
                text(query), {"sid": strategy_id, "limit": max_depth * 2}
            )
            return [dict(r._mapping) for r in result.fetchall()]
