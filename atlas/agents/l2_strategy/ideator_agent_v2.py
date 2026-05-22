print("=== IDEATOR V2 LOADED ===", flush=True)

import asyncio
import json
import random
import time as _time
from datetime import datetime
from loguru import logger
from redis.asyncio import Redis
import os

from atlas.core.agent_base import BaseAgent
from atlas.core.messaging import MessagingClient, Channel
from atlas.core.claude_client import claude as _claude
from atlas.config.settings import settings
from atlas.data.storage.timescale_client import TimescaleClient
from atlas.agents.l2_strategy.strategy_normalizer import (
    normalize_strategy,
    validate_strategy,
    compute_strategy_signature,
    _known_features_in,
    _FEATURE_FAMILIES,
)
from atlas.core.execution_cost_intelligence import (
    generate_cost_priors,
    estimate_round_trip_cost,
    classify_cost_profile,
    get_cost_governance_thresholds,
)

ARCHETYPES = [
    "momentum",
    "mean_reversion",
    "breakout",
    "volatility_regime",
    "trend_following",
]

TEMP_MAP = [0.4, 0.7, 0.5, 0.85, 1.0]

ALLOWED_FEATURES = [
    "returns",
    "rsi_14",
    "macd",
    "macd_signal",
    "ema_12",
    "ema_26",
    "sma_20",
    "bollinger_upper",
    "bollinger_lower",
    "vwap",
    "close",
    "open",
    "high",
    "low",
    "volume",
    "price_vs_vwap_pct",
    "ema_spread_pct",
    "relative_volume",
    "bollinger_band_position",
    "volatility_regime",
    "trend_strength",
]

# Proven fallback templates — these DO generate trades
LOCAL_TEMPLATES = {
    ("equity", "momentum"): (
        ["ema_spread_pct > 0.001", "relative_volume > 1.3"],
        ["ema_spread_pct < 0.0", "rsi_14 > 68"],
    ),
    ("equity", "mean_reversion"): (
        ["bollinger_band_position < 0.15", "rsi_14 < 38"],
        ["bollinger_band_position > 0.75"],
    ),
    ("equity", "breakout"): (
        ["bollinger_band_position > 0.92", "relative_volume > 1.8"],
        ["rsi_14 > 72"],
    ),
    ("equity", "trend_following"): (
        ["ema_12 > ema_26", "price_vs_vwap_pct > 0.001"],
        ["ema_12 < ema_26"],
    ),
    ("equity", "volatility_regime"): (
        ["volatility_regime > 1.4", "bollinger_band_position < 0.3"],
        ["volatility_regime < 0.9"],
    ),
    ("crypto", "momentum"): (
        ["ema_spread_pct > 0.002", "relative_volume > 1.5"],
        ["ema_spread_pct < 0.0", "rsi_14 > 70"],
    ),
    ("crypto", "mean_reversion"): (
        ["rsi_14 < 32", "price_vs_vwap_pct < -0.004"],
        ["rsi_14 > 58"],
    ),
    ("crypto", "breakout"): (
        ["bollinger_band_position > 0.9", "relative_volume > 2.0"],
        ["rsi_14 > 75"],
    ),
    ("crypto", "trend_following"): (
        ["ema_12 > ema_26", "trend_strength > 0.001"],
        ["ema_12 < ema_26"],
    ),
    ("crypto", "volatility_regime"): (
        ["volatility_regime > 1.5", "rsi_14 < 40"],
        ["volatility_regime < 0.8"],
    ),
}


# Diverse fallback templates — multiple variants per (asset, archetype)
DIVERSE_TEMPLATES: dict[tuple[str, str], list[tuple[list[str], list[str]]]] = {
    ("equity", "momentum"): [
        (
            ["ema_spread_pct > 0.001", "relative_volume > 1.3"],
            ["ema_spread_pct < 0.0", "rsi_14 > 68"],
        ),
        (
            ["price_vs_vwap_pct > 0.002", "relative_volume > 1.2"],
            ["price_vs_vwap_pct < -0.001"],
        ),
        (
            ["ema_12 > ema_26", "rsi_14 > 55", "relative_volume > 1.1"],
            ["ema_12 < ema_26"],
        ),
    ],
    ("equity", "mean_reversion"): [
        (
            ["bollinger_band_position < 0.15", "rsi_14 < 38"],
            ["bollinger_band_position > 0.75"],
        ),
        (["price_vs_vwap_pct < -0.002", "rsi_14 < 35"], ["price_vs_vwap_pct > 0.001"]),
        (
            ["bollinger_band_position < 0.2", "ema_spread_pct < -0.001"],
            ["bollinger_band_position > 0.7"],
        ),
    ],
    ("equity", "breakout"): [
        (["bollinger_band_position > 0.92", "relative_volume > 1.8"], ["rsi_14 > 72"]),
        (
            [
                "bollinger_band_position > 0.85",
                "relative_volume > 2.0",
                "trend_strength > 0.001",
            ],
            ["rsi_14 > 75"],
        ),
        (
            [
                "volatility_regime > 1.3",
                "bollinger_band_position > 0.8",
                "relative_volume > 1.5",
            ],
            ["bollinger_band_position < 0.6"],
        ),
    ],
    ("equity", "trend_following"): [
        (["ema_12 > ema_26", "price_vs_vwap_pct > 0.001"], ["ema_12 < ema_26"]),
        (
            ["trend_strength > 0.002", "ema_spread_pct > 0.001"],
            ["trend_strength < 0.0", "ema_spread_pct < -0.001"],
        ),
        (["sma_20 > ema_12", "relative_volume > 1.2"], ["sma_20 < ema_12"]),
    ],
    ("equity", "volatility_regime"): [
        (
            ["volatility_regime > 1.4", "bollinger_band_position < 0.3"],
            ["volatility_regime < 0.9"],
        ),
        (
            [
                "volatility_regime > 1.5",
                "rsi_14 < 40",
                "bollinger_band_position < 0.25",
            ],
            ["volatility_regime < 0.9", "rsi_14 > 50"],
        ),
        (
            [
                "volatility_regime > 1.3",
                "price_vs_vwap_pct < -0.002",
                "bollinger_band_position < 0.4",
            ],
            ["volatility_regime < 1.0"],
        ),
    ],
    ("crypto", "momentum"): [
        (
            ["ema_spread_pct > 0.002", "relative_volume > 1.5"],
            ["ema_spread_pct < 0.0", "rsi_14 > 70"],
        ),
        (
            ["price_vs_vwap_pct > 0.003", "relative_volume > 1.4"],
            ["price_vs_vwap_pct < -0.002"],
        ),
        (
            ["ema_12 > ema_26", "trend_strength > 0.002", "relative_volume > 1.3"],
            ["ema_12 < ema_26"],
        ),
    ],
    ("crypto", "mean_reversion"): [
        (["rsi_14 < 32", "price_vs_vwap_pct < -0.004"], ["rsi_14 > 58"]),
        (
            ["bollinger_band_position < 0.12", "rsi_14 < 35"],
            ["bollinger_band_position > 0.7"],
        ),
        (
            [
                "price_vs_vwap_pct < -0.005",
                "rsi_14 < 30",
                "bollinger_band_position < 0.2",
            ],
            ["price_vs_vwap_pct > 0.0"],
        ),
    ],
    ("crypto", "breakout"): [
        (["bollinger_band_position > 0.9", "relative_volume > 2.0"], ["rsi_14 > 75"]),
        (
            [
                "bollinger_band_position > 0.88",
                "relative_volume > 2.2",
                "volatility_regime > 1.5",
            ],
            ["rsi_14 > 78"],
        ),
        (
            [
                "price_vs_vwap_pct > 0.005",
                "relative_volume > 1.8",
                "bollinger_band_position > 0.85",
            ],
            ["price_vs_vwap_pct < 0.001"],
        ),
    ],
    ("crypto", "trend_following"): [
        (["ema_12 > ema_26", "trend_strength > 0.001"], ["ema_12 < ema_26"]),
        (
            [
                "trend_strength > 0.003",
                "ema_spread_pct > 0.002",
                "relative_volume > 1.3",
            ],
            ["trend_strength < 0.0"],
        ),
        (["sma_20 > ema_12", "price_vs_vwap_pct > 0.002"], ["sma_20 < ema_12"]),
    ],
    ("crypto", "volatility_regime"): [
        (["volatility_regime > 1.5", "rsi_14 < 40"], ["volatility_regime < 0.8"]),
        (
            ["volatility_regime > 1.6", "bollinger_band_position < 0.2", "rsi_14 < 35"],
            ["volatility_regime < 1.0"],
        ),
        (
            [
                "volatility_regime > 1.4",
                "price_vs_vwap_pct < -0.004",
                "bollinger_band_position < 0.3",
            ],
            ["volatility_regime < 0.9"],
        ),
    ],
}


# =====================================================
# STRATEGY GRAMMAR — Phase 19A deterministic generation
# =====================================================
STRATEGY_GRAMMAR = {
    "momentum": {
        "families": ["momentum", "volume"],
        "secondary": ["trend"],
        "entry_templates": [
            ("ema_spread_pct", ">", (0.001, 0.005)),
            ("relative_volume", ">", (1.2, 2.5)),
            ("price_vs_vwap_pct", ">", (0.001, 0.005)),
            ("trend_strength", ">", (0.001, 0.004)),
        ],
        "exit_templates": [
            ("ema_spread_pct", "<", (-0.001, 0.0)),
            ("rsi_14", ">", (65, 75)),
        ],
        "valid_regimes": ["bullish", "trending", "high_vol"],
    },
    "mean_reversion": {
        "families": ["oscillator", "volatility"],
        "secondary": ["volume"],
        "entry_templates": [
            ("rsi_14", "<", (28, 38)),
            ("bollinger_band_position", "<", (0.1, 0.2)),
            ("price_vs_vwap_pct", "<", (-0.005, -0.002)),
        ],
        "exit_templates": [
            ("rsi_14", ">", (55, 65)),
            ("bollinger_band_position", ">", (0.65, 0.8)),
        ],
        "valid_regimes": ["oversold", "ranging", "normal_vol"],
    },
    "breakout": {
        "families": ["volatility", "volume"],
        "secondary": ["momentum"],
        "entry_templates": [
            ("bollinger_band_position", ">", (0.85, 0.95)),
            ("relative_volume", ">", (1.5, 2.5)),
            ("volatility_regime", ">", (1.3, 1.8)),
        ],
        "exit_templates": [
            ("rsi_14", ">", (70, 80)),
            ("bollinger_band_position", "<", (0.5, 0.7)),
        ],
        "valid_regimes": ["high_vol", "bullish", "trending"],
    },
    "volatility_expansion": {
        "families": ["volatility", "momentum"],
        "secondary": ["oscillator"],
        "entry_templates": [
            ("volatility_regime", ">", (1.3, 1.8)),
            ("rsi_14", "<", (35, 42)),
            ("bollinger_band_position", "<", (0.15, 0.3)),
        ],
        "exit_templates": [
            ("volatility_regime", "<", (0.8, 1.0)),
            ("rsi_14", ">", (50, 60)),
        ],
        "valid_regimes": ["high_vol", "low_vol"],
    },
    "trend_following": {
        "families": ["trend", "momentum"],
        "secondary": ["volume"],
        "entry_templates": [
            ("ema_12", ">", "ema_26"),
            ("trend_strength", ">", (0.001, 0.004)),
            ("price_vs_vwap_pct", ">", (0.001, 0.003)),
        ],
        "exit_templates": [
            ("ema_12", "<", "ema_26"),
            ("trend_strength", "<", (-0.001, 0.0)),
        ],
        "valid_regimes": ["bullish", "bearish", "trending"],
    },
    "volatility_regime": {
        "families": ["volatility", "oscillator"],
        "secondary": ["volume"],
        "entry_templates": [
            ("volatility_regime", ">", (1.3, 1.8)),
            ("rsi_14", "<", (35, 42)),
            ("bollinger_band_position", "<", (0.15, 0.3)),
        ],
        "exit_templates": [
            ("volatility_regime", "<", (0.8, 1.0)),
            ("rsi_14", ">", (50, 60)),
        ],
        "valid_regimes": ["high_vol", "low_vol"],
    },
    "liquidity_absorption": {
        "families": ["volume", "price"],
        "secondary": ["oscillator"],
        "entry_templates": [
            ("relative_volume", ">", (1.8, 3.0)),
            ("price_vs_vwap_pct", "<", (-0.003, -0.001)),
            ("rsi_14", "<", (30, 40)),
        ],
        "exit_templates": [
            ("relative_volume", "<", (1.0, 1.3)),
            ("price_vs_vwap_pct", ">", (0.0, 0.002)),
        ],
        "valid_regimes": ["ranging", "normal_vol", "oversold"],
    },
}


class IdeatorAgentV2(BaseAgent):
    """
    Optimized ideator — real context, compressed prompts, cached DB.
    max_tokens=1500, context refreshed every 10 cycles.
    NOW WITH: Execution Cost Intelligence Layer integration for cost-aware generation.
    Phase 19A: Deterministic grammar-based generation as DEFAULT path.
              Claude repositioned as optional meta-advisor (USE_LLM_META_ADVISOR).
    """

    def __init__(
        self,
        instance_id: int,
        temperature: float,
        redis_client: Redis,
        db_client: TimescaleClient,
        mode: str = "rich",  # "rich" | "lean" | "local"
    ):
        super().__init__(
            name=f"IdeatorV2_{instance_id}_{mode[:1].upper()}",
            agent_type="ideator",
            layer="L2",
            redis_client=redis_client,
        )
        self.instance_id = instance_id
        self.temperature = max(0.0, min(1.0, temperature))
        self.mode = mode
        self._claude = _claude
        self.db_client = db_client
        self.messaging = MessagingClient(redis_client)
        self._archetype = ARCHETYPES[instance_id % len(ARCHETYPES)]
        self._asset_class = "equity" if instance_id % 2 == 0 else "crypto"

        # Context cache — refresh every 10 cycles
        self._ctx_cache: dict = {}
        self._ctx_cycle: int = 0
        self._CACHE_TTL: int = 10
        self._failure_warned: bool = False
        self._context_enabled: bool = True


        # Phase 26: Scout coupling state
        self._scout_archetype_weights: dict[str, float] | None = None
        self._scout_aggression_factor: float = 1.0
        self._scout_confidence_modulator: float = 1.0
        self._scout_liquidity_sensitivity: float = 1.0

        # Diversity governance — reject strategies with >70% feature overlap
        # 55-70% zone: soft penalty (feature variety encouraged but not rigidly enforced)
        self.DIVERSITY_SIMILARITY_THRESHOLD = 0.70
        self.DIVERSITY_SOFT_PENALTY_START = 0.55
        
        # Cost Intelligence Feature Flag
        self._cost_intelligence_enabled = os.environ.get(
            "EXECUTION_COST_INTELLIGENCE", "ADVISORY"
        ) in ("ADVISORY", "ENFORCED", "FULL", "ON")

        # Phase 19A: LLM Meta-Advisor flag
        # When False: ALL generation is deterministic (grammar + templates)
        # When True: deterministic generation + optional Claude advisory enrichment
        self._llm_meta_advisor = os.environ.get(
            "USE_LLM_META_ADVISOR", "false"
        ).lower() == "true"

    async def run(self):
        logger.info(
            f"{self.name}: START mode={self.mode} "
            f"arch={self._archetype} asset={self._asset_class}"
        )
        while True:
            try:
                # Refresh context every 10 cycles only
                if self._ctx_cycle % self._CACHE_TTL == 0:
                    self._ctx_cache = await self._build_context()
                    logger.debug(f"{self.name}: Context refreshed")
                self._ctx_cycle += 1

                spec, prompt, raw = await self._generate(self._ctx_cache)

                if not spec:
                    await asyncio.sleep(15)
                    continue

                # Dedup — exact signature match
                sig = compute_strategy_signature(spec)
                existing = await self.db_client.get_strategy_signatures(limit=500)
                if sig in existing:
                    logger.info(f"{self.name}: Duplicate — skip")
                    await asyncio.sleep(5)
                    continue

                # Diversity governance — reject if too similar to recent strategies
                existing_combos = await self.db_client.get_recent_feature_combos(limit=50)
                accepted, div_reason = self._check_diversity(spec, existing_combos)
                if not accepted:
                    logger.info(f"{self.name}: Diversity reject — {div_reason}")
                    await asyncio.sleep(5)
                    continue

                # Economic constraint governance — reject over-trading strategies
                econ_ok, econ_reason = self._check_economic_constraints(spec)
                if not econ_ok:
                    logger.info(f"{self.name}: Economic reject — {econ_reason}")
                    await asyncio.sleep(5)
                    continue

                import os
                batch = os.environ.get("GENERATION_BATCH", None)
                strategy_id = await self.db_client.save_strategy(
                    spec,
                    status="pending_code",
                    author_agent=self.name,
                    prompt=prompt,
                    raw_response=raw,
                    strategy_signature=sig,
                    generation_batch=batch,
                )

                # Phase 26E: Log economic attribution for scout-influenced strategy
                try:
                    cached_ctx = getattr(self, '_ctx_cache', {})
                    scout_weights = cached_ctx.get("scout_archetype_weights")
                    if scout_weights:
                        scout_max_key = max(scout_weights, key=scout_weights.get)
                        await self.db_client.log_economic_attribution(
                            source_scout=f"ideator_archetype_{scout_max_key}",
                            influence_type="archetype_selection",
                            target_agent=self.name,
                            strategy_id=strategy_id,
                            strategy_name=spec.get('strategy_name', ''),
                            attribution_weight=max(scout_weights.values()),
                            survived_validation=False,  # Validation hasn't run yet
                            regime_at_time=cached_ctx.get('regime', ''),
                            entropy_at_time=self._scout_confidence_modulator,
                            before_value=1.0 / max(1, len(ARCHETYPES)),
                        )
                except Exception:
                    pass

                logger.info(
                    f"{self.name}: ✅ {spec['strategy_name']} "
                    f"entry={spec.get('entry_conditions')}"
                )

                await self.messaging.publish(
                    Channel.STRATEGY_SIGNALS,
                    {
                        "event": "new_strategy",
                        "strategy_id": strategy_id,
                        "agent": self.name,
                    },
                )

                # Lean agents run faster
                sleep = 8 if self.mode == "lean" else 12
                await asyncio.sleep(sleep)

            except Exception as e:
                logger.error(f"{self.name}: {e}", exc_info=True)
                await asyncio.sleep(10)        # ─────────────────────────────────────────────────────────
        # CONTEXT — cached, compressed
        # ─────────────────────────────────────────────────────────
    async def _build_context(self) -> dict:
        if not self._context_enabled:
            return {
                "archetype": self._archetype,
                "asset_class": self._asset_class,
                "regime": "neutral",
                "snapshot_line": "",
                "failure_summary": "Context enrichment disabled.",
                "success_summary": "Context enrichment disabled.",
                "recent_names": [],
                "pattern_intelligence": "Context enrichment disabled.",
                "diversity_intelligence": "Context enrichment disabled.",
            }
        ctx = {
            "archetype": self._archetype,
            "asset_class": self._asset_class,
            "regime": "neutral",
            "snapshot_line": "",
            "failure_summary": "No failures yet.",
            "success_summary": "No validated strategies yet.",
            "recent_names": [],
            "pattern_intelligence": "No pattern data yet.",
            "diversity_intelligence": "No data on feature usage.",
        }

        try:
            symbols = (
                ["NVDA", "SPY", "AAPL", "TSLA"]
                if self._asset_class == "equity"
                else ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
            )
            features = await self.db_client.get_latest_features(symbols, limit=1)
            if features:
                best = max(
                    features.items(),
                    key=lambda x: len(x[1]) if isinstance(x[1], dict) else 0,
                )
                sym, vals = best
                if isinstance(vals, dict):
                    # Compressed: just key values as one line
                    picks = {
                        k: round(float(v), 4)
                        for k, v in vals.items()
                        if k
                        in [
                            "rsi_14",
                            "ema_spread_pct",
                            "bollinger_band_position",
                            "relative_volume",
                            "volatility_regime",
                            "price_vs_vwap_pct",
                        ]
                        and v is not None
                    }
                    ctx["snapshot_line"] = f"{sym}: " + ", ".join(
                        f"{k}={v}" for k, v in picks.items()
                    )
                    rsi = float(vals.get("rsi_14", 50))
                    vol = float(vals.get("volatility_regime", 1.0))
                    ema = float(vals.get("ema_spread_pct", 0))
                    if rsi < 35:
                        ctx["regime"] = "oversold"
                    elif rsi > 65:
                        ctx["regime"] = "overbought"
                    elif vol > 1.4:
                        ctx["regime"] = "high_volatility"
                    elif abs(ema) > 0.003:
                        ctx["regime"] = "trending"
                    else:
                        ctx["regime"] = "ranging"

        except Exception as e:
            logger.warning(f"{self.name}: Feature fetch: {e}")

        try:
            # Summarize failures as ONE compressed line (temporal-aware)
            failed = await self.db_client.get_strategies_with_backtest(
                statuses=["failed_validation", "repair_candidate"], limit=8
            )
            if failed:
                reasons = []
                zero_trade_count = 0
                low_score_count = 0
                for s in failed:
                    params = s.get("parameters", {})
                    if isinstance(params, str):
                        try:
                            params = json.loads(params)
                        except Exception:
                            params = {}
                    notes = params.get("validation_notes", "")
                    trades = s.get("total_trades", 0) or 0
                    score = s.get("composite_score", 0) or 0
                    if int(trades) < 3:
                        zero_trade_count += 1
                    if float(score) < 30:
                        low_score_count += 1
                    if "trades" in str(notes):
                        reasons.append("too few trades")
                    elif "composite" in str(notes) or "score" in str(notes):
                        reasons.append("low composite score")
                    elif "drawdown" in str(notes):
                        reasons.append("high drawdown")

                parts = []
                if zero_trade_count > 0:
                    parts.append(
                        f"{zero_trade_count}/{len(failed)} had <3 trades "
                        f"(thresholds too extreme)"
                    )
                if low_score_count > 0:
                    parts.append(
                        f"{low_score_count}/{len(failed)} had low composite score"
                    )
                ctx["failure_summary"] = (
                    "; ".join(parts)
                    if parts
                    else f"{len(failed)} failed: " + ", ".join(set(reasons))
                )

        except Exception as e:
            if not self._failure_warned:
                logger.warning(f"{self.name}: Failure fetch: {e}")
                self._failure_warned = True
            if any(
                c in str(e)
                for c in ["profit_factor", "composite_score", "does not exist"]
            ):
                self._context_enabled = False

        try:
            # Summarize successes as ONE compressed line (temporal-aware)
            wins = await self.db_client.get_top_strategies_by_composite_score(
                min_score=30, max_score=100, limit=3
            )
            if wins:
                lines = []
                for w in wins:
                    params = w.get("parameters", {})
                    if isinstance(params, str):
                        try:
                            params = json.loads(params)
                        except Exception:
                            params = {}
                    entry = params.get("entry_conditions", [])
                    score = round(float(w.get("short_window_score", 0)), 1)
                    lines.append(f"entry={entry} temporal_score={score}")
                ctx["success_summary"] = " | ".join(lines)

        except Exception as e:
            if not self._failure_warned:
                logger.warning(f"{self.name}: Success fetch: {e}")
                self._failure_warned = True

        try:
            ctx["recent_names"] = await self.db_client.get_recent_strategy_names(
                limit=15
            )
        except Exception:
            pass

        try:
            patterns = await self.db_client.get_top_patterns(
                min_confidence=0.0, limit=10
            )
            lines = []
            for p in patterns.get("winning", []):
                ff = ", ".join(p.get("feature_family", [])[:3]) or "any"
                lines.append(
                    f"[WIN] {p['archetype']} ({p['asset_class']}) "
                    f"score={p['composite_score_avg']} "
                    f"features=[{ff}] "
                    f"conf={p['confidence_score']}"
                )
            for p in patterns.get("neutral", []):
                ff = ", ".join(p.get("feature_family", [])[:3]) or "any"
                lines.append(
                    f"[NEUTRAL] {p['archetype']} ({p['asset_class']}) "
                    f"score={p['composite_score_avg']} "
                    f"features=[{ff}]"
                )
            for p in patterns.get("losing", []):
                ff = ", ".join(p.get("feature_family", [])[:3]) or "any"
                lines.append(
                    f"[AVOID] {p['archetype']} ({p['asset_class']}) "
                    f"score={p['composite_score_avg']} "
                    f"features=[{ff}]"
                )
            for p in patterns.get("cost_traps", []):
                ff = ", ".join(p.get("feature_family", [])[:3]) or "any"
                lines.append(
                    f"[COST_TRAP] {p['archetype']} ({p['asset_class']}) "
                    f"burden={p['cost_burden_avg']} "
                    f"features=[{ff}]"
                )
            ctx["pattern_intelligence"] = (
                "\n".join(lines) if lines else "No pattern data yet."
            )
        except Exception as e:
            ctx["pattern_intelligence"] = f"Pattern fetch failed: {e}"

        # ─────────────────────────────────────────────────────────
        # DIVERSITY INTELLIGENCE CONTEXT (NEW)
        # ─────────────────────────────────────────────────────────
        try:
            combos = await self.db_client.get_recent_feature_combos(limit=50)
            if combos:
                # Aggregate overrepresented feature families per archetype
                from collections import Counter
                arch_counter: dict[str, Counter] = {}
                for features, archetype in combos:
                    if archetype not in arch_counter:
                        arch_counter[archetype] = Counter()
                    for feat in features:
                        arch_counter[archetype][feat] += 1
                
                lines = []
                for archetype, counter in sorted(arch_counter.items()):
                    # Print top-5 most overused features for this archetype
                    top = counter.most_common(5)
                    feat_list = ", ".join(f"{f} ({c}x)" for f, c in top)
                    lines.append(f"{archetype}: {feat_list}")
                
                ctx["diversity_intelligence"] = (
                    "OVERUSED FEATURES BY ARCHETYPE (avoid these combos):\n"
                    + "\n".join(lines)
                )
            else:
                ctx["diversity_intelligence"] = "No data on feature usage."
        except Exception as e:
            ctx["diversity_intelligence"] = f"Diversity fetch failed: {e}"

        try:
            import os
            mutation_enabled = os.environ.get("MUTATION_INTELLIGENCE", "OFF") == "ON"
            if not mutation_enabled:
                ctx["mutation_intelligence"] = "Mutation Intelligence DISABLED via env."
            else:
                mutations = await self.db_client.get_mutation_leaderboard()
                if mutations:
                    m_lines = []
                    for m in mutations:
                        if m["conversion_rate"] is None:
                            continue
                        if m["conversion_rate"] > 30 and m["avg_score_delta"] > 0:
                            m_lines.append(f"[HIGH CONVICTION] {m['mutation_type']} (conv={m['conversion_rate']}%, delta={m['avg_score_delta']:+.2f})")
                        elif m["conversion_rate"] < 10 and m["avg_score_delta"] < 0:
                            m_lines.append(f"[AVOID] {m['mutation_type']} (conv={m['conversion_rate']}%, delta={m['avg_score_delta']:+.2f})")
                        else:
                            m_lines.append(f"[NEUTRAL] {m['mutation_type']} (conv={m['conversion_rate']}%, delta={m['avg_score_delta']:+.2f})")
                    ctx["mutation_intelligence"] = "\n".join(m_lines) if m_lines else "No actionable mutation data."
                else:
                    ctx["mutation_intelligence"] = "No mutation data available."
        except Exception as e:
            ctx["mutation_intelligence"] = f"Mutation fetch failed: {e}"

        # ─────────────────────────────────────────────────────────
        # SCOUT INTELLIGENCE CONTEXT (Phase 10)
        # ─────────────────────────────────────────────────────────
        try:
            scout = await self.db_client.get_latest_scout_intelligence()
            scout_lines = []
            if "regime" in scout:
                r = scout["regime"]
                scout_lines.append(
                    f"Market: vol={r.get('volatility','?')}, trend={r.get('trend','?')}, "
                    f"liq={r.get('liquidity','?')}, corr={r.get('correlation','?')}"
                )
            if "liquidity" in scout:
                liq = scout["liquidity"]
                scout_lines.append(
                    f"Liquidity: regime={liq.get('regime','?')}, "
                    f"score={liq.get('score',0):.0f}, risk={liq.get('risk',0):.1f}"
                )
            if "correlation" in scout:
                c = scout["correlation"]
                scout_lines.append(
                    f"Correlation: cluster={c.get('cluster','?')}, "
                    f"avg_corr={c.get('avg_corr',0):.2f}, state={c.get('risk_state','?')}"
                )
            if "execution" in scout:
                e = scout["execution"]
                scout_lines.append(
                    f"Execution: regime={e.get('regime','?')}, "
                    f"fill_score={e.get('fill_score',0):.0f}, slippage={e.get('slippage_bps',0):.1f}bps"
                )
            ctx["scout_intelligence"] = (
                "\n".join(scout_lines) if scout_lines else "Scout intelligence not yet available."
            )
        except Exception as e:
            ctx["scout_intelligence"] = f"Scout fetch failed: {e}"

        # SCOUT SIGNALS ENRICHMENT (Phase 25D) - only if main fetch succeeded
        if not ctx.get("scout_intelligence", "").startswith("Scout fetch failed"):
            try:
                ss = await self.db_client.get_scout_signal_summary()
                if ss and ss.get("total_signals", 0) > 0:
                    enriched_lines = []
                    for (src, sig_type), info in ss.get("by_source", {}).items():
                        enriched_lines.append(
                            f"  {src}: {info['count']} signals "
                            f"(type={sig_type}, "
                            f"avg_conf={info['avg_confidence']})"
                        )
                    for sig in ss.get("recent", []):
                        sym = sig.get("symbol") or "market"
                        enriched_lines.append(
                            f"  -> {sig['source']} flagged {sym} "
                            f"({sig['type']}, conf={sig['confidence']})"
                        )
                    existing = ctx.get("scout_intelligence", "")
                    ctx["scout_intelligence"] = existing + "\n\n" + "\n".join(enriched_lines)
            except Exception as e:
                logger.debug(f"{self.name}: Scout signals enrichment skipped: {e}")

        # ─────────────────────────────────────────────────────────
        # COST INTELLIGENCE CONTEXT (NEW)
        # ─────────────────────────────────────────────────────────
        if self._cost_intelligence_enabled:
            try:
                target_frequency = {
                    "momentum": 45,
                    "mean_reversion": 35,
                    "breakout": 25,
                    "trend_following": 18,
                    "volatility_regime": 20,
                }.get(self._archetype, 30)
                cost_priors = generate_cost_priors(
                    asset_class=self._asset_class,
                    archetype=self._archetype,
                    trade_frequency_target=target_frequency,
                )
                governance = get_cost_governance_thresholds(
                    target_frequency,
                    self._asset_class,
                )
                ctx["cost_intelligence"] = (
                    f"EXECUTION COST AWARENESS (Advisory):\n"
                    f"{cost_priors['cost_principle']}\n\n"
                    f"FREQUENCY GUIDANCE:\n{cost_priors['frequency_guidance']}\n\n"
                    f"COST AVOIDANCE:\n{cost_priors['cost_avoidance']}\n\n"
                    f"EDGE REQUIREMENT:\n{cost_priors['edge_requirement']}\n\n"
                    f"GOVERNANCE THRESHOLDS:\n"
                    f"min_edge_per_trade_bps={governance['min_edge_per_trade_bps']:.1f}, "
                    f"min_win_rate={governance['min_win_rate']:.2f}, "
                    f"min_profit_factor={governance['min_profit_factor']:.2f}, "
                    f"risk_category={governance['risk_category']}"
                )
                rt_cost_bps = estimate_round_trip_cost(self._asset_class, bps=True)
                ctx["cost_metrics"] = (
                    f"Round-trip cost: {rt_cost_bps:.0f} bps | "
                    f"Target frequency: {target_frequency} | "
                    f"Avoid: excessive crossover frequency, hyper-reactive RSI bands, ultra-tight exits"
                )
            except Exception as e:
                logger.warning(f"{self.name}: Cost intelligence fetch failed: {e}")
                ctx["cost_intelligence"] = "Cost Intelligence UNAVAILABLE"
                ctx["cost_metrics"] = ""
        else:
            ctx["cost_intelligence"] = "Cost Intelligence DISABLED via env"
            ctx["cost_metrics"] = ""


        # Phase 26A: Scout-aware archetype weighting and modulation
        try:
            regime_for_mod = ctx.get("regime", "neutral")
            scout_text = ctx.get("scout_intelligence", "")
            archetype_weights = self._compute_scout_archetype_weights(
                regime_for_mod, scout_text
            )
            ctx["scout_archetype_weights"] = archetype_weights
            ctx["scout_aggression_factor"] = self._compute_scout_aggression(
                regime_for_mod, scout_text
            )
            ctx["scout_timeframe_preference"] = self._compute_scout_timeframe(
                scout_text
            )

            # Log scout influence events
            import uuid
            if archetype_weights:
                dominant_archetype = max(archetype_weights, key=archetype_weights.get)
                await self.db_client.log_scout_influence(
                    source_scout="regime_scout",
                    target_agent=self.name,
                    influence_type="archetype_weighting",
                    influence_metric=dominant_archetype,
                    delta=archetype_weights.get(dominant_archetype, 0) - (1.0 / max(1, len(ARCHETYPES))),
                    confidence=0.6,
                    regime_context=regime_for_mod,
                    entropy_context=self._scout_confidence_modulator,
                    metadata={"all_weights": archetype_weights},
                )

            # Log aggression modulation
            if ctx["scout_aggression_factor"] != 1.0:
                await self.db_client.log_scout_influence(
                    source_scout="liquidity_scout",
                    target_agent=self.name,
                    influence_type="aggression_modulation",
                    influence_metric="aggression_factor",
                    before_value=1.0,
                    after_value=ctx["scout_aggression_factor"],
                    delta=ctx["scout_aggression_factor"] - 1.0,
                    confidence=0.5,
                    regime_context=regime_for_mod,
                )
        except Exception as e:
            logger.debug(f"{self.name}: Scout modulation failed: {e}")
            ctx["scout_archetype_weights"] = None
            ctx["scout_aggression_factor"] = 1.0
            ctx["scout_timeframe_preference"] = "1m"


        return ctx

    # ─────────────────────────────────────────────────────────
    # DIVERSITY GOVERNANCE
    # ─────────────────────────────────────────────────────────

    # ================================================================
    # PHASE 26A — SCOUT-AWARE ARCHETYPE AND AGGRESSION MODULATION
    # ================================================================

    def _compute_scout_archetype_weights(
        self, regime: str, scout_text: str
    ) -> dict[str, float]:
        """Compute scout-informed archetype weights based on regime and market conditions.
        
        Returns {archetype: weight} dict that can bias archetype selection.
        Higher weight = more likely to be selected.
        """
        base_weight = 1.0 / len(ARCHETYPES)
        weights = {a: base_weight for a in ARCHETYPES}

        # Regime-based modulation
        regime_lower = regime.lower()
        if "oversold" in regime_lower or "bearish" in regime_lower:
            weights["mean_reversion"] *= 1.8
            weights["momentum"] *= 0.6
            weights["breakout"] *= 0.4
            weights["trend_following"] *= 0.7
            weights["volatility_regime"] *= 1.3
        elif "overbought" in regime_lower or "bullish" in regime_lower:
            weights["momentum"] *= 1.6
            weights["trend_following"] *= 1.5
            weights["breakout"] *= 1.4
            weights["mean_reversion"] *= 0.5
        elif "high_vol" in regime_lower or "panic_vol" in regime_lower:
            weights["volatility_regime"] *= 2.0
            weights["breakout"] *= 1.3
            weights["mean_reversion"] *= 1.5
            weights["momentum"] *= 0.5
            weights["trend_following"] *= 0.6
        elif "ranging" in regime_lower or "neutral" in regime_lower:
            weights["mean_reversion"] *= 1.6
            weights["volatility_regime"] *= 1.2
            weights["momentum"] *= 0.8
            weights["breakout"] *= 0.7
            weights["trend_following"] *= 0.9

        # Scout text parsing for additional signals
        scout_lower = scout_text.lower()
        if "liquidity" in scout_lower and ("thin" in scout_lower or "stressed" in scout_lower):
            # Low liquidity: prefer lower-frequency archetypes
            weights["breakout"] *= 0.5
            weights["momentum"] *= 0.7
            weights["mean_reversion"] *= 1.3
        if "volatility" in scout_lower:
            if "high" in scout_lower or "panic" in scout_lower:
                weights["volatility_regime"] *= 1.5
            elif "low" in scout_lower:
                weights["volatility_regime"] *= 0.6
        if "correlation" in scout_lower and "spike" in scout_lower:
            # Correlation spikes: reduce regime-dependent strategies
            weights["volatility_regime"] *= 1.4
            weights["momentum"] *= 0.8

        # Normalize to sum to 1.0
        total = sum(weights.values())
        if total > 0:
            weights = {k: v / total for k, v in weights.items()}
        return weights

    def _compute_scout_aggression(self, regime: str, scout_text: str) -> float:
        """Compute aggression factor from scout conditions.
        1.0 = standard aggression.
        < 1.0 = more conservative (wider stops, lower leverage).
        > 1.0 = more aggressive (tighter stops, normal leverage).
        """
        factor = 1.0
        regime_lower = regime.lower()

        # High volatility -> lower aggression
        if "high_vol" in regime_lower or "panic_vol" in regime_lower:
            factor *= 0.6
        # Trending -> normal to slightly higher aggression
        elif "trending" in regime_lower:
            factor *= 1.1
        # Oversold -> moderate aggression (mean reversion)
        elif "oversold" in regime_lower:
            factor *= 0.9

        scout_lower = scout_text.lower()
        if "thin" in scout_lower or "stressed" in scout_lower:
            factor *= 0.5
        if "degraded" in scout_lower or "unstable" in scout_lower:
            factor *= 0.4
        if "spike" in scout_lower:
            factor *= 0.7

        return max(0.2, min(2.0, factor))

    def _compute_scout_timeframe(self, scout_text: str) -> str:
        """Select preferred timeframe based on scout conditions."""
        scout_lower = scout_text.lower()
        if "thin" in scout_lower or "stressed" in scout_lower:
            return "5m"  # Lower frequency when liquidity is poor
        if "degraded" in scout_lower:
            return "5m"
        if "panic" in scout_lower:
            return "15m"  # Very conservative in panic
        return "1m"  # Default


    def _check_diversity(
        self,
        spec: dict,
        recent_combos: list[tuple[set[str], str]],
    ) -> tuple[bool, str]:
        """
        Two-tier diversity governance:
        - >= 70% overlap: HARD REJECT (too similar, collapses search space)
        - 55-70% overlap: SOFT PENALTY (allowed but logged for monitoring)
        - < 55%: PASS freely

        Compares feature sets using Jaccard similarity.
        Guards against Claude collapsing into 100 variants of RSI+Bollinger.
        """
        entry = spec.get("entry_conditions", [])
        exit_ = spec.get("exit_conditions", [])

        # Extract all feature names used in this strategy's conditions
        new_features: set[str] = set()
        for cond in entry + exit_:
            if isinstance(cond, str):
                new_features |= _known_features_in(cond)

        if not new_features:
            return True, "no features to compare"

        # Compute feature families for family-level diversity check
        new_families: set[str] = set()
        for feat in new_features:
            family = _FEATURE_FAMILIES.get(feat)
            if family:
                new_families.add(family)

        # Check for single-family collapse (e.g. ONLY oscillators)
        if len(new_families) == 1:
            return False, (
                f"single feature family only ({list(new_families)[0]}) — "
                f"diverse families required"
            )

        max_overlap = 0.0
        worst_match = ""
        soft_penalty_logged = False

        for existing_features, archetype in recent_combos:
            if not existing_features:
                continue
            # Jaccard similarity: |intersection| / |union|
            intersection = new_features & existing_features
            union = new_features | existing_features
            jaccard = len(intersection) / len(union) if union else 0.0

            if jaccard > max_overlap:
                max_overlap = jaccard
                worst_match = archetype

            # HARD REJECT: overlap >= 70%
            if jaccard >= self.DIVERSITY_SIMILARITY_THRESHOLD:
                shared = ", ".join(sorted(intersection)[:5])
                return False, (
                    f"feature overlap {jaccard:.0%} with recent {archetype} strategy "
                    f"(shared: {shared}) — above {self.DIVERSITY_SIMILARITY_THRESHOLD:.0%} hard reject threshold"
                )

        # Log soft penalty ONCE after loop (not per-comparison to avoid spam)
        if not soft_penalty_logged and max_overlap >= self.DIVERSITY_SOFT_PENALTY_START:
            soft_penalty_logged = True
            logger.info(
                f"{self.name}: Diversity soft penalty — {max_overlap:.0%} max overlap "
                f"(archetype={worst_match}) — throughput allowed"
            )

        # Log the best match for monitoring
        if max_overlap > 0:
            logger.debug(
                f"Diversity: closest match = {max_overlap:.0%} (archetype={worst_match}), "
                f"features={sorted(new_features)}"
            )

        return True, f"diverse (max_overlap={max_overlap:.0%})"

    # ─────────────────────────────────────────────────────────
    # ECONOMIC CONSTRAINT GOVERNANCE
    # ─────────────────────────────────────────────────────────

    _LOOSE_THRESHOLD_RULES: dict[str, list[tuple[str, float, str]]] = {
        # feature: [(op, limit, direction)]
        # direction "above" = value > limit triggers looseness warning
        # direction "below" = value < limit triggers looseness warning
        "rsi_14": [("<", 40, "below"), (">", 60, "below")],
        "bollinger_band_position": [("<", 0.3, "below")],
        "relative_volume": [(">", 1.2, "below")],
        "ema_spread_pct": [(">", 0.0005, "below")],
        "price_vs_vwap_pct": [(">", 0.001, "below")],
    }

    def _estimate_trade_risk(self, spec: dict) -> tuple[str, str]:
        """
        Heuristically estimate whether a strategy would generate excessive trades
        by analyzing condition threshold tightness and pattern risk factors.

        Returns ("low"|"medium"|"high", detailed_reason).
        """
        import re
        entry = spec.get("entry_conditions", [])
        exit_ = spec.get("exit_conditions", [])
        all_conds = entry + exit_

        if not all_conds:
            return "low", "no conditions to evaluate"

        loose_count = 0
        loose_details: list[str] = []

        for cond in all_conds:
            if not isinstance(cond, str):
                continue
            cond_lower = cond.lower()
            for feature, rules in self._LOOSE_THRESHOLD_RULES.items():
                if feature not in cond_lower:
                    continue
                for op, limit, direction in rules:
                    # Extract numeric threshold after the operator
                    pattern = re.escape(op) + r"\s*([\d.]+)"
                    m = re.search(re.escape(feature) + r"\s*" + pattern, cond_lower)
                    if not m:
                        continue
                    try:
                        val = float(m.group(1))
                    except (ValueError, IndexError):
                        continue

                    is_loose = False
                    if op == "<" and val > limit:
                        is_loose = True
                    elif op == ">" and val < limit:
                        is_loose = True

                    if is_loose:
                        loose_count += 1
                        loose_details.append(
                            f"{feature} {op} {val} (should be {op} {limit})"
                        )

        # Check for micro-scalping: oscillator-only with no trend/volume filter
        features: set[str] = set()
        for cond in all_conds:
            if isinstance(cond, str):
                features |= _known_features_in(cond)
        families: set[str] = set()
        for f in features:
            fam = _FEATURE_FAMILIES.get(f)
            if fam:
                families.add(fam)

        oscillator_only = (
            len(families) <= 2
            and "oscillator" in families
            and "trend" not in families
            and "volume" not in families
        )

        # Check for ultra-tight exit patterns
        tight_exit = False
        for cond in exit_:
            if isinstance(cond, str):
                c = cond.lower()
                # Exits that would trigger on almost every bar
                if "rsi_14 > 55" in c or "rsi_14 > 50" in c:
                    tight_exit = True
                if "bollinger_band_position > 0.6" in c or "bollinger_band_position > 0.5" in c:
                    tight_exit = True
                if "ema_spread_pct < 0.0" in c:
                    tight_exit = True
                if "price_vs_vwap_pct > 0.0" in c or "price_vs_vwap_pct < 0.0" in c:
                    tight_exit = True

        # Score risk
        risk_score = loose_count * 2
        reasons: list[str] = []

        if oscillator_only:
            risk_score += 3
            reasons.append("oscillator-only with no trend/volume filter")

        if tight_exit:
            risk_score += 2
            reasons.append("ultra-tight exit pattern (fires on most bars)")

        if loose_count > 0:
            reasons.append(f"{loose_count} loose threshold(s)")

        if risk_score >= 5:
            return "high", "; ".join(reasons) + " → likely >150 trades"
        elif risk_score >= 3:
            return "medium", "; ".join(reasons)
        return "low", "; ".join(reasons) if reasons else "acceptable"

    def _check_economic_constraints(self, spec: dict) -> tuple[bool, str]:
        """
        Hard gateway that rejects strategies likely to fail economic survivability.
        Checks:
        1. Trade frequency risk (threshold looseness)
        2. Micro-scalping patterns (oscillator-only, no trend/volume filter)
        3. Minimum edge implied by entry/exit asymmetry

        Returns (accepted: bool, reason: str).
        """
        # Trade frequency check
        risk, risk_reason = self._estimate_trade_risk(spec)

        if risk == "high":
            return False, (
                f"HIGH TRADE FREQUENCY RISK: {risk_reason}. "
                f"Rejected — strategies with >150 estimated trades on 1m data "
                f"will not survive execution costs."
            )

        # Check for asymmetric reward:risk
        entry = spec.get("entry_conditions", [])
        exit_ = spec.get("exit_conditions", [])

        # Simple check: if exit has more conditions than entry, it's more restrictive
        # which means exits may not fire often enough → holding losers too long
        if len(exit_) > len(entry) + 1:
            logger.debug(f"Economic: exit ({len(exit_)}) >> entry ({len(entry)}) conditions — monitor")

        # Check for missing stop/take profit
        sl = spec.get("stop_loss", "")
        tp = spec.get("take_profit", "")
        if not sl or not tp:
            logger.debug(f"Economic: missing stop_loss or take_profit for {spec.get('strategy_name')}")

        return True, f"economic_ok (risk={risk}: {risk_reason})"

    # ─────────────────────────────────────────────────────────
    # GENERATE DISPATCHER — Phase 19A: Deterministic-first
    # ─────────────────────────────────────────────────────────
    async def _generate(self, ctx: dict):
        if self.mode == "local":
            return await self._generate_local(ctx)

        # Phase 19A: ALL modes now default to deterministic grammar generation
        spec, prompt, raw = await self._generate_deterministic_candidates(ctx)

        if spec and self._llm_meta_advisor:
            # Optional: Claude enriches metadata (name, hypothesis, reasoning)
            # but NEVER modifies entry/exit conditions or thresholds
            spec = await self._llm_advisory_enrichment(spec, ctx)

        if spec:
            return spec, prompt, raw

        # Fallback chain: deterministic failed → try Claude if enabled → template
        if self._llm_meta_advisor:
            try:
                max_tokens = 800 if self.mode == "lean" else 1500
                result = await self._call_claude(ctx, max_tokens=max_tokens)
                if result and result[0]:
                    return result
            except Exception as e:
                logger.warning(f"{self.name}: Claude fallback failed: {e}")

        # Ultimate fallback: proven templates
        return await self._generate_local(ctx)

    # ─────────────────────────────────────────────────────────
    # DETERMINISTIC GRAMMAR ENGINE — Phase 19A
    # ─────────────────────────────────────────────────────────
    async def _generate_deterministic_candidates(
        self, ctx: dict
    ) -> tuple[dict | None, str | None, str | None]:
        """
        Generate strategy candidates using structured grammar + probabilistic
        feature selection + regime awareness. No LLM calls.
        """
        asset = ctx["asset_class"]
        archetype = ctx["archetype"]
        regime = ctx.get("regime", "neutral")

        grammar = STRATEGY_GRAMMAR.get(archetype)
        if not grammar:
            return None, None, None
        # Phase 26A: Scout-aware archetype modulation
        scout_weights = ctx.get("scout_archetype_weights")
        if scout_weights:
            import random
            # Weighted random choice based on scout conditions
            arch_keys = list(scout_weights.keys())
            arch_weights = [scout_weights[k] for k in arch_keys]
            total_weight = sum(arch_weights)
            if total_weight <= 0:
                arch_weights = [1.0 / len(arch_keys)] * len(arch_keys)
            modulated_archetype = random.choices(arch_keys, weights=arch_weights, k=1)[0]
            if modulated_archetype != archetype:
                logger.info(
                    f"{self.name}: Scout modulated archetype "
                    f"{archetype} -> {modulated_archetype} "
                    f"(weights={scout_weights})"
                )
            archetype = modulated_archetype
            grammar = STRATEGY_GRAMMAR.get(archetype)
            if not grammar:
                archetype = ctx["archetype"]
                grammar = STRATEGY_GRAMMAR.get(archetype)

        # Scout aggression and timeframe modulation
        aggression = ctx.get("scout_aggression_factor", 1.0)
        timeframe = ctx.get("scout_timeframe_preference", "1m")


        # Select entry conditions from grammar templates
        entry_pool = list(grammar["entry_templates"])
        exit_pool = list(grammar["exit_templates"])

        # Shuffle for diversity
        random.shuffle(entry_pool)
        random.shuffle(exit_pool)

        # Pick 2-3 entry conditions, 1-2 exit conditions
        n_entry = random.choice([2, 2, 3])
        n_exit = random.choice([1, 1, 2])

        entry_conditions = []
        for tmpl in entry_pool[:n_entry]:
            cond = self._resolve_grammar_template(tmpl, asset)
            if cond:
                entry_conditions.append(cond)

        exit_conditions = []
        for tmpl in exit_pool[:n_exit]:
            cond = self._resolve_grammar_template(tmpl, asset)
            if cond:
                exit_conditions.append(cond)

        if not entry_conditions or not exit_conditions:
            return None, None, None

        # Build valid_regimes from grammar + regime context
        valid_regimes = list(grammar.get("valid_regimes", []))

        # Construct spec
        suffix = datetime.utcnow().strftime("%H%M%S") + f"_{random.randint(10, 99)}"
        spec = {
            "strategy_name": f"{archetype}_{asset}_det_{suffix}",
            "hypothesis": f"Deterministic {archetype} on {asset} 1m — grammar-generated",
            "reasoning": f"Grammar engine: {archetype} archetype with {', '.join(grammar['families'])} families",
            "entry_conditions": entry_conditions,
            "exit_conditions": exit_conditions,
            "valid_regimes": valid_regimes,
            "stop_loss": "0.5% below entry",
            "take_profit": "1.0% above entry",
            "position_sizing": "10% of portfolio",
            "timeframe": "1m",
            "asset_class": asset,
            "expected_sharpe": 1.0,
            "expected_win_rate": 0.50,
            "risk_level": "medium",
            "tags": [archetype, asset, "deterministic", "grammar"],
        }

        spec = normalize_strategy(spec)
        valid, reason = validate_strategy(spec)
        if not valid:
            logger.debug(f"{self.name}: Grammar spec invalid: {reason}")
            return None, None, None

        return spec, "deterministic_grammar", None

    def _resolve_grammar_template(
        self, tmpl: tuple, asset: str
    ) -> str | None:
        """Resolve a grammar template to a concrete condition string."""
        feature, op, threshold = tmpl

        # Cross-feature comparison (e.g., ema_12 > ema_26)
        if isinstance(threshold, str):
            return f"{feature} {op} {threshold}"

        # Numeric range — sample uniformly
        if isinstance(threshold, tuple) and len(threshold) == 2:
            lo, hi = threshold
            # Asset-class adjustment for crypto (wider thresholds)
            if asset == "crypto":
                if feature in ("ema_spread_pct", "price_vs_vwap_pct", "trend_strength"):
                    lo *= 1.5
                    hi *= 1.5
                elif feature == "relative_volume":
                    lo = max(lo, 1.3)
                    hi = max(hi, lo + 0.5)

            val = round(random.uniform(lo, hi), 4)
            return f"{feature} {op} {val}"

        return None

    async def _llm_advisory_enrichment(
        self, spec: dict, ctx: dict
    ) -> dict:
        """
        Phase 19A: Optional Claude enrichment for deterministic strategies.
        ONLY enriches metadata (name, hypothesis, reasoning).
        NEVER modifies entry_conditions, exit_conditions, or thresholds.
        """
        try:
            entry_str = ", ".join(spec.get("entry_conditions", []))
            exit_str = ", ".join(spec.get("exit_conditions", []))

            raw = await self._claude.complete(
                user=(
                    f"Given this {spec.get('asset_class')} {spec.get('tags', [''])[0]} strategy:\n"
                    f"Entry: {entry_str}\nExit: {exit_str}\n"
                    f"Regime: {ctx.get('regime', 'neutral')}\n\n"
                    f"Provide ONLY a JSON object with:\n"
                    f'{{"strategy_name": "creative_name", '
                    f'"hypothesis": "one sentence hypothesis", '
                    f'"reasoning": "why this could work"}}\n'
                    f"Do NOT modify conditions or thresholds."
                ),
                system="You label trading strategies with creative names and hypotheses. Output ONLY JSON.",
                max_tokens=200,
                temperature=0.6,
            )

            cleaned = raw.strip()
            f = cleaned.find("{")
            l = cleaned.rfind("}")
            if f != -1 and l != -1:
                enrichment = json.loads(cleaned[f:l + 1])
                # ONLY overwrite metadata fields, NEVER conditions
                if "strategy_name" in enrichment:
                    spec["strategy_name"] = enrichment["strategy_name"]
                if "hypothesis" in enrichment:
                    spec["hypothesis"] = enrichment["hypothesis"]
                if "reasoning" in enrichment:
                    spec["reasoning"] = enrichment["reasoning"]
                spec.setdefault("tags", []).append("llm_enriched")

        except Exception as e:
            logger.debug(f"{self.name}: LLM enrichment skipped: {e}")

        return spec

    # ─────────────────────────────────────────────────────────
    # CLAUDE CALL — 800 or 1500 tokens
    # ─────────────────────────────────────────────────────────
    async def _call_claude(
        self, ctx: dict, max_tokens: int
    ) -> tuple[dict | None, str | None, str | None]:

        asset = ctx["asset_class"]
        archetype = ctx["archetype"]
        regime = ctx["regime"]
        recent = ", ".join(ctx["recent_names"][:8]) or "none"

        returns_range = "-0.003 to 0.003" if asset == "equity" else "-0.008 to 0.008"

        system_prompt = (
            f"You are a quant researcher designing 1-minute {asset} "
            f"intraday strategies. Output ONLY valid JSON. "
            f"Reason internally. No markdown, no prose outside JSON."
        )

        # Build cost intelligence block if enabled
        cost_block = ""
        if self._cost_intelligence_enabled:
            cost_block = f"""
\n=== EXECUTION COST INTELLIGENCE (ADVISORY) ===
{ctx.get("cost_intelligence", "Cost Intelligence DISABLED")}
{ctx.get("cost_metrics", "")}
    AVOID LIST:
    - excessive crossover frequency
    - hyper-reactive RSI bands
    - ultra-tight exits
"""

        user_prompt = f"""Design ONE {archetype} strategy for 1-minute {asset} data.

Market: {ctx["snapshot_line"] or "no live data"}
Regime: {regime}

Recent failures: {ctx["failure_summary"]}
Working strategies: {ctx["success_summary"]}
Avoid names: {recent}

Structural Intelligence (learned patterns):
{ctx.get("pattern_intelligence", "No pattern data yet.")}

Mutation Intelligence (proven evolutionary priors):
{ctx.get("mutation_intelligence", "Mutation Intelligence DISABLED")}

Diversity Intelligence (overused features to avoid):
{ctx.get("diversity_intelligence", "No data on feature usage.")}

Scout Intelligence (live market conditions):
{ctx.get("scout_intelligence", "Scout intelligence pending.")}

RECOMMENDATIONS FROM SCOUT:
- If liquidity is thin or dangerous: discourage scalping, high-frequency churn, tight exits
- If volatility is panic_vol: require wider stops, bias toward mean reversion or skip entries altogether
- If trend is trending_up/trending_down: favor momentum/trend following, avoid mean reversion
- If execution is degraded or stressed: widen expected edge requirements, reduce trade frequency target
- If correlation is clustered or panic: avoid adding correlated positions, prefer regime-specific strategies
- If liquidity is excellent and execution optimal: standard strategy generation allowed

Scout intelligence is ADVISORY. Use it to bias your strategy construction toward the current market reality.{cost_block}
APPROVED FEATURES (use ONLY these exact names):
  rsi_14, macd, macd_signal, ema_12, ema_26, sma_20,
  bollinger_upper, bollinger_lower, vwap,
  returns, close, volume,
  price_vs_vwap_pct, ema_spread_pct,
  relative_volume, bollinger_band_position,
  volatility_regime, trend_strength

REALISTIC THRESHOLDS for {asset} 1m:
  rsi_14: 30-70 range (NOT <20 or >80 — too rare)
  returns: {returns_range} per bar
  relative_volume: 1.2-2.5 (NOT >4 — too rare)
  bollinger_band_position: 0.1-0.9
  ema_spread_pct: 0.0005-0.005
  volatility_regime: 0.8-2.0

HARD ECONOMIC CONSTRAINTS:
• Strategy WILL BE REJECTED if thresholds are too loose (entries fire on >20% of bars → 500+ trades).
• Strategy WILL BE REJECTED if oscillator-only with no trend or volume filter (micro-scalping).
• Strategy WILL BE REJECTED if ultra-tight exits (e.g. rsi_14 > 55, bollinger_position > 0.5) that fire on every bar.
• Target: 10–80 trades over entire backtest. NOT 150. NOT 500.
• Prefer wider profit targets, lower trade frequency, asymmetric risk/reward, trend persistence, durable momentum, multi-confirmation entries.
• Reject noisy microstructure scalping, immediate mean-reversion exits, ultra-tight oscillators.

REGIME INTELLIGENCE:
• Strategies MUST specify valid_regimes — the market state(s) where this strategy works.
• Valid regimes: high_vol, low_vol, normal_vol, bullish, bearish, trending, ranging, overbought, oversold.
• A momentum strategy likely works in bullish, trending, high_vol regimes.
• A mean_reversion strategy likely works in ranging, oversold, overbought regimes.
• A breakout strategy likely works in high_vol, bullish regimes.
• A trend_following strategy works best in bullish, bearish (directional), trending regimes.
• A volatility_regime strategy works best in high_vol, low_vol regimes.
• Pick 1-4 regimes that the strategy is DESIGNED for. Strategies restricted to a single regime are penalized.
• The regime filter ensures strategies don't fire in inappropriate market conditions, massively improving PF and reducing false signals.

REALISTIC THRESHOLDS for {asset} 1m:
  rsi_14: 30-70 range (NOT <20 or >80 — too rare)
  returns: {returns_range} per bar
  relative_volume: 1.2-2.5 (NOT >4 — too rare)
  bollinger_band_position: 0.1-0.9
  ema_spread_pct: 0.0005-0.005
  volatility_regime: 0.8-2.0

CRITICAL: Each condition must trigger on 5-15% of bars minimum.
Too restrictive = 0 trades = automatic failure.
Too loose = >200 trades = automatic rejection for cost suicide.

✅ CORRECT conditions: "rsi_14 < 38", "relative_volume > 1.4",
   "bollinger_band_position < 0.2", "ema_spread_pct > 0.001"
❌ WRONG: "For LONG: close breaks above..." (natural language)
❌ WRONG: "rsi_14 < 15" (too extreme, never fires)
❌ WRONG: "rsi_14 < 42" (too loose, fires on ~42% of bars = thousands of trades)
❌ WRONG: "bollinger_band_position < 0.4" (too loose, fires on ~40%)
✓ RIGHT: "bollinger_band_position < 0.2", "bollinger_band_position < 0.15"

Output ONLY this JSON:
{{"strategy_name":"unique_name","hypothesis":"one sentence",
"reasoning":"why these thresholds trigger realistically",
"entry_conditions":["feature op value","feature op value"],
"exit_conditions":["feature op value"],
"valid_regimes":["regime1","regime2"],
"stop_loss":"0.5% below entry","take_profit":"1.0% above entry",
"position_sizing":"10% of portfolio","timeframe":"1m",
"asset_class":"{asset}","expected_sharpe":1.2,
"expected_win_rate":0.52,"risk_level":"medium",
"tags":["{archetype}","{asset}"]}}"""

        try:
            raw = await self._claude.complete(
                user=user_prompt,
                system=system_prompt,
                max_tokens=max_tokens,
                temperature=self.temperature,
            )
            logger.info(f"{self.name}: Claude response:\n{raw[:400]}")

            cleaned = raw.strip()
            if "```" in cleaned:
                start = cleaned.find("\n", cleaned.find("```")) + 1
                end = cleaned.rfind("```")
                cleaned = cleaned[start:end].strip()
            f = cleaned.find("{")
            l = cleaned.rfind("}")
            if f == -1:
                raise ValueError("No JSON in response")

            spec = json.loads(cleaned[f : l + 1])
            spec["asset_class"] = asset
            spec = normalize_strategy(spec)
            valid, reason = validate_strategy(spec)
            if not valid:
                raise ValueError(f"Invalid: {reason}")

            await self.db_client.log(
                agent_id=self.agent_id,
                level="INFO",
                message=f"Generated: {spec['strategy_name']}",
                metadata={
                    "mode": self.mode,
                    "tokens": max_tokens,
                    "archetype": archetype,
                    "reasoning": spec.get("reasoning", "")[:200],
                    "entry": spec.get("entry_conditions"),
                },
            )
            return spec, user_prompt, raw

        except Exception as e:
            logger.warning(f"{self.name}: Claude failed: {e}")
            return await self._generate_local(ctx)

    # ─────────────────────────────────────────────────────────
    # LOCAL FALLBACK — proven templates, always generates trades
    # ─────────────────────────────────────────────────────────
    async def _generate_local(self, ctx: dict) -> tuple[dict, None, None]:
        asset = ctx["asset_class"]
        archetype = ctx["archetype"]
        regime = ctx.get("regime", "neutral")
        key = (asset, archetype)

        variants = DIVERSE_TEMPLATES.get(key, DIVERSE_TEMPLATES[("equity", "momentum")])

        # Pattern-informed template selection: prefer winning feature families
        pat = ctx.get("pattern_intelligence", "")
        if "[WIN]" in pat and archetype in pat:
            # Use first variant (optimized path) when this archetype is winning
            entry, exit_ = variants[0]
        elif "[AVOID]" in pat and archetype in pat:
            # Use last variant (less aggressive) when this archetype is losing
            entry, exit_ = variants[-1]
        else:
            entry, exit_ = random.choice(variants)

        # Regime-aware threshold adjustments
        if regime in ("oversold", "oversold/bearish"):
            entry = [c for c in entry if "bollinger_band_position >" not in c]
            exit_ = [c for c in exit_ if "bollinger_band_position <" not in c]
        elif regime in ("overbought", "overbought/bullish"):
            entry = [
                c.replace("rsi_14 > 68", "rsi_14 > 72")
                .replace("rsi_14 > 70", "rsi_14 > 75")
                .replace("rsi_14 > 72", "rsi_14 > 78")
                for c in entry
            ]

        # Regime-aware valid_regimes per archetype
        _REGIME_MAP = {
            "momentum": ["bullish", "trending", "high_vol"],
            "mean_reversion": ["oversold", "ranging", "normal_vol"],
            "breakout": ["high_vol", "bullish", "trending"],
            "trend_following": ["bullish", "bearish", "trending"],
            "volatility_regime": ["high_vol", "low_vol"],
        }
        valid_regimes = _REGIME_MAP.get(archetype, [])

        suffix = datetime.utcnow().strftime("%H%M%S")
        spec = {
            "strategy_name": f"{archetype}_{asset}_tmpl_{suffix}",
            "hypothesis": f"Template {archetype} on {asset} 1m",
            "reasoning": "Proven template — guaranteed signal generation",
            "entry_conditions": entry,
            "exit_conditions": exit_,
            "valid_regimes": valid_regimes,
            "stop_loss": "0.5% below entry",
            "take_profit": "1.0% above entry",
            "position_sizing": "10% of portfolio",
            "timeframe": "1m",
            "asset_class": asset,
            "expected_sharpe": 1.0,
            "expected_win_rate": 0.50,
            "risk_level": "medium",
            "tags": [archetype, asset, "template"],
        }
        return normalize_strategy(spec), None, None


# ─────────────────────────────────────────────────────────────
# MAIN — 2 rich + 2 lean + 1 local
# ─────────────────────────────────────────────────────────────
async def main():
    print("=== IDEATOR V2 MAIN ===", flush=True)
    db = TimescaleClient(settings.database_url)
    await db.connect()
    redis = Redis.from_url(settings.redis_url)

    agents = [
        # 2 rich-context Claude agents (1500 tokens)
        IdeatorAgentV2(0, 0.5, redis, db, mode="rich"),
        IdeatorAgentV2(1, 0.7, redis, db, mode="rich"),
        # 2 lean Claude agents (800 tokens, faster)
        IdeatorAgentV2(2, 0.4, redis, db, mode="lean"),
        IdeatorAgentV2(3, 0.85, redis, db, mode="lean"),
        # 1 local template agent (no API cost, always works)
        IdeatorAgentV2(4, 0.0, redis, db, mode="local"),
    ]

    for a in agents:
        await a.start()
    # Keep alive until all agent main tasks complete
    main_tasks = [a._main_task for a in agents if a._main_task]
    if main_tasks:
        await asyncio.gather(*main_tasks, return_exceptions=True)


if __name__ == "__main__":
    print("=== IDEATOR V2 HIT ===", flush=True)
    asyncio.run(main())
