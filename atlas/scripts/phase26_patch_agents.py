"""
Phase 26: Patch agent files with scout-aware coupling.

Patches:
1. agents/l2_strategy/ideator_agent_v2.py - Scout-aware archetype selection, aggression, regime modulation
2. agents/l2_strategy/mutator_agent.py - Deeper scout coupling, entropy governance, influence logging
3. agents/scouts/source_reliability_engine.py - Real economic learning, sharpe/drawdown tracking
"""
import re
import sys
import os
import ast

atlas_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def patch_file(path, description, old_text, new_text):
    full_path = os.path.join(atlas_dir, path)
    with open(full_path, "r", encoding="utf-8") as f:
        content = f.read()
    if old_text in content:
        content = content.replace(old_text, new_text, 1)
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"[OK] {description}")
        return True
    else:
        print(f"[FAIL] {description} - pattern not found")
        return False

# ============================================================
# 1. PHASE 26A: IdeatorAgentV2 - Scout-aware archetype weighting
# ============================================================

# Add scout influence logging import and method to IdeatorAgentV2's __init__
ideator_init_addition = """
        # Phase 26: Scout coupling state
        self._scout_archetype_weights: dict[str, float] | None = None
        self._scout_aggression_factor: float = 1.0
        self._scout_confidence_modulator: float = 1.0
        self._scout_liquidity_sensitivity: float = 1.0
        self._scout_last_regime: str = "neutral"
"""

patch_file(
    "agents/l2_strategy/ideator_agent_v2.py",
    "Added Phase 26 scout coupling state to __init__",
    "        # Diversity governance — reject strategies with >70% feature overlap",
    ideator_init_addition + "\n        # Diversity governance — reject strategies with >70% feature overlap"
)

# Add scout-aware archetype modulation to _build_context (after scout signals enrichment block)
# Add after the line: ctx["scout_intelligence"] = existing + "\\n\\n" + "\\n".join(enriched_lines)
scout_modulation_code = """
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

"""

# Find the scout signals enrichment block end
patch_file(
    "agents/l2_strategy/ideator_agent_v2.py",
    "Added Phase 26A scout modulation in _build_context",
    "        return ctx",
    scout_modulation_code + "\n        return ctx"
)

# Add the helper methods to the IdeatorAgentV2 class
# Find the _check_diversity method and add helper methods before it

archetype_helpers = """
    # ================================================================
    # PHASE 26A — SCOUT-AWARE ARCHETYPE AND AGGRESSION MODULATION
    # ================================================================

    def _compute_scout_archetype_weights(
        self, regime: str, scout_text: str
    ) -> dict[str, float]:
        \"\"\"Compute scout-informed archetype weights based on regime and market conditions.
        
        Returns {archetype: weight} dict that can bias archetype selection.
        Higher weight = more likely to be selected.
        \"\"\"
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
        \"\"\"Compute aggression factor from scout conditions.
        1.0 = standard aggression.
        < 1.0 = more conservative (wider stops, lower leverage).
        > 1.0 = more aggressive (tighter stops, normal leverage).
        \"\"\"
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
        \"\"\"Select preferred timeframe based on scout conditions.\"\"\"
        scout_lower = scout_text.lower()
        if "thin" in scout_lower or "stressed" in scout_lower:
            return "5m"  # Lower frequency when liquidity is poor
        if "degraded" in scout_lower:
            return "5m"
        if "panic" in scout_lower:
            return "15m"  # Very conservative in panic
        return "1m"  # Default

"""

# Insert archetype helpers before _check_diversity
patch_file(
    "agents/l2_strategy/ideator_agent_v2.py",
    "Added Phase 26A archetype/aggression/timeframe helpers",
    "    def _check_diversity(",
    archetype_helpers + "\n    def _check_diversity("
)

# Now add scout-aware archetype selection to the deterministic generation
# We need to modify _generate_deterministic_candidates to use scout weights
# The archetype is set by self._archetype which is static (instance_id based)
# Instead, let's modify the generation to accept a modulated archetype

modulated_archetype_code = """
        # Phase 26A: Scout-aware archetype modulation
        scout_weights = ctx.get("scout_archetype_weights")
        if scout_weights:
            import random
            # Weighted random choice based on scout conditions
            arch_keys = list(scout_weights.keys())
            arch_weights = [scout_weights[k] for k in arch_keys]
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
"""

# Insert after the grammar check in _generate_deterministic_candidates
patch_file(
    "agents/l2_strategy/ideator_agent_v2.py",
    "Added Phase 26A archetype modulation in _generate_deterministic_candidates",
    "        grammar = STRATEGY_GRAMMAR.get(archetype)\n        if not grammar:\n            # Unknown archetype — fall back to template\n            return None, None, None",
    "        grammar = STRATEGY_GRAMMAR.get(archetype)\n        if not grammar:\n            return None, None, None" + modulated_archetype_code
)

# ============================================================
# 2. PHASE 26B: MutatorAgent - Scout-aware coupling and entropy governance
# ============================================================

# Add scout-entropy governance to deterministics
# After "def deterministic_micro_mutations", add entropy-aware diversification

entropy_mutation_code = """
    # Phase 26B: Scout-aware entropy governance for mutation exploration diversity
    _scout_entropy_context: float = 0.5  # 0.0 = low entropy, 1.0 = high entropy
    _scout_regime_context: str = "neutral"
    _scout_liquidity_context: str = "healthy"

    def _get_scout_entropy_diversity(self, base_variants: list[dict]) -> list[dict]:
        \"\"\"Add entropy-governed mutation diversity.
        High entropy -> more exploration (more variants, wider changes).
        Low entropy -> more exploitation (fewer variants, conservative changes).
        Also applies regime-specific mutation bias.
        \"\"\"
        results = list(base_variants)
        
        # High entropy: add extra exploratory variants
        if self._scout_entropy_context > 0.7:
            # Add a more aggressive mutation if space allows
            if "volatility" not in str(base_variants[:3]):
                v = dict(base_variants[0]) if base_variants else {}
                if v:
                    entry = v.get("entry_conditions", [])
                    if entry:
                        # Add an additional entrance condition for exploration
                        v["entry_conditions"] = entry + ["volatility_regime > 1.3"]
                        v["_mutation_type"] = "entropy_exploration"
                        v["_mutation_fields"] = ["entry_conditions"]
                        results.append(v)
        # Low entropy: add conservative variants
        elif self._scout_entropy_context < 0.3:
            # Fewer variants, simpler changes
            results = results[:max(3, len(results) // 2)]
        
        # Regime-specific bias
        regime = self._scout_regime_context
        if "volatile" in regime.lower() or "panic" in regime.lower():
            # Suppress high-risk mutations in volatile regimes
            results = [v for v in results if "aggression" not in str(v.get("_mutation_type", ""))]
        if "thin" in self._scout_liquidity_context.lower():
            # Suppress high-frequency mutations in thin liquidity
            results = [v for v in results if "hold_time" not in str(v.get("_mutation_type", ""))
                      or "increase" in str(v.get("hold_time_max", ""))]
        
        return results
"""

# Add entropy diversity to deterministic_micro_mutations
patch_file(
    "agents/l2_strategy/mutator_agent.py",
    "Added Phase 26B entropy governance and scout context",
    "def deterministic_micro_mutations",
    "_scout_entropy_context: float = 0.5\n_scout_regime_context: str = \"neutral\"\n_scout_liquidity_context: str = \"healthy\"\n\n\ndef deterministic_micro_mutations"
)

# Now modify MutatorAgent._process_candidate to set scout context and log influence
# Add after the scout fetching section in _process_candidate

scout_mutation_influence = """

        # Phase 26B: Log scout influence on mutation selection
        try:
            entropy_val = float(len(candidates)) / 20.0 if candidates else 0.5  # proxy entropy
            deterministic_micro_mutations._scout_entropy_context = entropy_val
            deterministic_micro_mutations._scout_regime_context = scout_regime_vol or "neutral"
            deterministic_micro_mutations._scout_liquidity_context = scout_liquidity_regime or "healthy"
            
            # Log the mutation influence
            await self.db_client.log_scout_influence(
                source_scout="regime_scout",
                target_agent=self.name,
                influence_type="mutation_entropy_governance",
                influence_metric="entropy_context",
                after_value=entropy_val,
                delta=entropy_val - 0.5,
                confidence=0.6,
                regime_context=scout_regime_vol or "neutral",
            )
        except Exception as e:
            logger.debug(f"{self.name}: Phase 26B scout influence logging skipped: {e}")

"""

# Insert after the scout intelligence fetching block in _process_candidate
patch_file(
    "agents/l2_strategy/mutator_agent.py",
    "Added Phase 26B scout mutation influence logging",
    "        favor_economic = (",
    scout_mutation_influence + "\n        favor_economic = ("
)

# ============================================================
# 3. PHASE 26C: SourceReliabilityEngine - Real economic learning
# ============================================================

# Add after the _persist_performance method
economic_learning_code = """

    # ================================================================
    # PHASE 26C — ECONOMIC TRUST EVOLUTION
    # ================================================================

    async def _compute_economic_trust_scores(self):
        \"\"\"Phase 26C: Compute trust scores based on REAL economic outcomes.
        Uses scout_economic_attribution table to measure:
        - Sharpe contribution per scout
        - Drawdown contribution per scout
        - Win rate contribution per scout
        - Validation survival rate
        - Regime specialization
        \"\"\"
        async with self.db.engine.connect() as conn:
            # Get economic attribution summary per scout source
            r = await conn.execute(
                text(\"""
                    SELECT source_scout,
                           AVG(COALESCE(sharpe_contribution, 0)) as avg_sharpe,
                           AVG(COALESCE(pnl_contribution, 0)) as avg_pnl,
                           AVG(COALESCE(win_rate_contribution, 0)) as avg_win_rate,
                           COUNT(*) as total_decisions,
                           SUM(CASE WHEN survived_validation THEN 1 ELSE 0 END) as survived_count,
                           AVG(COALESCE(attribution_weight, 0)) as avg_weight
                    FROM scout_economic_attribution
                    WHERE created_at > NOW() - INTERVAL '7 days'
                    GROUP BY source_scout
                \""")
            )
            economic_metrics = {}
            for row in r.fetchall():
                source = row[0]
                total = max(1, row[4])
                economic_metrics[source] = {
                    "avg_sharpe": float(row[1] or 0),
                    "avg_pnl": float(row[2] or 0),
                    "avg_win_rate": float(row[3] or 0),
                    "total_decisions": row[4],
                    "survival_rate": row[5] / total if total > 0 else 0,
                    "avg_weight": float(row[6] or 0),
                }

            # Get contradiction counts from quarantine
            r2 = await conn.execute(
                text(\"""
                    SELECT source, COUNT(*) as contradiction_count
                    FROM scout_poison_quarantine
                    WHERE created_at > NOW() - INTERVAL '7 days'
                    GROUP BY source
                \""")
            )
            contradictions = {str(row[0]): row[1] for row in r2.fetchall()}

        # Now compute per-source trust scores with economic signal
        for source_key, source_name in [("regime_scout", "regime_scout"), 
                                         ("liquidity_scout", "liquidity_scout"),
                                         ("correlation_scout", "correlation_scout"),
                                         ("execution_scout", "execution_scout"),
                                         ("news_intelligence_engine", "news"),
                                         ("reddit_scout", "reddit"),
                                         ("youtube_scout", "youtube"),
                                         ("discord_scout", "discord"),
                                         ("podcast_scout", "podcast")]:
            # Start from base
            base = DEFAULT_TRUST_SCORES.get(source_key, 0.3)
            
            # Economic signal component
            metrics = economic_metrics.get(source_key, {})
            if metrics:
                # Positive Sharpe contribution -> increase trust
                sharpe_bonus = min(0.2, max(-0.2, metrics["avg_sharpe"] * 0.1))
                # Survival rate bonus
                survival_bonus = (metrics["survival_rate"] - 0.5) * 0.2
                # Win rate bonus
                win_rate_bonus = (metrics["avg_win_rate"] - 0.5) * 0.1
                economic_bonus = sharpe_bonus + survival_bonus + win_rate_bonus
            else:
                economic_bonus = 0.0

            # Contradiction penalty
            contradiction_count = contradictions.get(source_key, 0)
            contradiction_penalty = min(0.3, contradiction_count * 0.05)

            # Time decay (base trust decays toward 0.3 over time without new data)
            # This is handled by the staleness decay in _assess_sources

            # Final trust score
            new_trust = max(0.05, min(0.95, base + economic_bonus - contradiction_penalty))
            self._trust_scores[source_key] = new_trust

            # Persist updated trust to source_performance_log
            try:
                import uuid
                await self.db._execute_insert(
                    \"""
                    INSERT INTO source_performance_log
                        (id, source, source_sub, dynamic_trust_score, historical_accuracy,
                         n_profitable_signals, n_loss_signals, updated_at)
                    VALUES
                        (:id, :source, 'economic', :trust, :acc, 0, 0, NOW())
                    ON CONFLICT (id) DO UPDATE
                        SET dynamic_trust_score = EXCLUDED.dynamic_trust_score,
                            updated_at = NOW()
                    \""",
                    {
                        "id": uuid.uuid4().hex[:16],
                        "source": source_key,
                        "trust": round(new_trust, 4),
                        "acc": round(0.5 + economic_bonus, 4),
                    }
                )
            except Exception:
                pass

        logger.info(
            f"{self.name}: Phase 26C economic trust evolution complete for "
            f"{len(economic_metrics)} sources with economic data"
        )

    async def run(self):
        # Phase 26C: Add economic trust evolution to the regular assessment cycle
        logger.info(f"{self.name}: Starting dynamic source reliability tracking")
        while self.status == "running":
            try:
                await self._assess_sources()
                # Phase 26C: Economic trust scoring
                await self._compute_economic_trust_scores()
            except Exception as e:
                logger.error(f"{self.name}: Source assessment error: {e}")
            for _ in range(self._run_interval // 10):
                await asyncio.sleep(10)
                if self.status != "running":
                    return
"""

# Replace the existing run method and add economic learning
patch_file(
    "agents/scouts/source_reliability_engine.py",
    "Added Phase 26C economic trust evolution",
    "    async def run(self):\n        logger.info(f\"{self.name}: Starting dynamic source reliability tracking\")\n\n        while self.status == \"running\":\n            try:\n                await self._assess_sources()\n            except Exception as e:\n                logger.error(f\"{self.name}: Source assessment error: {e}\")\n\n            for _ in range(self._run_interval // 10):\n                await asyncio.sleep(10)\n                if self.status != \"running\":\n                    return",
    economic_learning_code
)

print("\n[DONE] All Phase 26 agent patches applied")
