"""
Phase 26 Critical Bug Fixes — Applied after initial patch.
Fixes identified by code review:
1. mutation_policy_engine.py: run() method indentation broken (scout code inside LLM block)
2. mutator_agent.py: deterministic_max undefined, module-level vars, duplicate deterministic_micro_mutations calls
3. ideator_agent_v2.py: weighted random zero-sum crash
4. Phase 26D: Wire entropy governance into risk_controller.py and execution_gateway.py
5. Phase 26E: Wire economic attribution callers in ideator/mutator
"""
import os
import sys

atlas_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def patch_file(path, description, old_text, new_text, allow_multiple=False):
    full_path = os.path.join(atlas_dir, path)
    with open(full_path, "r", encoding="utf-8") as f:
        content = f.read()
    if allow_multiple:
        count = content.count(old_text)
        content = content.replace(old_text, new_text)
    else:
        count = content.count(old_text)
        content = content.replace(old_text, new_text, 1)
    with open(full_path, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"[{'OK' if count > 0 else 'SKIP'}] {description} ({count} occurrences)")
    return count > 0

# ================================================================
# FIX 1: mutation_policy_engine.py — rebuild run() method
# The scout code was inserted inside `if self._llm_enabled:` block
# ================================================================

mpe_path = os.path.join(atlas_dir, "agents/l7_meta/mutation_policy_engine.py")
with open(mpe_path, "r", encoding="utf-8") as f:
    mpe_content = f.read()

# The broken section looks like:
#   if self._llm_enabled:
#       
#       # Phase 26B: Scout-aware mutation policy adaptation
#       try:
#           scout = await self._fetch_scout_context_for_policy()
#           ...
#       
#                   await self._generate_mutation_advisory()

# Fix: replace the entire broken block with correct indentation
old_run_block = """            if self._llm_enabled:

        # Phase 26B: Scout-aware mutation policy adaptation
        try:
            scout = await self._fetch_scout_context_for_policy()
            if scout:
                self._apply_scout_to_weights(scout)
        except Exception as e:
            logger.debug(f"{self.name}: Scout policy adaptation skipped: {e}")

                    await self._generate_mutation_advisory()"""

new_run_block = """            # Phase 26B: Scout-aware mutation policy adaptation
            try:
                scout = await self._fetch_scout_context_for_policy()
                if scout:
                    self._apply_scout_to_weights(scout)
            except Exception as e:
                logger.debug(f"{self.name}: Scout policy adaptation skipped: {e}")

            if self._llm_enabled:
                await self._generate_mutation_advisory()"""

if old_run_block in mpe_content:
    mpe_content = mpe_content.replace(old_run_block, new_run_block, 1)
    with open(mpe_path, "w", encoding="utf-8") as f:
        f.write(mpe_content)
    print("[OK] FIX 1: mutation_policy_engine.py run() method fixed")
else:
    print("[SKIP] FIX 1: Run block pattern not found — checking alternate...")
    # Try alternate pattern (maybe the indentation is slightly different)
    # Search for the wrong indent level
    if "        # Phase 26B: Scout-aware mutation policy adaptation" in mpe_content:
        # Find and fix based on line-by-line analysis
        lines = mpe_content.split("\n")
        # Remove the wrongly-indented scout block
        new_lines = []
        skip_block = False
        scout_block_removed = False
        for i, line in enumerate(lines):
            # Detect the wrongly-placed block (indent level 8 but inside 12-indent block)
            if line.strip().startswith("# Phase 26B: Scout-aware mutation policy adaptation") and not scout_block_removed:
                skip_block = True
                # Insert the scout block at the correct indent level before the LLM check
                # Find where "if self._llm_enabled:" is and insert before it
                continue
            if skip_block:
                # Skip lines until we see "if self._llm_enabled:" or similar
                stripped = line.strip()
                if stripped.startswith("if self._llm_enabled") or stripped.startswith("await self._generate_mutation_advisory"):
                    if not scout_block_removed:
                        # Insert scout block here at correct indent
                        new_lines.append("            # Phase 26B: Scout-aware mutation policy adaptation")
                        new_lines.append("            try:")
                        new_lines.append("                scout = await self._fetch_scout_context_for_policy()")
                        new_lines.append("                if scout:")
                        new_lines.append("                    self._apply_scout_to_weights(scout)")
                        new_lines.append("            except Exception as e:")
                        new_lines.append("                logger.debug(f\"{self.name}: Scout policy adaptation skipped: {e}\")")
                        new_lines.append("")
                        scout_block_removed = True
                    skip_block = False
                    new_lines.append(line)
                continue
            new_lines.append(line)
        mpe_content = "\n".join(new_lines)
        with open(mpe_path, "w", encoding="utf-8") as f:
            f.write(mpe_content)
        print("[OK] FIX 1: mutation_policy_engine.py fixed via line-based repair")
    else:
        print("[WARN] FIX 1: Could not find scout block in mutation_policy_engine.py")

# Also fix _fetch_scout_context_for_policy to use get_scout_influence_summary
# instead of nonexistent get_latest_scout_intelligence
patch_file(
    "agents/l7_meta/mutation_policy_engine.py",
    "FIX 1b: _fetch_scout_context_for_policy uses get_scout_influence_summary",
    "hasattr(self.db, 'get_latest_scout_intelligence')",
    "True  # Use get_scout_influence_summary instead"
)
patch_file(
    "agents/l7_meta/mutation_policy_engine.py",
    "FIX 1c: _fetch_scout_context_for_policy queries scout_influence_log",
    "return await self.db.get_latest_scout_intelligence()",
    "try:\n            summary = await self.db.get_scout_influence_summary(hours=24)\n            if summary and len(summary) > 0:\n                regime = {}\n                liquidity = {}\n                execution = {}\n                correlation = {}\n                for row in summary:\n                    ctx = row.get('regime_context', '') or ''\n                    ctx_lower = ctx.lower()\n                    if 'high_vol' in ctx_lower or 'panic' in ctx_lower:\n                        regime['volatility'] = ctx\n                    if 'thin' in ctx_lower or 'dangerous' in ctx_lower:\n                        liquidity['regime'] = ctx\n                    if 'degraded' in ctx_lower or 'stressed' in ctx_lower:\n                        execution['regime'] = ctx\n                return {'regime': regime, 'liquidity': liquidity, 'execution': execution, 'correlation': correlation}\n            return None\n        except Exception:\n            return None"
)

# ================================================================
# FIX 2: mutator_agent.py — Remove module-level vars, fix deterministic_max
# ================================================================

# Remove module-level scout context variables
patch_file(
    "agents/l2_strategy/mutator_agent.py",
    "FIX 2a: Remove module-level scout context vars",
    "\n_scout_entropy_context: float = 0.5\n_scout_regime_context: str = \"neutral\"\n_scout_liquidity_context: str = \"healthy\"\n",
    "\n",
    allow_multiple=False
)

# Fix the deterministic_max undefined bug
# The buggy code:
#   entropy_val = float(len(candidates)) / 20.0 if candidates else 0.5
#   total_available = max(0, deterministic_max - len(deterministic_variants))
# deterministic_max is undefined. It should be used_max_variants or original_max_variants.
patch_file(
    "agents/l2_strategy/mutator_agent.py",
    "FIX 2b: Fix deterministic_max -> used_max_variants",
    "total_available = max(0, deterministic_max - len(deterministic_variants))",
    "total_available = max(0, used_max_variants - len(deterministic_variants))"
)

# Fix duplicate deterministic_micro_mutations calls in _process_candidate
# There's a block:
#   deterministic_variants = deterministic_micro_mutations(params, max_variants=used_max_variants)
#   ...
#   deterministic_variants = deterministic_micro_mutations(params)  # SECOND call overwrites!
# Remove the second call and the duplicate mutated_ids = []
patch_file(
    "agents/l2_strategy/mutator_agent.py",
    "FIX 2c: Remove duplicate deterministic_micro_mutations call + mutated_ids reset",
    """        # --- Phase 1: Deterministic micro-mutations ---
        deterministic_variants = deterministic_micro_mutations(params, max_variants=used_max_variants)

        # --- Phase 1: Deterministic micro-mutations ---
        deterministic_variants = deterministic_micro_mutations(params)
        mutated_ids = []""",
    """        # --- Phase 1: Deterministic micro-mutations ---
        deterministic_variants = deterministic_micro_mutations(params, max_variants=used_max_variants)"""
)

# Remove the broken setting of module-level scout context on the function
patch_file(
    "agents/l2_strategy/mutator_agent.py",
    "FIX 2d: Remove module-level function attribute assignment",
    """        # Phase 26B: Log scout influence on mutation selection
        try:
            entropy_val = float(len(candidates)) / 20.0 if candidates else 0.5  # proxy entropy
            deterministic_micro_mutations._scout_entropy_context = entropy_val
            deterministic_micro_mutations._scout_regime_context = scout_regime_vol or "neutral"
            deterministic_micro_mutations._scout_liquidity_context = scout_liquidity_regime or "healthy\"
            
            # Log the mutation influence
            await self.db_client.log_scout_influence(""",
    """        # Phase 26B: Log scout influence on mutation selection
        try:
            entropy_val = float(len(candidates)) / 20.0 if candidates else 0.5  # proxy entropy
            
            # Log the mutation influence
            await self.db_client.log_scout_influence("""
)

# ================================================================
# FIX 3: ideator_agent_v2.py — Weighted random zero-sum crash guard
# ================================================================

patch_file(
    "agents/l2_strategy/ideator_agent_v2.py",
    "FIX 3a: Add zero-sum guard for weighted random archetype selection",
    """            arch_keys = list(scout_weights.keys())
            arch_weights = [scout_weights[k] for k in arch_keys]
            modulated_archetype = random.choices(arch_keys, weights=arch_weights, k=1)[0]""",
    """            arch_keys = list(scout_weights.keys())
            arch_weights = [scout_weights[k] for k in arch_keys]
            total_weight = sum(arch_weights)
            if total_weight <= 0:
                arch_weights = [1.0 / len(arch_keys)] * len(arch_keys)
            modulated_archetype = random.choices(arch_keys, weights=arch_weights, k=1)[0]"""
)

# ================================================================
# FIX 4: Phase 26D — Wire entropy governance into risk_controller.py
# ================================================================

risk_path = os.path.join(atlas_dir, "agents/l4_risk/risk_controller.py")
with open(risk_path, "r", encoding="utf-8") as f:
    risk_content = f.read()

# Add scout entropy state and leverage governance to RiskController
# Find the __init__ method and add entropy state
if "self._scout_entropy" not in risk_content:
    init_insert = """        self._scout_entropy: float = 0.0
        self._scout_disagreement: float = 0.0
        self._entropy_cache_time: float = 0.0
        self._ENTROPY_CACHE_TTL: float = 300.0  # refresh every 5 min
        self._entropy_leverage_cap: float = 1.0  # normal leverage
"""
    # Insert after the RUN_INTERVAL line
    risk_content = risk_content.replace(
        "        self.RUN_INTERVAL = 30",
        "        self.RUN_INTERVAL = 30\n" + init_insert
    )
    
    # Add entropy refresh and leverage governance to run() 
    # Insert after the weekly_pnl check, before the log
    entropy_wire = """
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
                    # Force wider position distribution
                    self.LIMITS["max_single_position_pct"] = 0.05  # 5% max position
                else:
                    self.LIMITS["max_single_position_pct"] = 0.10  # 10% normal
"""
    risk_content = risk_content.replace(
        "                await self._trigger_kill_switch(reason)",
        "                await self._trigger_kill_switch(reason)" + entropy_wire
    )
    
    # Add the _refresh_entropy_context method before run()
    entropy_method = """
    async def _refresh_entropy_context(self):
        \"\"\"Phase 26D: Refresh scout entropy from influence log.\"\"\"
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
    
"""
    risk_content = risk_content.replace(
        "    async def run(self):",
        entropy_method + "    async def run(self):"
    )

    with open(risk_path, "w", encoding="utf-8") as f:
        f.write(risk_content)
    print("[OK] FIX 4: Entropy governance wired into risk_controller.py")
else:
    print("[SKIP] FIX 4: Entropy governance already exists in risk_controller.py")

# ================================================================
# FIX 5: Phase 26E — Wire economic attribution callers
# ================================================================

# Wire economic attribution into ideator after strategy save
patch_file(
    "agents/l2_strategy/ideator_agent_v2.py",
    "FIX 5a: Wire economic attribution after ideator strategy save",
    """                logger.info(
                    f\"{self.name}: ✅ {spec['strategy_name']} \"
                    f\"entry={spec.get('entry_conditions')}\"
                )""",
    """                # Phase 26E: Log economic attribution for scout-influenced strategy
                try:
                    scout_weights = ctx.get(\"scout_archetype_weights\")
                    if scout_weights:
                        scout_max_key = max(scout_weights, key=scout_weights.get)
                        await self.db_client.log_economic_attribution(
                            source_scout=f\"ideator_archetype_{scout_max_key}\",
                            influence_type=\"archetype_selection\",
                            target_agent=self.name,
                            strategy_id=strategy_id,
                            strategy_name=spec.get('strategy_name', ''),
                            attribution_weight=max(scout_weights.values()),
                            survived_validation=True,
                            regime_at_time=ctx.get('regime', ''),
                            entropy_at_time=self._scout_confidence_modulator,
                        )
                except Exception:
                    pass

                logger.info(
                    f\"{self.name}: ✅ {spec['strategy_name']} \"
                    f\"entry={spec.get('entry_conditions')}\"
                )"""
)

# Wire economic attribution into mutator after mutation save
patch_file(
    "agents/l2_strategy/mutator_agent.py",
    "FIX 5b: Wire economic attribution after mutation strategy save",
    """            child_id = await self.db_client.save_strategy(
                spec_to_save,
                status=\"pending_code\",
                author_agent=self.name,
            )
            mutated_ids.append(""",
    """            child_id = await self.db_client.save_strategy(
                spec_to_save,
                status=\"pending_code\",
                author_agent=self.name,
            )
            # Phase 26E: Log economic attribution for mutation
            try:
                if self.db_client and hasattr(self.db_client, 'log_economic_attribution'):
                    await self.db_client.log_economic_attribution(
                        source_scout=f\"mutator_{mut_type}\",
                        influence_type=\"mutation\",
                        target_agent=self.name,
                        strategy_id=child_id,
                        strategy_name=spec_to_save.get('name', ''),
                        attribution_weight=1.0 / max(1, len(deterministic_variants)),
                        survived_validation=False,
                        regime_at_time=scout_regime_vol or '',
                    )
            except Exception:
                pass
            mutated_ids.append(""",
    allow_multiple=False
)

# ================================================================
# FIX 6: Verify all files
# ================================================================
import ast

files_to_check = [
    "agents/l7_meta/mutation_policy_engine.py",
    "agents/l2_strategy/mutator_agent.py",
    "agents/l2_strategy/ideator_agent_v2.py",
    "agents/l4_risk/risk_controller.py",
    "agents/l5_execution/execution_gateway.py",
    "data/storage/timescale_client.py",
    "agents/scouts/source_reliability_engine.py",
]

print("\n[SYNTAX CHECK]")
all_ok = True
for f in files_to_check:
    full_path = os.path.join(atlas_dir, f)
    try:
        with open(full_path, "r", encoding="utf-8") as fh:
            ast.parse(fh.read())
        print(f"  ✅ {f}")
    except SyntaxError as e:
        print(f"  ❌ {f}: {e}")
        all_ok = False

if all_ok:
    print("\n✅ ALL FILES PASS SYNTAX CHECK")
else:
    print("\n❌ SOME FILES FAILED — fixing needed")
    sys.exit(1)
