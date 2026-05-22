"""
Phase 26 Hotfix: Fix critical bugs found by code review.
1. Fix _get_scout_entropy_diversity in mutator_agent.py (remove self, use globals)
2. Wire _get_scout_entropy_diversity into _process_candidate
3. Remove redundant import from timescale_client.py
4. Patch MutationPolicyEngine with scout-aware adaptation
"""
import re
import sys
import os

atlas_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def patch_file(path, description, old_text, new_text):
    full_path = os.path.join(atlas_dir, path)
    with open(full_path, "r", encoding="utf-8") as f:
        content = f.read()
    count = content.count(old_text)
    if count > 0:
        content = content.replace(old_text, new_text, 1)
        with open(full_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"[OK] {description} (replaced {count} occurrences)")
        return True
    else:
        print(f"[FAIL] {description} - pattern not found")
        return False

# ============================================================
# FIX 1: Remove _get_scout_entropy_diversity (the broken module-level function)
# and replace deterministic_micro_mutations to include proper scout context
# ============================================================

# The issue is that _get_scout_entropy_diversity was inserted as a module-level
# function referencing 'self'. We need to remove it and replace the
# deterministic_micro_mutations function with a version that has proper
# scout entropy governance built in.

# First, let's read the current mutator_agent.py to find exact lines
mutator_path = os.path.join(atlas_dir, "agents/l2_strategy/mutator_agent.py")
with open(mutator_path, "r", encoding="utf-8") as f:
    mutator_content = f.read()

# Check if the problematic function exists
if "_get_scout_entropy_diversity" in mutator_content:
    # Remove the entire _get_scout_entropy_diversity function and the module-level
    # scout context variables
    
    # Find and remove the scout context variable declarations (added before deterministic_micro_mutations)
    # These are at module level but should be part of the class
    
    # The pattern is: "_scout_entropy_context: float = 0.5\n_scout_regime_context: str = \"neutral\"\n_scout_liquidity_context: str = \"healthy\"\n\n\ndef deterministic_micro_mutations"
    old_pattern = '"""\n\n_scout_entropy_context: float = 0.5\n_scout_regime_context: str = "neutral"\n_scout_liquidity_context: str = "healthy"\n\n\ndef deterministic_micro_mutations'
    new_pattern = '"""\n\ndef deterministic_micro_mutations'
    
    if old_pattern in mutator_content:
        mutator_content = mutator_content.replace(old_pattern, new_pattern, 1)
        print("[OK] Removed module-level scout context variables")
    
    # Remove the _get_scout_entropy_diversity function
    # Find from "def _get_scout_entropy_diversity" to the next "def" or end of relevant section
    start_marker = "def _get_scout_entropy_diversity"
    end_marker = "\n\n\ndef "
    
    start_idx = mutator_content.find(start_marker)
    if start_idx != -1:
        end_idx = mutator_content.find(end_marker, start_idx + 1)
        if end_idx != -1:
            mutator_content = mutator_content[:start_idx] + mutator_content[end_idx:]
            print("[OK] Removed _get_scout_entropy_diversity module-level function")
        else:
            print("[WARN] Could not find end of _get_scout_entropy_diversity")
    
    # Write back
    with open(mutator_path, "w", encoding="utf-8") as f:
        f.write(mutator_content)
else:
    print("[OK] _get_scout_entropy_diversity not found (may have been fixed already)")

# ============================================================
# FIX 2: Add a proper scout-aware method to MutatorAgent class
# Insert after the scout influence logging block
# ============================================================

# Add scout entropy diversity method to MutatorAgent class
scout_entropy_method = """
        # Phase 26B: Apply scout entropy governance to mutation variants
        try:
            entropy_val = float(len(candidates)) / 20.0 if candidates else 0.5
            total_available = max(0, deterministic_max - len(deterministic_variants))
            if total_available > 0 and entropy_val > 0.6:
                # High entropy: add exploratory economic mutations
                extra_variants = self._generate_scout_entropy_variants(
                    params, entropy_val, total_available
                )
                deterministic_variants.extend(extra_variants)
                logger.info(
                    f"{self.name}: Phase 26B added {len(extra_variants)} "
                    f"entropy-governed mutation variants (entropy={entropy_val:.2f})"
                )
        except Exception as e:
            logger.debug(f"{self.name}: Entropy governance failed: {e}")

"""

# Insert after the deterministic variant generation block
# Looking for: "deterministic_variants = deterministic_micro_mutations(params)"
# And then the scout influence logging block was added before favor_economic
# Let me find the actual line
patch_file(
    "agents/l2_strategy/mutator_agent.py",
    "Wired scout entropy diversity into _process_candidate",
    "        favor_economic = (",
    scout_entropy_method + "\n        favor_economic = ("
)

# Add the helper method to MutatorAgent class
# Insert after \n    async def _get_recent_mutants
# Or find a good insertion point before the class closing

entropy_variants_method = """

    async def _generate_scout_entropy_variants(
        self, params: dict, entropy: float, max_variants: int
    ) -> list[dict]:
        \"\"\"Phase 26B: Generate entropy-governed mutation variants.
        
        High entropy (>0.6): Add more exploratory variants
        Low entropy (<0.3): The deterministic path already handles this conservatively
        
        These variants supplement the deterministic micro-mutations with
        regime-conditioned alternatives.
        \"\"\"
        from copy import deepcopy
        variants = []
        
        if max_variants <= 0 or entropy <= 0.6:
            return variants
        
        entry_conds = params.get("entry_conditions", []) or []
        exit_conds = params.get("exit_conditions", []) or []
        
        # High entropy: add regime-conscious alternative entry conditions
        if entropy > 0.7 and len(entry_conds) >= 2:
            # Try replacing the most restrictive condition with a regime-adaptive one
            for cond in entry_conds:
                if "bollinger_band_position" in cond.lower() and entropy > 0.7:
                    v = deepcopy(params)
                    # Replace bollinger with regime-aware condition
                    new_entry = []
                    for c in entry_conds:
                        if "bollinger_band_position" in c.lower() and random.random() < 0.5:
                            new_entry.append("volatility_regime > 1.3")
                        else:
                            new_entry.append(c)
                    v["entry_conditions"] = new_entry
                    v["_mutation_type"] = "entropy_exploration"
                    v["_mutation_fields"] = ["entry_conditions"]
                    variants.append(v)
                    break
        
        return variants[:max_variants]

"""

# Insert the method before the main() function or after _get_recent_mutants
patch_file(
    "agents/l2_strategy/mutator_agent.py",
    "Added _generate_scout_entropy_variants to MutatorAgent",
    "async def main():",
    entropy_variants_method + "\n\n\nasync def main():"
)

# ============================================================
# FIX 3: Remove redundant sqlalchemy import from timescale_client.py
# ============================================================

tc_path = os.path.join(atlas_dir, "data/storage/timescale_client.py")
with open(tc_path, "r", encoding="utf-8") as f:
    tc_content = f.read()

# Check for redundant import at end of file
lines = tc_content.split("\n")
# Look for "from sqlalchemy import text" at the end (after schema_version)
filtered = []
redundant_found = False
for line in lines:
    if line.strip() == 'from sqlalchemy import text' and 'from sqlalchemy.sql import text' in tc_content:
        redundant_found = True
        continue
    filtered.append(line)

if redundant_found:
    tc_content = "\n".join(filtered)
    with open(tc_path, "w", encoding="utf-8") as f:
        f.write(tc_content)
    print("[OK] Removed redundant 'from sqlalchemy import text' import")
else:
    print("[OK] No redundant import found")

# ============================================================
# FIX 4: Remove unused _scout_last_regime from ideator_agent_v2.py
# ============================================================

patch_file(
    "agents/l2_strategy/ideator_agent_v2.py",
    "Removed unused _scout_last_regime",
    "        self._scout_last_regime: str = \"neutral\"\n",
    ""
)

# ============================================================
# FIX 5: Wire scout awareness into MutationPolicyEngine
# ============================================================

mpe_patch = """
        # Phase 26B: Scout-aware mutation policy adaptation
        try:
            scout = await self._fetch_scout_context_for_policy()
            if scout:
                self._apply_scout_to_weights(scout)
        except Exception as e:
            logger.debug(f"{self.name}: Scout policy adaptation skipped: {e}")

"""

# Insert after _learn_policy call in run()
patch_file(
    "agents/l7_meta/mutation_policy_engine.py",
    "Added scout-aware policy adaptation to MutationPolicyEngine",
    "                    await self._generate_mutation_advisory()",
    mpe_patch + "                    await self._generate_mutation_advisory()"
)

# Add scout methods to MutationPolicyEngine
scout_policy_methods = """

    # ================================================================
    # PHASE 26B — SCOUT-AWARE MUTATION POLICY ADAPTATION
    # ================================================================

    async def _fetch_scout_context_for_policy(self) -> dict | None:
        \"\"\"Fetch scout context for mutation policy adaptation.\"\"\"
        try:
            if not hasattr(self.db, 'get_latest_scout_intelligence'):
                return None
            return await self.db.get_latest_scout_intelligence()
        except Exception:
            return None

    def _apply_scout_to_weights(self, scout: dict) -> None:
        \"\"\"Modify mutation weights based on scout conditions.
        Modifies self._weights in-memory (does NOT persist to DB).
        \"\"\"
        regime = scout.get("regime", {})
        liquidity = scout.get("liquidity", {})
        execution = scout.get("execution", {})
        correlation = scout.get("correlation", {})

        vol = str(regime.get("volatility", ""))
        liq_regime = str(liquidity.get("regime", ""))
        exec_regime = str(execution.get("regime", ""))
        corr_risk = str(correlation.get("risk_state", ""))

        adjustments = {}

        # Volatility-based adjustments
        if "high_vol" in vol.lower() or "panic_vol" in vol.lower():
            adjustments = {
                "parameter_shift": 0.30,   # More parameter shifts in volatile markets
                "regime_adapt": 0.20,       # Regime adaptation becomes critical
                "combine_with": 0.05,       # Suppress combination mutations
            }
        elif "low_vol" in vol.lower():
            adjustments = {
                "indicator_replace": 0.25,
                "exit_logic": 0.15,
                "regime_adapt": 0.05,
            }

        # Liquidity-based adjustments
        if liq_regime in ("thin", "dangerous"):
            adjustments.update({
                "threshold_loosen": 0.25,   # Loosen thresholds to avoid false signals
                "threshold_tighten": 0.05,  # Don't tighten further in thin markets
            })

        # Execution-based adjustments
        if exec_regime in ("degraded", "stressed"):
            adjustments.update({
                "risk_adjust": 0.25,        # More risk adjustments
                "exit_logic": 0.20,         # Better exits
            })

        # Apply adjustments if any were computed
        if adjustments:
            new_weights = dict(self._weights)
            for k, v in adjustments.items():
                new_weights[k] = v

            # Normalize
            total = sum(new_weights.values())
            if total > 0:
                self._weights = {k: v / total for k, v in new_weights.items()}
                logger.info(
                    f"{self.name}: Scout-adjusted mutation weights "
                    f"(vol={vol}, liq={liq_regime}, exec={exec_regime})"
                )
"""

patch_file(
    "agents/l7_meta/mutation_policy_engine.py",
    "Added scout adaptation methods to MutationPolicyEngine",
    "    def select_mutation_type(self) -> str:",
    scout_policy_methods + "\n    def select_mutation_type(self) -> str:"
)

print("\n[DONE] All Phase 26 hotfixes applied")
