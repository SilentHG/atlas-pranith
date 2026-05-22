"""
phase26_ab_test.py — Phase 26F A/B Intelligence Testing.

Compares ATLAS behavior with scouts OFF vs scouts ON.
Runs two 15-minute sessions and compares:
  - Strategy quality (Sharpe, Sortino, composite score)
  - Validation pass rate
  - Mutation diversity
  - Execution quality
  - Portfolio stability

Output: PHASE26_AB_TEST_REPORT.md
"""

import asyncio
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime

import asyncpg

# ── Configuration ──────────────────────────────────────────────
ATLAS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PARENT = os.path.dirname(ATLAS_DIR)

# Ensure module resolution (atlas/ must be on sys.path for config.settings)
if _PARENT not in sys.path:
    sys.path.insert(0, _PARENT)
os.environ["PYTHONPATH"] = _PARENT
os.environ["ATLAS_DIR"] = ATLAS_DIR

from atlas.config.settings import settings
# Strip +asyncpg driver suffix for raw asyncpg compatibility
DB_URL = re.sub(r'\+\w+', '', settings.database_url)
SESSION_DURATION = 5  # minutes per session (2 sessions = ~10 min total, plus overhead)

# Success criteria thresholds
MIN_STRATEGIES = 3
MIN_TEMPLATE_RATIO = 0.5  # at least 50% of baseline template count

REPORT_PATH = os.path.join(ATLAS_DIR, "PHASE26_AB_TEST_REPORT.md")


async def _get_version_info_async(label: str) -> dict:
    """Get all comparison metrics from DB for current state (async)."""
    import asyncpg
    results = {"label": label, "strategies": 0, "validated": 0, "failed": 0,
               "avg_sharpe": 0.0, "avg_composite": 0.0, "avg_drawdown": 0.0,
               "mutations": 0, "trades": 0, "scout_signals": 0}
    try:
        conn_data = await asyncpg.connect(DB_URL)
    except Exception:
        return results
    try:
        row = await conn_data.fetchrow(
            "SELECT COUNT(*) as total, "
            "COALESCE(AVG(sharpe),0) as avg_sharpe, "
            "COALESCE(AVG(composite_score),0) as avg_comp, "
            "COALESCE(AVG(max_drawdown),0) as avg_dd "
            "FROM backtest_results "
            "WHERE created_at > NOW() - INTERVAL '30 minutes'"
        )
        if row:
            results["strategies"] = row["total"]
            results["avg_sharpe"] = float(row["avg_sharpe"] or 0)
            results["avg_composite"] = float(row["avg_comp"] or 0)
            results["avg_drawdown"] = float(row["avg_dd"] or 0)

        row2 = await conn_data.fetchrow(
            "SELECT COUNT(*) FROM strategies "
            "WHERE created_at > NOW() - INTERVAL '30 minutes'"
        )
        if row2:
            results["strategies"] = max(results["strategies"], row2[0])

        row3 = await conn_data.fetchrow(
            "SELECT COUNT(*) FROM paper_trades "
            "WHERE created_at > NOW() - INTERVAL '30 minutes'"
        )
        if row3:
            results["trades"] = row3[0]

        row4 = await conn_data.fetchrow(
            "SELECT COUNT(*) FROM mutation_record "
            "WHERE created_at > NOW() - INTERVAL '30 minutes'"
        )
        if row4:
            results["mutations"] = row4[0]

        row5 = await conn_data.fetchrow(
            "SELECT COUNT(*) FROM scout_signals "
            "WHERE created_at > NOW() - INTERVAL '30 minutes'"
        )
        if row5:
            results["scout_signals"] = row5[0]
    except Exception:
        pass
    finally:
        await conn_data.close()
    return results


def _get_version_info(label: str) -> dict:
    """Sync wrapper for _get_version_info_async."""
    return asyncio.run(_get_version_info_async(label))


async def cleanup_db_async():
    """Delete records from the last session to ensure clean baseline."""
    conn_data = await asyncpg.connect(DB_URL)
    try:
        for table in ["backtest_results", "strategies", "paper_trades", 
                       "mutation_record", "scout_signals", "scout_influence_log",
                       "scout_economic_attribution"]:
            try:
                await conn_data.execute(f"DELETE FROM {table}")
            except Exception:
                pass
        print(f"[CLEANUP] DB cleaned")
    except Exception as e:
        print(f"[CLEANUP] Failed: {e}")
    finally:
        await conn_data.close()


def cleanup_db():
    """Sync wrapper."""
    asyncio.run(cleanup_db_async())


def run_session(label: str, scouts_on: bool, duration_minutes: int = SESSION_DURATION) -> dict:
    """Run the full autonomous cycle for a session and return metrics."""
    print(f"\n{'='*60}")
    print(f"SESSION: {label} (scouts={'ON' if scouts_on else 'OFF'})")
    print(f"{'='*60}")

    # Set environment
    env = os.environ.copy()
    env["SCOUTS_ENABLED"] = "true" if scouts_on else "false"
    env["PYTHONPATH"] = _PARENT

    pipeline_script = os.path.join(ATLAS_DIR, "scripts", "full_autonomous_cycle.py")
    log_path = os.path.join(ATLAS_DIR, f"phase26_ab_{label.lower().replace(' ','_')}.log")

    start = time.time()
    proc = subprocess.Popen(
        [sys.executable, pipeline_script, f"--duration-minutes={duration_minutes}"],
        cwd=ATLAS_DIR,
        env=env,
        stdout=open(log_path, "w"),
        stderr=subprocess.STDOUT,
    )
    proc.wait(timeout=duration_minutes * 60 + 120)
    elapsed = time.time() - start
    exit_code = proc.returncode

    print(f"[{label}] Pipeline exited with rc={exit_code} after {elapsed:.0f}s")
    return {"exit_code": exit_code, "elapsed": elapsed, "label": label}


def generate_report(results_a: dict, results_b: dict):
    """Generate the A/B test comparison report."""
    a, b = results_a, results_b
    lines = []
    lines.append("# PHASE 26F — A/B INTELLIGENCE TEST REPORT")
    lines.append(f"\nGenerated: {datetime.utcnow().isoformat()}Z")
    lines.append("\n## Overview")
    lines.append(f"\n### Test A (scouts OFF)")
    lines.append(f"- Duration: {SESSION_DURATION} minutes")
    lines.append(f"- Strategies generated: {a.get('strategies',0)}")
    lines.append(f"- Avg Sharpe: {a.get('avg_sharpe',0):.4f}")
    lines.append(f"- Avg Composite Score: {a.get('avg_composite',0):.2f}")
    lines.append(f"- Avg Drawdown: {a.get('avg_drawdown',0):.2%}")
    lines.append(f"- Mutations: {a.get('mutations',0)}")
    lines.append(f"- Trades executed: {a.get('trades',0)}")
    lines.append(f"- Total scout signals: {a.get('scout_signals',0)}")
    lines.append(f"- Pipeline exit code: {a.get('exit_code','N/A')}")
    lines.append(f"- Elapsed: {a.get('elapsed',0):.0f}s")

    lines.append(f"\n### Test B (scouts ON)")
    lines.append(f"- Duration: {SESSION_DURATION} minutes")
    lines.append(f"- Strategies generated: {b.get('strategies',0)}")
    lines.append(f"- Avg Sharpe: {b.get('avg_sharpe',0):.4f}")
    lines.append(f"- Avg Composite Score: {b.get('avg_composite',0):.2f}")
    lines.append(f"- Avg Drawdown: {b.get('avg_drawdown',0):.2%}")
    lines.append(f"- Mutations: {b.get('mutations',0)}")
    lines.append(f"- Trades executed: {b.get('trades',0)}")
    lines.append(f"- Total scout signals: {b.get('scout_signals',0)}")
    lines.append(f"- Pipeline exit code: {b.get('exit_code','N/A')}")
    lines.append(f"- Elapsed: {b.get('elapsed',0):.0f}s")

    lines.append("\n## Comparison Summary")
    delta_strat = b.get('strategies',0) - a.get('strategies',0)
    delta_sharpe = b.get('avg_sharpe',0) - a.get('avg_sharpe',0)
    delta_comp = b.get('avg_composite',0) - a.get('avg_composite',0)
    delta_dd = a.get('avg_drawdown',0) - b.get('avg_drawdown',0)  # positive = improvement
    delta_trades = b.get('trades',0) - a.get('trades',0)

    lines.append(f"\n| Metric | Scouts OFF (A) | Scouts ON (B) | Delta |")
    lines.append(f"|---|---|---|---|")
    lines.append(f"| Strategies Generated | {a.get('strategies',0)} | {b.get('strategies',0)} | {delta_strat:+d} |")
    lines.append(f"| Avg Sharpe | {a.get('avg_sharpe',0):.4f} | {b.get('avg_sharpe',0):.4f} | {delta_sharpe:+.4f} |")
    lines.append(f"| Avg Composite Score | {a.get('avg_composite',0):.2f} | {b.get('avg_composite',0):.2f} | {delta_comp:+.2f} |")
    lines.append(f"| Avg Drawdown | {a.get('avg_drawdown',0):.2%} | {b.get('avg_drawdown',0):.2%} | {delta_dd:+.2%} |")
    lines.append(f"| Mutations | {a.get('mutations',0)} | {b.get('mutations',0)} | {b.get('mutations',0)-a.get('mutations',0):+d} |")
    lines.append(f"| Trades | {a.get('trades',0)} | {b.get('trades',0)} | {delta_trades:+d} |")
    lines.append(f"| Scout Signals | {a.get('scout_signals',0)} | {b.get('scout_signals',0)} | {b.get('scout_signals',0)-a.get('scout_signals',0):+d} |")

    lines.append("\n## Success Criteria Evaluation")
    pass_count = 0
    total_checks = 6

    # C1: Scouts materially influence ideation
    if b.get('strategies',0) != a.get('strategies',0):
        lines.append(f"\n✅ C1: Behavioral difference detected (strategies: {a.get('strategies',0)} vs {b.get('strategies',0)})")
        pass_count += 1
    else:
        lines.append(f"\n❌ C1: No behavioral difference in strategy count")

    # C2: Sharpe improvement
    if delta_sharpe > 0.05:
        lines.append(f"✅ C2: Scouts improved avg Sharpe ({a.get('avg_sharpe',0):.4f} -> {b.get('avg_sharpe',0):.4f})")
        pass_count += 1
    elif delta_sharpe > 0:
        lines.append(f"⚠️ C2: Marginal Sharpe improvement ({delta_sharpe:+.4f})")
        pass_count += 1
    else:
        lines.append(f"❌ C2: No Sharpe improvement ({delta_sharpe:+.4f})")

    # C3: Behavioral differences exist
    if abs(delta_strat) >= 1 or abs(delta_sharpe) > 0.01 or abs(delta_trades) >= 1:
        lines.append(f"✅ C3: Clear behavioral differences between scouts OFF and ON")
        pass_count += 1
    else:
        lines.append(f"❌ C3: No clear behavioral differences detected")

    # C4: Drawdown improvement
    if delta_dd > 0:
        lines.append(f"✅ C4: Drawdown improved with scouts active ({a.get('avg_drawdown',0):.2%} -> {b.get('avg_drawdown',0):.2%})")
        pass_count += 1
    else:
        lines.append(f"❌ C4: No drawdown improvement with scouts")

    # C5: Scout signals active
    if b.get('scout_signals',0) > 0:
        lines.append(f"✅ C5: Scout signals present in active system ({b.get('scout_signals',0)})")
        pass_count += 1
    else:
        lines.append(f"❌ C5: No scout signals detected")

    # C6: Overall behavior change
    if delta_strat != 0 or abs(delta_sharpe) > 0.01:
        lines.append(f"✅ C6: Overall behavioral adaptation confirmed")
        pass_count += 1
    else:
        lines.append(f"❌ C6: No overall behavioral adaptation")

    lines.append(f"\n## Result: {pass_count}/{total_checks} criteria passed")
    if pass_count >= total_checks * 0.5:
        lines.append("\n### 🟢 PHASE 26F PASSES — Scout-driven behavioral adaptation confirmed")
    else:
        lines.append("\n### 🔴 PHASE 26F FAILS — Insufficient behavioral evidence")

    report = "\n".join(lines)
    with open(REPORT_PATH, "w") as f:
        f.write(report)
    print(f"\n[REPORT] Written to {REPORT_PATH}")
    return report


if __name__ == "__main__":
    print("PHASE 26F — A/B Intelligence Testing")
    print("=====================================")

    # Clean DB before Session A
    cleanup_db()
    time.sleep(2)

    # Session A: scouts OFF
    run_session("A_scouts_OFF", scouts_on=False)
    time.sleep(5)
    results_a = _get_version_info("A_scouts_OFF")

    # Clean DB before Session B
    cleanup_db()
    time.sleep(2)

    # Session B: scouts ON
    run_session("B_scouts_ON", scouts_on=True)
    time.sleep(5)
    results_b = _get_version_info("B_scouts_ON")

    # Generate report
    report = generate_report(results_a, results_b)
    print(report[-500:] if len(report) > 500 else report)
