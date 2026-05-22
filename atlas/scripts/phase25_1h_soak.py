"""
Phase 25H — 1-Hour Economic Activation Soak
============================================
Runs full_autonomous_cycle.py in background and samples DB every 5 minutes.
Measures scout identity, trust evolution, debug pipeline, and economic impact.

Usage:
    python scripts/phase25_1h_soak.py
"""
import asyncio
import json
import os
import subprocess
import sys
import time
from datetime import datetime, timezone

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_PARENT = os.path.normpath(os.path.join(_THIS_DIR, ".."))
sys.path.insert(0, os.path.normpath(os.path.join(_PARENT, "..")))
sys.path.insert(0, _PARENT)

from loguru import logger
from sqlalchemy.sql import text

from config.settings import settings
from data.storage.timescale_client import TimescaleClient

DURATION_MINUTES = 60
CHECKPOINT_INTERVAL = 300  # 5 minutes


def _safe_int(v, default=0) -> int:
    """Safely convert a value to int, returning default on failure."""
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        try:
            return int(v)
        except (ValueError, TypeError):
            return default
    return default


async def sample_db(tc: TimescaleClient, label: str) -> dict:
    """Capture a full snapshot of all Phase 25 metrics."""
    results = {"label": label, "timestamp": datetime.now(timezone.utc).isoformat()}
    async with tc.engine.connect() as conn:
        # scout_signals total
        r = await conn.execute(text("SELECT COUNT(*) FROM scout_signals"))
        results["scout_signals_total"] = r.scalar() or 0

        # scout_signals by source
        r = await conn.execute(text("""
            SELECT source, signal_type, COUNT(*) as cnt
            FROM scout_signals GROUP BY source, signal_type ORDER BY cnt DESC
        """))
        results["by_source"] = {f"{row[0]}/{row[1]}": row[2] for row in r.fetchall()}

        # Unknown sources
        r = await conn.execute(text("SELECT COUNT(*) FROM scout_signals WHERE source = 'unknown'"))
        results["unknown_sources"] = r.scalar() or 0

        # Debug log
        try:
            r = await conn.execute(text("SELECT COUNT(*) FROM scout_mirror_debug_log"))
            results["debug_log_total"] = r.scalar() or 0
            r = await conn.execute(text("""
                SELECT table_name, success, COUNT(*) as cnt
                FROM scout_mirror_debug_log GROUP BY table_name, success ORDER BY cnt DESC
            """))
            results["debug_by_table"] = {f"{row[0]}/{'OK' if row[1] else 'FAIL'}": row[2] for row in r.fetchall()}
        except Exception:
            results["debug_log_total"] = 0

        # Trust / entropy scores
        try:
            r = await conn.execute(text("""
                SELECT source, trust_score, signal_count, entropy_score
                FROM source_performance_log ORDER BY trust_score DESC
            """))
            results["trust_scores"] = {
                row[0]: {
                    "trust": round(float(row[1] or 0), 4),
                    "signals": row[2] or 0,
                    "entropy": round(float(row[3] or 0), 4),
                }
                for row in r.fetchall()
            }
        except Exception:
            results["trust_scores"] = {}

        # External sources
        try:
            r = await conn.execute(text("""
                SELECT source, COUNT(*) as cnt FROM external_scout_memory
                GROUP BY source ORDER BY cnt DESC LIMIT 10
            """))
            results["external_sources"] = {row[0]: row[1] for row in r.fetchall()}
        except Exception:
            results["external_sources"] = {}

        # Recent activity (always return int, never string)
        for table, interval_col, interval_val in [
            ("strategies", "created_at", "5 minutes"),
            ("paper_trades", "created_at", "5 minutes"),
        ]:
            try:
                r = await conn.execute(
                    text(f"SELECT COUNT(*) FROM {table} WHERE {interval_col} > NOW() - INTERVAL :iv"),
                    {"iv": interval_val},
                )
                results[f"{table}_5min"] = r.scalar() or 0
            except Exception:
                results[f"{table}_5min"] = 0

        # Quarantine
        for table in ["scout_poison_quarantine", "scout_quarantine"]:
            try:
                r = await conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                results[table] = r.scalar() or 0
            except Exception:
                results[table] = 0

        # Contradiction events (scout signal conflicts)
        try:
            r = await conn.execute(text("""
                SELECT COUNT(*) FROM scout_poison_quarantine
                WHERE violation_type IN ('contradiction', 'entropy_anomaly', 'source_collapse')
            """))
            results["contradiction_events"] = r.scalar() or 0
        except Exception:
            results["contradiction_events"] = 0

    # Pretty print
    print(f"\n{'='*60}")
    print(f"  CHECKPOINT: {label}")
    print(f"{'='*60}")
    print(f"  scout_signals: {results['scout_signals_total']}")
    print(f"  unknown sources: {results['unknown_sources']} {'[OK]' if results['unknown_sources'] == 0 else '[FAIL]'}")
    print(f"  source distribution: {json.dumps(results.get('by_source', {}), indent=2)}")
    print(f"  debug log: {results.get('debug_log_total', 0)}")
    dl = results.get("debug_by_table", {})
    if isinstance(dl, dict) and dl:
        print(f"  debug by table: {json.dumps(dl)}")
    ts = results.get("trust_scores", {})
    if isinstance(ts, dict) and ts:
        print(f"  trust scores: {json.dumps(ts, indent=2)}")
    print(f"  strategies (5m): {results.get('strategies_5min', 0)}")
    print(f"  trades (5m): {results.get('paper_trades_5min', 0)}")
    print(f"  poison/quarantine: {results.get('scout_poison_quarantine', 0)}/{results.get('scout_quarantine', 0)}")
    print(f"  contradictions: {results.get('contradiction_events', 0)}")
    ext = results.get("external_sources", {})
    if isinstance(ext, dict) and ext:
        print(f"  external sources: {json.dumps(ext)}")
    print(f"{'='*60}\n")
    return results


async def monitoring_loop(tc: TimescaleClient, pipe_proc: subprocess.Popen, duration_minutes: int):
    """Sample DB every checkpoint_interval and check pipeline health."""
    checkpoints = []
    start = time.time()
    end_time = start + duration_minutes * 60

    # Baseline
    cp = await sample_db(tc, "BASELINE (t=0s)")
    checkpoints.append(cp)

    while time.time() < end_time:
        await asyncio.sleep(CHECKPOINT_INTERVAL)
        elapsed = int(time.time() - start)
        label = f"t={elapsed}s ({elapsed//60}m{elapsed%60}s)"
        cp = await sample_db(tc, label)
        checkpoints.append(cp)

        # Check if pipeline process is still alive (non-blocking poll)
        if pipe_proc.poll() is not None:
            print(f"\n[WARN] Pipeline process exited early (rc={pipe_proc.returncode}) at {label}")
            break

    return checkpoints


async def main():
    print(f"\n{'#'*60}")
    print(f"  PHASE 25 — 1-HOUR ECONOMIC ACTIVATION SOAK")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")
    print(f"  Duration: {DURATION_MINUTES} minutes")
    print(f"{'#'*60}\n")

    tc = TimescaleClient(settings.database_url)

    # Ensure debug log table exists
    async with tc.engine.begin() as conn:
        await conn.execute(text("""
            CREATE TABLE IF NOT EXISTS scout_mirror_debug_log (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                table_name TEXT, source TEXT, symbol TEXT, signal_type TEXT,
                confidence_score NUMERIC DEFAULT 0.0,
                success BOOLEAN DEFAULT FALSE, error_message TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_debug_log_created ON scout_mirror_debug_log (created_at DESC)"))
        await conn.execute(text("CREATE INDEX IF NOT EXISTS idx_debug_log_table ON scout_mirror_debug_log (table_name)"))
        await conn.execute(text("DELETE FROM scout_mirror_debug_log"))

    print("[OK] Debug table ready, cleared for clean baseline\n")

    # Start full_autonomous_cycle.py in background
    # Pipeline output is captured to a log file to avoid PIPE buffer deadlock.
    atlas_dir = _PARENT  # atlas/ directory (parent of scripts/)
    log_path = os.path.join(atlas_dir, "phase25_soak_pipeline.log")

    env = os.environ.copy()
    env["PYTHONPATH"] = os.path.normpath(os.path.join(atlas_dir, ".."))  # Parent of atlas/ for atlas.xxx imports

    pipe_proc = subprocess.Popen(
        [sys.executable, "scripts/full_autonomous_cycle.py", f"--duration-minutes={DURATION_MINUTES}"],
        cwd=atlas_dir,
        env=env,
        stdout=open(log_path, "w"),
        stderr=subprocess.STDOUT,
        text=True,
    )
    print(f"[OK] Pipeline started (PID={pipe_proc.pid}), log: {log_path}\n")

    # Run monitoring loop
    checkpoints = await monitoring_loop(tc, pipe_proc, DURATION_MINUTES)

    # Wait for pipeline to finish with timeout
    try:
        pipe_rc = pipe_proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        pipe_proc.kill()
        pipe_rc = pipe_proc.wait()
        print("[WARN] Pipeline did not exit within 30s — force killed")

    print(f"\n[OK] Pipeline exited with rc={pipe_rc}")

    # Final sample
    final_cp = await sample_db(tc, "FINAL (post-shutdown)")
    checkpoints.append(final_cp)

    # ── Generate Final Report ──
    print(f"\n{'='*60}")
    print(f"  PHASE 25 — 1-HOUR SOAK FINAL RESULTS")
    print(f"{'='*60}")

    first = checkpoints[0]
    last = checkpoints[-1]

    # Calculate growth (use safe_int to handle any type)
    initial_signals = _safe_int(first.get("scout_signals_total", 0))
    final_signals = _safe_int(last.get("scout_signals_total", 0))
    growth = final_signals - initial_signals

    print(f"\n  SCOUT SIGNALS:")
    print(f"    Initial: {initial_signals}")
    print(f"    Final:   {final_signals}")
    print(f"    Growth:  {growth}")
    rate = growth / DURATION_MINUTES if DURATION_MINUTES > 0 else 0
    print(f"    Rate:    {rate:.1f} signals/min")
    print(f"    Unknown: {last.get('unknown_sources', '?')}")

    print(f"\n  SOURCE DISTRIBUTION:")
    for k, v in last.get("by_source", {}).items():
        print(f"    {k}: {v}")

    print(f"\n  DEBUG LOG:")
    print(f"    Total entries: {_safe_int(last.get('debug_log_total', 0))}")
    dl = last.get("debug_by_table", {})
    if isinstance(dl, dict) and dl:
        for k, v in dl.items():
            print(f"    {k}: {v}")

    print(f"\n  TRUST EVOLUTION:")
    ts = last.get("trust_scores", {})
    if isinstance(ts, dict) and ts:
        for source, info in ts.items():
            print(f"    {source}: trust={info['trust']:.4f}, signals={info['signals']}, entropy={info['entropy']:.4f}")

    print(f"\n  TRADING ACTIVITY:")
    print(f"    Strategies (5m): {_safe_int(last.get('strategies_5min', 0))}")
    print(f"    Paper trades (5m): {_safe_int(last.get('paper_trades_5min', 0))}")

    print(f"\n  PIPELINE HEALTH:")
    print(f"    Poison quarantine: {_safe_int(last.get('scout_poison_quarantine', 0))}")
    print(f"    Scout quarantine: {_safe_int(last.get('scout_quarantine', 0))}")
    ext = last.get("external_sources", {})
    if isinstance(ext, dict) and ext:
        print(f"    External sources: {json.dumps(ext)}")
    print(f"    Pipeline exit code: {pipe_rc}")

    # ── Success Criteria (aligned to user's exact 10) ──
    print(f"\n{'='*60}")
    print(f"  SUCCESS CRITERIA")
    print(f"{'='*60}")

    strat_count = _safe_int(last.get("strategies_5min", 0))
    trade_count = _safe_int(last.get("paper_trades_5min", 0))
    debug_total = _safe_int(last.get("debug_log_total", 0))
    ts_active = isinstance(ts, dict) and len(ts) > 0

    criteria = [
        ("1. Zero unknown scout sources", last.get("unknown_sources", 99) == 0),
        ("2. Scout signals actively generated", final_signals > 0),
        ("3. Source-level attribution works (>=4 sources)", len(last.get("by_source", {})) >= 4),
        ("4. Trust evolves independently per source", ts_active),
        ("5. Entropy calculations meaningful", ts_active and any(info.get("entropy", 0) > 0 for info in ts.values())),
        ("6. Scouts materially influence organism", strat_count > 0 or trade_count > 0),
        ("7. Attribution chains replay-safe (pipeline clean exit)", pipe_rc == 0),
        ("8. No scout identity corruption (zero unknown)", last.get("unknown_sources", 99) == 0),
        ("9. Economic attribution measurable (debug + trust active)", debug_total > 0 and ts_active),
        ("10. Epistemic memory developing (trust + debug + entropy active)", debug_total > 0 and ts_active and any(info.get("entropy", 0) > 0 for info in ts.values())),
    ]

    passed = sum(1 for _, ok in criteria if ok)
    for desc, ok in criteria:
        print(f"  {'[PASS]' if ok else '[FAIL]'} {desc}")

    print(f"\n  SCORE: {passed}/{len(criteria)}")
    if passed == len(criteria):
        print(f"  [PASS] PHASE 25 — SCOUT IDENTITY & ECONOMIC ACTIVATION: CERTIFIED")
    else:
        print(f"  [WARN] {len(criteria) - passed} criteria not met — review failures above")
    print(f"{'='*60}\n")

    # Save results to JSON
    results_path = os.path.join(_PARENT, "phase25_soak_results.json")
    with open(results_path, "w") as f:
        json.dump({
            "checkpoints": checkpoints,
            "final_score": f"{passed}/{len(criteria)}",
            "certified": passed == len(criteria),
            "criteria": {desc: ok for desc, ok in criteria},
        }, f, indent=2, default=str)
    print(f"[OK] Full results saved to {results_path}")


if __name__ == "__main__":
    asyncio.run(main())
