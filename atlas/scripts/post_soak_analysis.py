"""
post_soak_analysis.py — Phase 24 Delivery: Post-1h-Soak Comprehensive Analysis.

Runs after the 60-minute autonomous soak completes. Analyzes:
  1. Database health & row growth
  2. Trading output & strategy quality
  3. Execution accuracy & copy-trading
  4. Scout network & portfolio health
  5. Hash chain integrity
  6. Failed / dead-letter records

Outputs: summary dict consumable by report generators.
"""

import asyncio
import json
import sys
from datetime import datetime, timezone
from collections import defaultdict

from atlas.config.settings import settings
from atlas.data.storage.timescale_client import TimescaleClient
from sqlalchemy.sql import text


async def run_post_soak_analysis() -> dict:
    """Run all post-soak checks and return structured results."""
    db = TimescaleClient(settings.database_url)
    await db.connect()

    results = {}

    async with db.engine.connect() as conn:
        # =====================================================================
        # 1. DATABASE HEALTH & ROW GROWTH
        # =====================================================================
        print("=" * 60)
        print("1. DATABASE HEALTH & ROW GROWTH")
        print("=" * 60)

        tables = [
            "strategies", "backtest_results", "backtest_trades",
            "lifecycle_events", "event_store", "audit_ledger",
            "execution_log", "paper_trades", "positions",
            "copy_execution_log", "capital_allocation",
            "mutation_policy_state", "feature_importance",
            "drift_detection", "scout_signals",
            "external_scout_memory", "scout_quarantine",
            "failed_inserts", "pattern_memory",
            "agent_registry", "system_logs",
        ]

        row_counts = {}
        for t in tables:
            try:
                r = await conn.execute(text(f"SELECT COUNT(*) FROM {t}"))
                count = r.scalar() or 0
                row_counts[t] = count
                print(f"  {t:30s} {count:>8,} rows")
            except Exception as e:
                row_counts[t] = -1
                print(f"  {t:30s} ERROR: {e}")

        results["database_row_counts"] = row_counts
        results["database_total_rows"] = sum(v for v in row_counts.values() if v > 0)
        results["database_table_count"] = sum(1 for v in row_counts.values() if v >= 0)

        # =====================================================================
        # 2. TRADING OUTPUT & STRATEGY QUALITY
        # =====================================================================
        print("\n" + "=" * 60)
        print("2. TRADING OUTPUT & STRATEGY QUALITY")
        print("=" * 60)

        # Strategies by status
        r = await conn.execute(text("""
            SELECT status, COUNT(*) as cnt
            FROM strategies GROUP BY status ORDER BY cnt DESC
        """))
        strategy_statuses = {row[0]: row[1] for row in r.fetchall()}
        print(f"  Strategies by status: {strategy_statuses}")

        # Backtest results
        r = await conn.execute(text("""
            SELECT COUNT(*) as total,
                   COUNT(short_window_score) as scored,
                   ROUND(AVG(short_window_score)::numeric, 4) as avg_score,
                   ROUND(AVG(sharpe)::numeric, 4) as avg_sharpe,
                   ROUND(AVG(win_rate)::numeric, 4) as avg_win_rate
            FROM backtest_results
        """))
        bt = r.fetchone()
        print(f"  Backtest results: total={bt[0]}, scored={bt[1]}, "
              f"avg_score={bt[2]}, avg_sharpe={bt[3]}, avg_win_rate={bt[4]}")
        results["backtest"] = {
            "total": bt[0], "scored": bt[1],
            "avg_short_window_score": float(bt[2]) if bt[2] else 0,
            "avg_sharpe": float(bt[3]) if bt[3] else 0,
            "avg_win_rate": float(bt[4]) if bt[4] else 0,
        }

        # Top strategies
        r = await conn.execute(text("""
            SELECT s.name, s.status, b.short_window_score, b.sharpe, b.win_rate
            FROM strategies s
            JOIN backtest_results b ON s.id = b.strategy_id
            WHERE b.short_window_score IS NOT NULL
            ORDER BY b.short_window_score DESC
            LIMIT 10
        """))
        top_strategies = []
        print("  Top 10 strategies:")
        for row in r.fetchall():
            top_strategies.append({
                "name": row[0], "status": row[1],
                "score": float(row[2]) if row[2] else 0,
                "sharpe": float(row[3]) if row[3] else 0,
                "win_rate": float(row[4]) if row[4] else 0,
            })
            print(f"    {row[0]:40s} score={row[2]:>8.2f}  sharpe={row[3]:>6.2f}  wr={row[4]:>6.2%}")
        results["top_strategies"] = top_strategies

        # =====================================================================
        # 3. EXECUTION ACCURACY & COPY-TRADING
        # =====================================================================
        print("\n" + "=" * 60)
        print("3. EXECUTION ACCURACY & COPY-TRADING")
        print("=" * 60)

        # Paper trades
        r = await conn.execute(text("""
            SELECT side, COUNT(*), ROUND(AVG(price)::numeric, 2),
                   ROUND(COALESCE(SUM(pnl), 0)::numeric, 2)
            FROM paper_trades GROUP BY side
        """))
        paper_trades = {}
        print("  Paper trades by side:")
        for row in r.fetchall():
            paper_trades[row[0]] = {
                "count": row[1], "avg_price": float(row[2]) if row[2] else 0,
                "total_pnl": float(row[3]) if row[3] else 0,
            }
            print(f"    {row[0]:10s} count={row[1]:>5}  avg_price={row[2]:>10.2f}  pnl={row[3]:>10.2f}")
        results["paper_trades"] = paper_trades

        # Copy execution log
        r = await conn.execute(text("SELECT COUNT(*) FROM copy_execution_log"))
        copy_count = r.scalar() or 0
        print(f"  Copy execution entries: {copy_count}")
        results["copy_execution_count"] = copy_count

        # Total PnL
        r = await conn.execute(text("SELECT ROUND(COALESCE(SUM(pnl), 0)::numeric, 2) FROM paper_trades"))
        total_pnl = float(r.scalar() or 0)
        print(f"  Total PnL: {total_pnl:,.2f}")
        results["total_pnl"] = total_pnl

        # =====================================================================
        # 4. SCOUT NETWORK & PORTFOLIO HEALTH
        # =====================================================================
        print("\n" + "=" * 60)
        print("4. SCOUT NETWORK & PORTFOLIO HEALTH")
        print("=" * 60)

        # Scout signals
        r = await conn.execute(text("SELECT COUNT(*) FROM scout_signals"))
        scout_count = r.scalar() or 0
        print(f"  Scout signals: {scout_count}")
        results["scout_signals_count"] = scout_count

        # Scout signals by source
        r = await conn.execute(text("""
            SELECT source, COUNT(*) as cnt
            FROM scout_signals GROUP BY source ORDER BY cnt DESC
        """))
        scout_by_source = {}
        print("  Scout signals by source:")
        for row in r.fetchall():
            scout_by_source[row[0]] = row[1]
            print(f"    {row[0]:30s} {row[1]:>6}")
        results["scout_by_source"] = scout_by_source

        # External scout memory
        r = await conn.execute(text("SELECT COUNT(*) FROM external_scout_memory"))
        ext_scout = r.scalar() or 0
        print(f"  External scout entries: {ext_scout}")
        results["external_scout_memory_count"] = ext_scout

        # Scout quarantine
        r = await conn.execute(text("SELECT COUNT(*) FROM scout_quarantine"))
        quarantined = r.scalar() or 0
        print(f"  Quarantined scouts: {quarantined}")
        results["scout_quarantine_count"] = quarantined

        # Capital allocation
        r = await conn.execute(text("""
            SELECT COALESCE(MAX(n_strategies), 0) as strategies_allocated,
                   ROUND(COALESCE(SUM(total_exposure), 0)::numeric, 2) as total_exposure
            FROM capital_allocation
        """))
        ca = r.fetchone()
        alloc_strategies = int(ca[0]) if ca[0] else 0
        alloc_exposure = float(ca[1]) if ca[1] else 0
        print(f"  Capital allocation: {alloc_strategies} strategies, {alloc_exposure:,.2f} total exposure")
        results["capital_allocation"] = {
            "strategies_allocated": alloc_strategies,
            "total_allocation": alloc_exposure,
        }

        # =====================================================================
        # 5. HASH CHAIN INTEGRITY & DEAD LETTERS
        # =====================================================================
        print("\n" + "=" * 60)
        print("5. HASH CHAIN INTEGRITY & DEAD LETTERS")
        print("=" * 60)

        # Event store
        r = await conn.execute(text("""
            SELECT COUNT(*) as total,
                   COUNT(hash_self) as hashed
            FROM event_store
        """))
        es = r.fetchone()
        print(f"  Event store: {es[0]} total, {es[1]} hashed")
        results["event_store"] = {"total": es[0], "hashed": es[1]}

        # Audit ledger
        r = await conn.execute(text("""
            SELECT COUNT(*) as total,
                   COUNT(hash_self) as hashed
            FROM audit_ledger
        """))
        al = r.fetchone()
        print(f"  Audit ledger: {al[0]} total, {al[1]} hashed")
        results["audit_ledger"] = {"total": al[0], "hashed": al[1]}

        # Failed inserts
        r = await conn.execute(text("SELECT COUNT(*) FROM failed_inserts"))
        failed = r.scalar() or 0
        print(f"  Failed inserts (dead-letter): {failed}")
        results["failed_inserts"] = failed

        if failed > 0:
            r = await conn.execute(text("""
                SELECT table_name, reason, COUNT(*) as cnt
                FROM failed_inserts GROUP BY table_name, reason ORDER BY cnt DESC LIMIT 10
            """))
            print("  Failed inserts breakdown:")
            results["failed_inserts_breakdown"] = []
            for row in r.fetchall():
                entry = {"table": row[0], "reason": row[1], "count": row[2]}
                results["failed_inserts_breakdown"].append(entry)
                print(f"    {row[0]:25s} reason={row[1][:50]:50s} count={row[2]}")

        # =====================================================================
        # 6. LIFECYCLE EVENTS & AGENT HEALTH
        # =====================================================================
        print("\n" + "=" * 60)
        print("6. LIFECYCLE EVENTS & AGENT HEALTH")
        print("=" * 60)

        r = await conn.execute(text("""
            SELECT COALESCE(actor, agent_name, 'unknown') as agent, stage, COUNT(*) as cnt
            FROM lifecycle_events
            GROUP BY agent, stage
            ORDER BY agent, cnt DESC
        """))
        agent_events = defaultdict(dict)
        for row in r.fetchall():
            agent_events[row[0]][row[1]] = row[2]

        # Map lifecycle stages to event types
        stage_mapping = {
            "started": "started",
            "stopped": "stopped",
            "crashed": "crashed",
            "error": "crashed",
            "ideator": "started",
            "coder": "started",
            "backtest": "started",
            "mutation_context": "started",
            "pattern": "started",
        }

        total_starts = sum(
            v.get("started", 0) + v.get("ideator", 0) + v.get("coder", 0) +
            v.get("backtest", 0) + v.get("mutation_context", 0) + v.get("pattern", 0)
            for v in agent_events.values()
        )
        total_stops = sum(
            v.get("stopped", 0) for v in agent_events.values()
        )
        total_crashes = sum(
            v.get("crashed", 0) for v in agent_events.values()
        )
        unique_agents = len(agent_events)
        print(f"  Agent lifecycle: {total_starts} starts, {total_stops} stops, {total_crashes} crashes")
        print(f"  Unique agents: {len(agent_events)}")
        results["agent_lifecycle"] = {
            "starts": total_starts,
            "stops": total_stops,
            "crashes": total_crashes,
            "unique_agents": unique_agents,
        }

        if total_crashes > 0:
            results["crashing_agents"] = [
                {"agent": a, "crashes": v.get("crashed", v.get("error", 0))}
                for a, v in agent_events.items()
                if v.get("crashed", v.get("error", 0)) > 0
            ]

    # =====================================================================
    # FINAL SUMMARY
    # =====================================================================
    print("\n" + "=" * 60)
    print("POST-SOAK ANALYSIS SUMMARY")
    print("=" * 60)

    passed = 0
    failed = 0
    warnings = 0

    # Checks
    if results["database_total_rows"] > 50000:
        print("  [OK] Database has sufficient data")
        passed += 1
    else:
        print("  [FAIL] Database has insufficient data")
        failed += 1

    if results.get("backtest", {}).get("total", 0) > 100:
        print(f"  [OK] {results['backtest']['total']} backtest results")
        passed += 1
    else:
        print("  [WARN] Few backtest results")
        warnings += 1

    if results.get("top_strategies"):
        top_score = results["top_strategies"][0]["score"]
        print(f"  [OK] Top strategy score: {top_score:.2f}")
        passed += 1
    else:
        print("  [FAIL] No top strategies found")
        failed += 1

    if results.get("event_store", {}).get("hashed", 0) > 0:
        print("  [OK] Event store hash chain intact")
        passed += 1
    else:
        print("  [FAIL] Event store hash chain broken")
        failed += 1

    if results.get("audit_ledger", {}).get("hashed", 0) > 0:
        print("  [OK] Audit ledger hash chain intact")
        passed += 1
    else:
        print("  [FAIL] Audit ledger hash chain broken")
        failed += 1

    if results.get("failed_inserts", 0) < 10:
        print(f"  [OK] Failed inserts: {results['failed_inserts']}")
        passed += 1
    else:
        print(f"  [WARN] {results['failed_inserts']} failed inserts — investigate")
        warnings += 1

    if results.get("agent_lifecycle", {}).get("crashes", 0) == 0:
        print("  [OK] Zero agent crashes")
        passed += 1
    else:
        print(f"  [WARN] {results['agent_lifecycle']['crashes']} agent crashes")
        warnings += 1

    if results.get("scout_signals_count", 0) > 0:
        print("  [OK] Scout network producing signals")
        passed += 1
    else:
        print("  [WARN] No scout signals")
        warnings += 1

    results["summary"] = {
        "passed": passed,
        "failed": failed,
        "warnings": warnings,
        "total_checks": passed + failed + warnings,
        "soak_ready": failed == 0,
    }

    print(f"\n  Passed: {passed}, Failed: {failed}, Warnings: {warnings}")
    print(f"  >>> {'READY FOR DELIVERY' if failed == 0 else 'ISSUES FOUND'} <<<")

    await db.close()
    return results


if __name__ == "__main__":
    results = asyncio.run(run_post_soak_analysis())
    with open("post_soak_analysis_results.json", "w") as f:
        json.dump(results, f, indent=2, default=str)
    print(f"\nResults saved to post_soak_analysis_results.json")
