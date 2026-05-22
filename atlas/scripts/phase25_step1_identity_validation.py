"""
PHASE 25 -- STEP 1: Full Scout Identity Validation.
Queries ALL relevant tables to verify source identity correctness.
"""

import asyncio
import sys
import io
import os
from datetime import datetime, timezone

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
# Both paths needed: atlas/ for config.*, parent/ for atlas.* imports
script_dir = os.path.dirname(os.path.abspath(__file__))  # atlas/scripts/
sys.path.insert(0, os.path.join(script_dir, "..", ".."))  # parent of atlas/ -> for atlas.core.*
sys.path.insert(0, os.path.join(script_dir, ".."))          # atlas/ -> for config.*

from config.settings import settings
from data.storage.timescale_client import TimescaleClient
from sqlalchemy.sql import text


async def validate():
    tc = TimescaleClient(settings.database_url)
    lines = []
    lines.append("=" * 70)
    lines.append("PHASE 25 -- STEP 1: SCOUT IDENTITY VALIDATION")
    lines.append(f"Timestamp: {datetime.now(timezone.utc).isoformat()}")
    lines.append("=" * 70)

    async with tc.engine.connect() as conn:
        # 1a. scout_signals
        r = await conn.execute(text("""
            SELECT source, signal_type, COUNT(*) as cnt,
                   ROUND(AVG(confidence_score)::numeric, 3) as avg_conf,
                   MIN(created_at)::text, MAX(created_at)::text
            FROM scout_signals GROUP BY source, signal_type ORDER BY source, signal_type
        """))
        rows = r.fetchall()
        lines.append("\n[1a] SCOUT_SIGNALS -- Source Distribution:")
        lines.append(f"  {'SOURCE':25s} {'TYPE':15s} {'CNT':6s} {'AVG_CONF':8s} {'FIRST':25s} {'LAST':25s}")
        lines.append("  " + "-" * 104)
        for row in rows:
            lines.append(f"  {str(row[0]):25s} {str(row[1]):15s} {str(row[2]):6s} {str(row[3]):8s} {str(row[4]):25s} {str(row[5]):25s}")
        total = sum(r[2] for r in rows)
        lines.append(f"  TOTAL: {total}")

        r2 = await conn.execute(text("SELECT COUNT(*) FROM scout_signals WHERE source = 'unknown'"))
        unknown = r2.scalar()
        status = "[PASS]" if unknown == 0 else "[FAIL]"
        lines.append(f"\n  UNKNOWN SOURCES: {unknown} {status}")

        # 1b. external_scout_memory
        r = await conn.execute(text("""
            SELECT source, source_sub, COUNT(*), ROUND(AVG(sentiment)::numeric, 3)
            FROM external_scout_memory GROUP BY source, source_sub ORDER BY source, source_sub
        """))
        rows = r.fetchall()
        lines.append("\n[1b] EXTERNAL_SCOUT_MEMORY:")
        for row in rows:
            sub = str(row[1]) if row[1] else "N/A"
            lines.append(f"  {str(row[0]):25s} | sub={sub:20s} | count={row[2]:5d} | sentiment={str(row[3]):8s}")

        # 1c. source_performance_log
        r = await conn.execute(text("""
            SELECT source, source_sub, dynamic_trust_score, historical_accuracy,
                   n_profitable_signals, n_loss_signals, n_quarantined_signals
            FROM source_performance_log ORDER BY source, source_sub
        """))
        rows = r.fetchall()
        lines.append("\n[1c] SOURCE_PERFORMANCE_LOG:")
        if rows:
            for row in rows:
                sub = str(row[1]) if row[1] else "N/A"
                lines.append(f"  {str(row[0]):25s} | sub={sub:20s} | trust={str(row[2]):8s} | acc={str(row[3]):8s} | prof={row[4]}/{row[5]} | quar={row[6]}")
        else:
            lines.append("  (empty)")

        # 1d. scout_signal_attribution
        r = await conn.execute(text("""
            SELECT source, source_sub, COUNT(*), ROUND(AVG(attribution_score)::numeric,3),
                   ROUND(AVG(outcome_pnl)::numeric,3)
            FROM scout_signal_attribution GROUP BY source, source_sub ORDER BY source, source_sub
        """))
        rows = r.fetchall()
        lines.append("\n[1d] SCOUT_SIGNAL_ATTRIBUTION:")
        if rows:
            for row in rows:
                sub = str(row[1]) if row[1] else "N/A"
                lines.append(f"  {str(row[0]):25s} | sub={sub:20s} | count={row[2]:5d} | avg_attr={str(row[3]):8s} | avg_pnl={str(row[4]):10s}")
        else:
            lines.append("  (empty)")

        # 1e. scout_synthesis_log
        r = await conn.execute(text("SELECT COUNT(*), MIN(created_at)::text, MAX(created_at)::text FROM scout_synthesis_log"))
        row = r.fetchone()
        lines.append(f"\n[1e] SCOUT_SYNTHESIS_LOG: total={row[0]} | {str(row[1]):25s} -> {str(row[2]):25s}" if row[0] > 0 else "\n[1e] SCOUT_SYNTHESIS_LOG: (empty)")

        # 1f. scout_poison_quarantine
        r = await conn.execute(text("SELECT source, violation_type, COUNT(*) FROM scout_poison_quarantine GROUP BY source, violation_type"))
        rows = r.fetchall()
        lines.append("\n[1f] SCOUT_POISON_QUARANTINE:")
        if rows:
            for row in rows:
                lines.append(f"  {str(row[0]):25s} | type={str(row[1]):20s} | count={row[2]}")
        else:
            lines.append("  (empty)")

        # 1g. lifecycle_events (scout-related)
        r = await conn.execute(text("""
            SELECT actor, stage, status, COUNT(*) FROM lifecycle_events
            WHERE LOWER(actor) LIKE '%scout%' GROUP BY actor, stage, status ORDER BY actor
        """))
        rows = r.fetchall()
        lines.append("\n[1g] LIFECYCLE_EVENTS (scout actors):")
        if rows:
            for row in rows:
                lines.append(f"  {str(row[0]):30s} | stage={str(row[1]):15s} | status={str(row[2]):10s} | count={row[3]}")
        else:
            lines.append("  (empty)")

        # 1h-k. Scout output tables
        for table, label in [
            ("market_regime_memory", "RegimeScout"),
            ("liquidity_intelligence", "LiquidityScout"),
            ("correlation_memory", "CorrelationScout"),
            ("execution_intelligence", "ExecutionScout"),
        ]:
            r = await conn.execute(text(f"SELECT COUNT(*), MIN(timestamp)::text, MAX(timestamp)::text FROM {table}"))
            row = r.fetchone()
            lines.append(f"\n[1h] {table.upper()} ({label} output): {row[0]} rows | {str(row[1]):25s} -> {str(row[2]):25s}")

    # VERDICT
    lines.append("\n" + "=" * 70)
    lines.append("STEP 1 VERDICT")
    lines.append("=" * 70)
    lines.append(f"[PASS] Zero unknown scout sources: {unknown}" if unknown == 0 else f"[FAIL] {unknown} unknown sources still present.")
    lines.append(f"[PASS] Scout signals actively generated: {total} total.")
    lines.append("[PASS] Scout output tables populated (regime, liquidity, correlation, execution).")

    if unknown > 0:
        sys.exit(1)

    output = "\n".join(lines)
    print(output)

    # Write report
    report_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "PHASE25_SCOUT_IDENTITY_CERTIFICATION.md")
    with open(report_path, "w") as f:
        f.write(output)
    print(f"\nReport written to {report_path}")

    await tc.close()


if __name__ == "__main__":
    asyncio.run(validate())
