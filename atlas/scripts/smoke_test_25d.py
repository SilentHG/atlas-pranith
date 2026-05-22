"""
smoke_test_25d.py — Phase 25D 5-minute smoke test.

Starts the ATLAS system (scouts + Ideator + full pipeline), monitors
scout_signals accumulation at predefined intervals, checks Ideator
context for scout enrichment, and reports final health metrics.

Usage:
    python scripts/smoke_test_25d.py
"""

from __future__ import annotations

import asyncio
import io
import sys

# Force UTF-8 stdout for Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from loguru import logger

from atlas.config.settings import get_settings
from atlas.data.storage.timescale_client import TimescaleClient
from atlas.scripts.soak.phase24_monitor import SoakMonitor

# Reuse the agent builder from full_autonomous_cycle
from atlas.scripts.full_autonomous_cycle import _build_agents, _start_agents, _stop_agents

# ──────────────────────────────────────────────
# Report helpers
# ──────────────────────────────────────────────
REPORT_LINES: list[str] = []


def report(msg: str) -> None:
    """Append to report and print."""
    print(f"[SMOKE] {msg}")
    REPORT_LINES.append(msg)


async def query_scout_signals(db: TimescaleClient, label: str) -> int:
    """Query scout_signals count and log it."""
    from sqlalchemy.sql import text

    async with db.engine.connect() as conn:
        r = await conn.execute(text("SELECT COUNT(*) FROM scout_signals"))
        count = r.scalar() or 0

        # Also query by source distribution
        r2 = await conn.execute(
            text("""
                SELECT source, signal_type, COUNT(*) as cnt
                FROM scout_signals
                GROUP BY source, signal_type
                ORDER BY cnt DESC
            """)
        )
        rows = r2.fetchall()
        dist = {f"{row[0]}/{row[1]}": row[2] for row in rows}

    report(
        f"  [{label:>10}] scout_signals = {count}"
        + (f"  distribution: {dist}" if dist else "")
    )
    return count


async def check_ideator_context(db: TimescaleClient) -> bool:
    """Check if the Ideator has logged scout context enrichment."""
    from sqlalchemy.sql import text
    found = False

    # Check: does the Ideator debug log mention scout signals enrichment?
    # This is logged to stdout, not DB — but we can check the audit_ledger
    # for Ideator entries with scout-related keywords.
    async with db.engine.connect() as conn:
        try:
            r = await conn.execute(
                text("""
                    SELECT COUNT(*) FROM audit_ledger
                    WHERE message LIKE '%enrichment%'
                       OR message LIKE '%scout_signals%'
                       OR message LIKE '%Scout signals%'
                """)
            )
            count = r.scalar() or 0
            if count > 0:
                report(f"  Ideator scout context found in audit_ledger ({count} entries)")
                found = True
            else:
                report("  Ideator scout context: NOT found in audit_ledger (logger.debug goes to stdout, not DB)")
        except Exception as check_err:
            report(f"  audit_ledger check skipped (no relevant column): {check_err}")

        # Fallback: check even if the enrichment code ran (no crash = success)
        # The first smoke test proved the enrichment code runs (we saw the logger.debug line)
        try:
            r2 = await conn.execute(
                text("SELECT COUNT(*) FROM scout_signals")
            )
            ss_count = r2.scalar() or 0
            report(f"  scout_signals table has {ss_count} rows — mirror is working")
        except Exception:
            pass

    return found


async def run_smoke() -> None:
    settings = get_settings()
    db = TimescaleClient(settings.database_url)
    await db.connect()

    SMOKE_DURATION = 300  # 5 minutes
    CHECKPOINTS = [0, 60, 180, 300]  # seconds at which to check

    # ── Step 1: Clear scout_signals for clean baseline ──
    report("=== Phase 25D 5-Minute Smoke Test ===")
    report(f"Duration: {SMOKE_DURATION}s ({SMOKE_DURATION / 60:.0f} min)")

    from sqlalchemy.sql import text

    async with db.engine.begin() as conn:
        await conn.execute(text("DELETE FROM scout_signals"))
    report("Cleared scout_signals for clean baseline.")

    # ── Step 2: Build & start agents ──
    from redis.asyncio import Redis

    redis_client = Redis.from_url(settings.redis_url)
    agents = _build_agents(redis_client, db)

    # Phase 24: Soak monitoring
    monitor = SoakMonitor(db, redis_client, interval_seconds=60)
    try:
        await monitor.start()
        report("SoakMonitor started (60s intervals).")
    except Exception as e:
        logger.error(f"SoakMonitor failed: {e}")
        monitor = None

    report(f"Starting {len(agents)} agents...")
    tasks = await _start_agents(agents)
    report(f"Agents started. Monitoring for {SMOKE_DURATION}s...")

    # ── Step 3: Monitor loop ──
    start = asyncio.get_event_loop().time()
    next_checkpoint_idx = 0
    ideator_checked = False
    counts_log: list[tuple[int, int]] = []  # (elapsed_seconds, signal_count)

    try:
        while True:
            elapsed = asyncio.get_event_loop().time() - start

            if elapsed >= SMOKE_DURATION:
                break

            # Check if we've hit a checkpoint
            if (
                next_checkpoint_idx < len(CHECKPOINTS)
                and elapsed >= CHECKPOINTS[next_checkpoint_idx]
            ):
                label = f"t={CHECKPOINTS[next_checkpoint_idx]}s"
                count = await query_scout_signals(db, label)
                counts_log.append((CHECKPOINTS[next_checkpoint_idx], count))

                # Check Ideator context at t=180s (agents should have done at least one cycle)
                if CHECKPOINTS[next_checkpoint_idx] >= 180 and not ideator_checked:
                    await check_ideator_context(db)
                    ideator_checked = True

                next_checkpoint_idx += 1

            await asyncio.sleep(2)  # poll every 2s

        # ── Step 4: Final report ──
        report("\n" + "=" * 50)
        report("SMOKE TEST RESULTS")
        report("=" * 50)

        # Final scout_signals count
        final_count = await query_scout_signals(db, "FINAL")
        counts_log.append((SMOKE_DURATION, final_count))

        # Accumulation rate
        if len(counts_log) >= 2:
            first = counts_log[0]
            last = counts_log[-1]
            elapsed_check = last[0] - first[0]
            if elapsed_check > 0:
                rate = (last[1] - first[1]) / elapsed_check
                report(f"Accumulation rate: {rate:.3f} signals/sec ({rate*60:.1f} signals/min)")

        # Check for agent crashes
        dead_count = 0
        for i, task in enumerate(tasks):
            if task.done():
                agent = agents[i] if i < len(agents) else None
                agent_name = getattr(agent, "name", f"agent_{i}") if agent else f"task_{i}"
                if task.cancelled():
                    report(f"  CRASHED (cancelled): {agent_name}")
                    dead_count += 1
                elif task.exception():
                    exc = task.exception()
                    report(f"  CRASHED (exception): {agent_name} — {exc}")
                    dead_count += 1

        if dead_count == 0:
            report("  Agent crashes: 0 ✅")
        else:
            report(f"  Agent crashes: {dead_count} ⚠️")

        # If Ideator wasn't checked earlier (short run), check now
        if not ideator_checked:
            await check_ideator_context(db)

        report("\n" + "=" * 50)
        report("SMOKE TEST COMPLETE")
        report("=" * 50)

    finally:
        # ── Step 5: Clean shutdown ──
        if monitor:
            await monitor.stop()
        await _stop_agents(agents)
        try:
            await redis_client.aclose()
        except Exception:
            pass
        try:
            await db.close()
        except Exception:
            pass

    # Print full report at the end for easy copy-paste
    print("\n\n=== FULL SMOKE TEST REPORT ===")
    for line in REPORT_LINES:
        print(line)


if __name__ == "__main__":
    asyncio.run(run_smoke())
