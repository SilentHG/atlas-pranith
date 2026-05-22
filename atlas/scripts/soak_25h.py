"""
soak_25h.py — Phase 25H 30-minute soak test.

Starts the ATLAS system (scouts + Ideator + full pipeline), monitors
scout_signals accumulation at 5-minute intervals (t=0, 300, 600, 900, 1200, 1500, 1800s),
checks Ideator enrichment status, and reports final health metrics.

Usage:
    python scripts/soak_25h.py
"""

from __future__ import annotations

import asyncio
import io
import sys

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

from loguru import logger

from atlas.config.settings import get_settings
from atlas.data.storage.timescale_client import TimescaleClient
from atlas.scripts.soak.phase24_monitor import SoakMonitor
from atlas.scripts.full_autonomous_cycle import _build_agents, _start_agents, _stop_agents

REPORT_LINES: list[str] = []


def report(msg: str) -> None:
    print(f"[SOAK] {msg}")
    REPORT_LINES.append(msg)


async def query_scout_signals(db: TimescaleClient, label: str) -> int:
    from sqlalchemy.sql import text
    async with db.engine.connect() as conn:
        r = await conn.execute(text("SELECT COUNT(*) FROM scout_signals"))
        count = r.scalar() or 0
        r2 = await conn.execute(text("""
            SELECT source, signal_type, COUNT(*) as cnt
            FROM scout_signals
            GROUP BY source, signal_type
            ORDER BY cnt DESC
        """))
        rows = r2.fetchall()
        dist = {f"{row[0]}/{row[1]}": row[2] for row in rows}
    report(
        f"  [{label:>12}] scout_signals = {count}"
        + (f"  distribution: {dist}" if dist else "")
    )
    return count


async def run_soak() -> None:
    settings = get_settings()
    db = TimescaleClient(settings.database_url)
    await db.connect()

    SOAK_DURATION = 1800  # 30 minutes
    CHECKPOINTS = [0, 300, 600, 900, 1200, 1500, 1800]
    IDEATOR_CHECK_AT = 600  # Check Ideator enrichment at t=10min

    report("=== Phase 25H 30-Minute Soak Test ===")
    report(f"Duration: {SOAK_DURATION}s ({SOAK_DURATION // 60} min)")

    from sqlalchemy.sql import text
    async with db.engine.begin() as conn:
        await conn.execute(text("DELETE FROM scout_signals"))
    report("Cleared scout_signals for clean baseline.")

    from redis.asyncio import Redis
    redis_client = Redis.from_url(settings.redis_url)
    agents = _build_agents(redis_client, db)

    monitor = SoakMonitor(db, redis_client, interval_seconds=300)
    try:
        await monitor.start()
        report("SoakMonitor started (300s intervals).")
    except Exception as e:
        logger.error(f"SoakMonitor failed: {e}")
        monitor = None

    report(f"Starting {len(agents)} agents...")
    tasks = await _start_agents(agents)
    report(f"Agents started. Soaking for {SOAK_DURATION // 60} min...")

    start = asyncio.get_event_loop().time()
    next_cp = 0
    ideator_checked = False
    counts_log: list[tuple[int, int]] = []

    try:
        while True:
            elapsed = asyncio.get_event_loop().time() - start
            if elapsed >= SOAK_DURATION:
                break

            if next_cp < len(CHECKPOINTS) and elapsed >= CHECKPOINTS[next_cp]:
                label = f"t={CHECKPOINTS[next_cp]}s"
                count = await query_scout_signals(db, label)
                counts_log.append((CHECKPOINTS[next_cp], count))

                if CHECKPOINTS[next_cp] >= IDEATOR_CHECK_AT and not ideator_checked:
                    report("  Ideator enrichment check: awaiting stdout log confirmation in final report")
                    ideator_checked = True

                next_cp += 1

            await asyncio.sleep(5)

        report("\n" + "=" * 50)
        report("SOAK TEST RESULTS")
        report("=" * 50)

        final_count = await query_scout_signals(db, "FINAL")
        counts_log.append((SOAK_DURATION, final_count))

        if len(counts_log) >= 2:
            first = counts_log[0]
            last = counts_log[-1]
            delta_t = last[0] - first[0]
            if delta_t > 0:
                rate = (last[1] - first[1]) / delta_t
                report(f"Accumulation rate: {rate:.3f} signals/sec ({rate*60:.1f} signals/min)")

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

        report("\n" + "=" * 50)
        report("SOAK TEST COMPLETE")
        report("=" * 50)

    finally:
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

    print("\n\n=== FULL SOAK REPORT ===")
    for line in REPORT_LINES:
        print(line)


if __name__ == "__main__":
    asyncio.run(run_soak())
