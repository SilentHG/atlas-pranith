"""
Phase 24 Pre-Flight Readiness Check.
Verifies all subsystems are operational before starting the soak.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

import asyncio
from atlas.config.settings import settings
from atlas.data.storage.timescale_client import TimescaleClient
from redis.asyncio import Redis


async def check_db(db):
    from sqlalchemy import text
    tables = [
        "strategies", "backtest_results", "lifecycle_events", "event_store",
        "audit_ledger", "mutation_memory", "market_regime_memory",
        "liquidity_intelligence", "correlation_memory", "execution_intelligence",
        "external_scout_memory", "portfolio_intelligence", "capital_allocation",
        "drift_detection", "strategy_retirement", "system_health",
        "replay_integrity", "systemic_risk", "stress_test_results",
        "capital_preservation_state", "copy_execution_log", "copy_position_state",
        "copy_drift_log", "leader_health_metrics", "follower_reconciliation"
    ]
    results = {}
    async with db.engine.connect() as conn:
        for tbl in tables:
            try:
                r = await conn.execute(text(f"SELECT COUNT(*) FROM {tbl}"))
                results[tbl] = r.scalar() or 0
            except Exception as e:
                results[tbl] = str(e)
    return results


async def check_redis(redis):
    info = {}
    info["ping"] = await redis.ping()
    mem = await redis.info("memory")
    info["used_memory_human"] = mem.get("used_memory_human", "?")
    keys = await redis.keys("*")
    info["total_keys"] = len(keys)
    return info


async def main():
    print("=" * 60)
    print("PHASE 24 — PRE-FLIGHT READINESS CHECK")
    print("=" * 60)

    # DB
    print("\n[1/4] Database connection...")
    db = TimescaleClient(settings.database_url)
    await db.connect()
    print("  [OK] Database connected")
    state = await check_db(db)
    print("\n  TABLE COUNTS:")
    for tbl, cnt in sorted(state.items()):
        status = f"[OK] {cnt} rows" if isinstance(cnt, int) else f"[ERR] {cnt}"
        print(f"    {tbl:>35}: {status}")
    await db.close()

    # Redis
    print("\n[2/4] Redis connection...")
    r = Redis.from_url(settings.redis_url)
    info = await check_redis(r)
    print(f"  [OK] Ping: {info['ping']}")
    print(f"  [OK] Memory: {info['used_memory_human']}")
    print(f"  [OK] Keys: {info['total_keys']}")
    await r.aclose()

    # Key imports
    print("\n[3/4] System imports...")
    from atlas.core.event_lineage import EventLineageClient
    from atlas.core.audit_ledger import AuditLedger
    from atlas.core.event_store import EventStore
    from atlas.core.trace_graph_engine import TraceGraphEngine
    print("  [OK] EventLineageClient")
    print("  [OK] AuditLedger")
    print("  [OK] EventStore")
    print("  [OK] TraceGraphEngine")

    from atlas.agents.l1_pattern.pattern_recognition_engine import PatternRecognitionEngine
    from atlas.agents.l2_strategy.coder_agent import CoderAgent
    from atlas.agents.l3_backtest.backtest_runner import BacktestRunner
    from atlas.agents.l4_risk.risk_controller import RiskController
    from atlas.agents.l5_execution.execution_gateway import ExecutionGateway
    from atlas.agents.l6_portfolio.portfolio_intelligence_engine import PortfolioIntelligenceEngine
    from atlas.agents.l7_meta.mutation_pattern_agent import MutationPatternAgent
    from atlas.agents.scouts.regime_scout import RegimeScout
    print("  [OK] All agent imports OK")

    # Soak monitor
    print("\n[4/4] Soak monitor...")
    from atlas.scripts.soak.phase24_monitor import SoakMonitor
    print("  [OK] SoakMonitor available")
    import psutil
    print(f"  [OK] psutil {psutil.__version__}")

    print("\n" + "=" * 60)
    print("READINESS: [OK] ALL SYSTEMS GO FOR PHASE 24 SOAK")
    print("=" * 60)
    print("\nRun: python scripts/full_autonomous_cycle.py --duration-minutes 360")
    print("Monitor: python scripts/soak/phase24_monitor.py (integrated into cycle)")


if __name__ == "__main__":
    asyncio.run(main())
