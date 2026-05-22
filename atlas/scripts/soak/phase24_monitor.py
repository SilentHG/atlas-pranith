"""
phase24_monitor.py — Phase 24 Soak Test Monitoring Harness

Captures operational metrics every 5 minutes across ALL dimensions:
- Infrastructure (RAM, CPU, Redis, DB, event-loop lag, threads)
- Async/Lifecycle (orphan tasks, dead agents, restart counts, queues, leases)
- Replay (lineage gaps, orphan traces, hash integrity, ordering)
- Execution (slippage, latency, duplicates, stale execution)
- Copy Trading (follower drift, sync quality, degraded followers)
- Scout Network (trust drift, entropy, stale intel, poisoning)
- Mutation Health (entropy, archetype diversity, mortality, clone saturation)
- Portfolio Health (concentration, exposure clustering, allocation drift)
- Meta-Layer (hypothesis decay, reasoning stability, drift escalation)
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from loguru import logger

# Try psutil — graceful fallback if not installed
try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False
    logger.warning("psutil not installed — infrastructure metrics will be limited")


class SoakMonitor:
    """Captures all Phase 24 monitoring dimensions every interval."""

    def __init__(
        self,
        db_client,
        redis_client,
        interval_seconds: int = 300,  # 5 minutes
        output_path: Optional[Path] = None,
    ):
        self.db = db_client
        self.redis = redis_client
        self.interval = interval_seconds
        self.output_path = output_path or Path(__file__).resolve().parent / "phase24_metrics.jsonl"
        self._snapshots: list[dict] = []
        self._restart_counts: dict[str, int] = defaultdict(int)
        self._start_time: float = 0
        self._task: Optional[asyncio.Task] = None
        self._process = psutil.Process(os.getpid()) if HAS_PSUTIL else None

    async def start(self):
        """Start the monitoring loop."""
        self._start_time = time.time()
        self._task = asyncio.create_task(self._monitor_loop())
        logger.info(f"SoakMonitor started — interval={self.interval}s, output={self.output_path}")

    async def stop(self):
        """Stop the monitoring loop."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(f"SoakMonitor stopped — {len(self._snapshots)} snapshots captured")

    def record_restart(self, agent_name: str):
        """Record an agent restart event."""
        self._restart_counts[agent_name] += 1

    def get_snapshots(self) -> list[dict]:
        """Return all captured snapshots."""
        return self._snapshots

    async def _monitor_loop(self):
        """Main monitoring loop — captures metrics every interval."""
        try:
            while True:
                try:
                    snapshot = await self._capture_snapshot()
                    self._snapshots.append(snapshot)
                    self._persist_snapshot(snapshot)
                    
                    elapsed_min = (time.time() - self._start_time) / 60
                    logger.info(
                        f"[SOAK MONITOR] t={elapsed_min:.0f}m | "
                        f"RAM={snapshot.get('infrastructure', {}).get('ram_mb', '?')}MB | "
                        f"CPU={snapshot.get('infrastructure', {}).get('cpu_pct', '?')}% | "
                        f"tasks={snapshot.get('async_lifecycle', {}).get('total_tasks', '?')} | "
                        f"threads={snapshot.get('infrastructure', {}).get('thread_count', '?')}"
                    )
                except Exception as e:
                    logger.error(f"[SOAK MONITOR] Snapshot capture failed: {e}")

                await asyncio.sleep(self.interval)
        except asyncio.CancelledError:
            pass

    async def _capture_snapshot(self) -> dict:
        """Capture a full monitoring snapshot across all dimensions."""
        now = datetime.now(timezone.utc)
        elapsed = time.time() - self._start_time

        snapshot = {
            "timestamp": now.isoformat(),
            "elapsed_seconds": round(elapsed, 1),
            "elapsed_minutes": round(elapsed / 60, 1),
            "infrastructure": await self._capture_infrastructure(),
            "async_lifecycle": self._capture_async_lifecycle(),
            "replay": await self._capture_replay_metrics(),
            "execution": await self._capture_execution_metrics(),
            "copy_trading": await self._capture_copy_trading_metrics(),
            "scout_network": await self._capture_scout_metrics(),
            "mutation_health": await self._capture_mutation_metrics(),
            "portfolio_health": await self._capture_portfolio_metrics(),
            "meta_layer": await self._capture_meta_metrics(),
            "restart_counts": dict(self._restart_counts),
        }
        return snapshot

    # ─────────────────────────────────────────────
    # Infrastructure
    # ─────────────────────────────────────────────

    async def _capture_infrastructure(self) -> dict:
        """RAM, CPU, Redis memory, DB connections, event-loop lag, threads."""
        infra = {}

        # RAM & CPU via psutil
        if self._process:
            try:
                mem = self._process.memory_info()
                infra["ram_mb"] = round(mem.rss / (1024 * 1024), 1)
                infra["ram_vms_mb"] = round(mem.vms / (1024 * 1024), 1)
                infra["cpu_pct"] = self._process.cpu_percent(interval=0.1)
                infra["thread_count"] = self._process.num_threads()
            except Exception as e:
                infra["psutil_error"] = str(e)
        else:
            infra["thread_count"] = threading.active_count()

        # Event-loop lag
        loop = asyncio.get_event_loop()
        t0 = loop.time()
        await asyncio.sleep(0)
        lag = (loop.time() - t0) * 1000
        infra["event_loop_lag_ms"] = round(lag, 2)

        # Redis memory
        try:
            info = await self.redis.info("memory")
            infra["redis_used_memory_mb"] = round(info.get("used_memory", 0) / (1024 * 1024), 1)
            infra["redis_peak_memory_mb"] = round(info.get("used_memory_peak", 0) / (1024 * 1024), 1)
            infra["redis_connected_clients"] = (await self.redis.info("clients")).get("connected_clients", 0)
        except Exception as e:
            infra["redis_error"] = str(e)

        # DB connection pool
        try:
            pool = self.db.engine.pool
            infra["db_pool_size"] = pool.size()
            infra["db_checked_out"] = pool.checkedout()
            infra["db_overflow"] = pool.overflow()
            infra["db_checked_in"] = pool.checkedin()
        except Exception as e:
            infra["db_pool_error"] = str(e)

        return infra

    # ─────────────────────────────────────────────
    # Async / Lifecycle
    # ─────────────────────────────────────────────

    def _capture_async_lifecycle(self) -> dict:
        """Orphan tasks, total tasks, thread growth."""
        try:
            all_tasks = asyncio.all_tasks()
            return {
                "total_tasks": len(all_tasks),
                "total_threads": threading.active_count(),
                "total_restarts": sum(self._restart_counts.values()),
                "agents_restarted": len(self._restart_counts),
            }
        except Exception as e:
            return {"error": str(e)}

    # ─────────────────────────────────────────────
    # Replay
    # ─────────────────────────────────────────────

    async def _capture_replay_metrics(self) -> dict:
        """Lineage gaps, orphan traces, hash integrity, ordering."""
        metrics = {}
        try:
            from sqlalchemy.sql import text
            async with self.db.engine.connect() as conn:
                # Total events in event store
                r = await conn.execute(text("SELECT COUNT(*) FROM event_store"))
                metrics["total_events"] = r.scalar() or 0

                # Recent events (last hour)
                r = await conn.execute(text(
                    "SELECT COUNT(*) FROM event_store WHERE created_at > NOW() - INTERVAL '1 hour'"
                ))
                metrics["events_last_hour"] = r.scalar() or 0

                # Orphan events (no aggregate_id)
                r = await conn.execute(text(
                    "SELECT COUNT(*) FROM event_store WHERE aggregate_id IS NULL"
                ))
                metrics["orphan_events"] = r.scalar() or 0

                # Lifecycle events count
                r = await conn.execute(text("SELECT COUNT(*) FROM lifecycle_events"))
                metrics["total_lifecycle_events"] = r.scalar() or 0

                # Replay integrity (latest check)
                try:
                    r = await conn.execute(text(
                        "SELECT integrity_score, n_violations FROM replay_integrity ORDER BY checked_at DESC LIMIT 1"
                    ))
                    row = r.fetchone()
                    if row:
                        metrics["replay_integrity_score"] = float(row[0]) if row[0] else None
                        metrics["replay_violations"] = row[1] or 0
                except Exception:
                    pass  # Table may not exist yet

        except Exception as e:
            metrics["error"] = str(e)
        return metrics

    # ─────────────────────────────────────────────
    # Execution
    # ─────────────────────────────────────────────

    async def _capture_execution_metrics(self) -> dict:
        """Slippage, latency, duplicates, stale execution."""
        metrics = {}
        try:
            from sqlalchemy.sql import text
            async with self.db.engine.connect() as conn:
                # Recent execution intelligence
                r = await conn.execute(text("""
                    SELECT AVG(avg_slippage_bps), AVG(fill_latency_ms),
                           AVG(fill_quality_score), COUNT(*)
                    FROM execution_intelligence
                    WHERE timestamp > NOW() - INTERVAL '1 hour'
                """))
                row = r.fetchone()
                if row:
                    metrics["avg_slippage_bps"] = round(float(row[0] or 0), 2)
                    metrics["avg_fill_latency_ms"] = round(float(row[1] or 0), 1)
                    metrics["avg_fill_quality"] = round(float(row[2] or 0), 3)
                    metrics["recent_executions"] = row[3] or 0

                # Check for duplicate executions
                try:
                    r = await conn.execute(text("""
                        SELECT COUNT(*) FROM (
                            SELECT leader_order_id, follower_id, COUNT(*) as cnt
                            FROM copy_execution_log
                            WHERE created_at > NOW() - INTERVAL '1 hour'
                            GROUP BY leader_order_id, follower_id
                            HAVING COUNT(*) > 1
                        ) dupes
                    """))
                    metrics["duplicate_executions"] = r.scalar() or 0
                except Exception:
                    metrics["duplicate_executions"] = 0

        except Exception as e:
            metrics["error"] = str(e)
        return metrics

    # ─────────────────────────────────────────────
    # Copy Trading
    # ─────────────────────────────────────────────

    async def _capture_copy_trading_metrics(self) -> dict:
        """Follower drift, sync quality, degraded followers."""
        metrics = {}
        try:
            from sqlalchemy.sql import text
            async with self.db.engine.connect() as conn:
                # Copy execution log stats
                r = await conn.execute(text("""
                    SELECT status, COUNT(*) FROM copy_execution_log
                    WHERE created_at > NOW() - INTERVAL '1 hour'
                    GROUP BY status
                """))
                metrics["copy_status_distribution"] = {row[0]: row[1] for row in r.fetchall()}

                # Average copy latency
                r = await conn.execute(text("""
                    SELECT AVG(latency_ms), MAX(latency_ms)
                    FROM copy_execution_log
                    WHERE created_at > NOW() - INTERVAL '1 hour' AND latency_ms IS NOT NULL
                """))
                row = r.fetchone()
                if row:
                    metrics["avg_copy_latency_ms"] = round(float(row[0] or 0), 1)
                    metrics["max_copy_latency_ms"] = round(float(row[1] or 0), 1)

                # Check for degraded followers via Redis
                try:
                    cursor = 0
                    degraded = 0
                    while True:
                        cursor, keys = await self.redis.scan(cursor=cursor, match="copy_failover:*:mode", count=100)
                        for key in keys:
                            mode = await self.redis.get(key)
                            if mode and mode.decode() != "normal":
                                degraded += 1
                        if cursor == 0:
                            break
                    metrics["degraded_followers"] = degraded
                except Exception:
                    metrics["degraded_followers"] = 0

        except Exception as e:
            metrics["error"] = str(e)
        return metrics

    # ─────────────────────────────────────────────
    # Scout Network
    # ─────────────────────────────────────────────

    async def _capture_scout_metrics(self) -> dict:
        """Trust drift, entropy, stale intel, poisoning."""
        metrics = {}
        try:
            from sqlalchemy.sql import text
            async with self.db.engine.connect() as conn:
                # Scout signal counts by source
                r = await conn.execute(text("""
                    SELECT source, COUNT(*), AVG(sentiment), STDDEV(sentiment)
                    FROM external_scout_memory
                    WHERE timestamp > NOW() - INTERVAL '1 hour'
                    GROUP BY source
                """))
                source_stats = {}
                for row in r.fetchall():
                    source_stats[row[0]] = {
                        "count": row[1],
                        "avg_sentiment": round(float(row[2] or 0), 3),
                        "std_sentiment": round(float(row[3] or 0), 3),
                    }
                metrics["source_stats"] = source_stats

                # Calculate disagreement entropy
                if len(source_stats) >= 2:
                    import numpy as np
                    sentiments = [s["avg_sentiment"] for s in source_stats.values()]
                    shifted = np.array([s + 1.01 for s in sentiments])
                    probs = shifted / np.sum(shifted)
                    entropy = -np.sum(probs * np.log2(probs))
                    max_entropy = np.log2(len(probs)) if len(probs) > 1 else 1.0
                    metrics["disagreement_entropy"] = round(float(entropy / max_entropy), 3)
                else:
                    metrics["disagreement_entropy"] = 0.0

                # Stale intelligence (signals older than 6 hours still referenced)
                r = await conn.execute(text("""
                    SELECT COUNT(*) FROM external_scout_memory
                    WHERE timestamp < NOW() - INTERVAL '6 hours'
                      AND timestamp > NOW() - INTERVAL '7 hours'
                """))
                metrics["stale_signals_6h"] = r.scalar() or 0

                # Quarantined signals (anti-poisoning)
                try:
                    r = await conn.execute(text("""
                        SELECT COUNT(*) FROM scout_quarantine
                        WHERE quarantined_at > NOW() - INTERVAL '1 hour'
                    """))
                    metrics["quarantined_last_hour"] = r.scalar() or 0
                except Exception:
                    metrics["quarantined_last_hour"] = 0

                # Regime scout data freshness
                r = await conn.execute(text("""
                    SELECT MAX(timestamp) FROM market_regime_memory
                """))
                row = r.fetchone()
                if row and row[0]:
                    metrics["regime_scout_last_update"] = row[0].isoformat()

        except Exception as e:
            metrics["error"] = str(e)
        return metrics

    # ─────────────────────────────────────────────
    # Mutation Health
    # ─────────────────────────────────────────────

    async def _capture_mutation_metrics(self) -> dict:
        """Entropy, archetype diversity, mortality, clone saturation."""
        metrics = {}
        try:
            from sqlalchemy.sql import text
            async with self.db.engine.connect() as conn:
                # Mutation type distribution
                r = await conn.execute(text("""
                    SELECT mutation_type, COUNT(*),
                           COUNT(*) FILTER (WHERE improved = TRUE),
                           AVG(score_delta)
                    FROM mutation_memory
                    WHERE created_at > NOW() - INTERVAL '6 hours'
                    GROUP BY mutation_type
                """))
                mutation_types = {}
                for row in r.fetchall():
                    mutation_types[row[0]] = {
                        "count": row[1],
                        "improved": row[2],
                        "avg_delta": round(float(row[3] or 0), 4),
                    }
                metrics["mutation_types"] = mutation_types
                metrics["n_mutation_types"] = len(mutation_types)

                # Mutation entropy
                if mutation_types:
                    import numpy as np
                    counts = np.array([m["count"] for m in mutation_types.values()], dtype=float)
                    if np.sum(counts) > 0:
                        probs = counts / np.sum(counts)
                        entropy = -np.sum(probs * np.log2(probs + 1e-10))
                        max_entropy = np.log2(len(probs)) if len(probs) > 1 else 1.0
                        metrics["mutation_entropy"] = round(float(entropy / max_entropy), 3)
                    else:
                        metrics["mutation_entropy"] = 0.0

                # Strategy status distribution (mortality)
                r = await conn.execute(text("""
                    SELECT status, COUNT(*) FROM strategies
                    GROUP BY status
                """))
                metrics["strategy_status"] = {row[0]: row[1] for row in r.fetchall()}

                # Archetype diversity
                r = await conn.execute(text("""
                    SELECT COUNT(DISTINCT archetype) FROM pattern_memory
                """))
                metrics["archetype_diversity"] = r.scalar() or 0

        except Exception as e:
            metrics["error"] = str(e)
        return metrics

    # ─────────────────────────────────────────────
    # Portfolio Health
    # ─────────────────────────────────────────────

    async def _capture_portfolio_metrics(self) -> dict:
        """Concentration, exposure clustering, allocation drift."""
        metrics = {}
        try:
            from sqlalchemy.sql import text
            async with self.db.engine.connect() as conn:
                # Latest portfolio intelligence
                r = await conn.execute(text("""
                    SELECT concentration_risk, diversification_score,
                           ensemble_survivability_score, n_strategies,
                           computed_at
                    FROM portfolio_intelligence
                    ORDER BY computed_at DESC LIMIT 1
                """))
                row = r.fetchone()
                if row:
                    metrics["concentration_risk"] = round(float(row[0] or 0), 3)
                    metrics["diversification_score"] = round(float(row[1] or 0), 3)
                    metrics["ensemble_survivability"] = round(float(row[2] or 0), 3)
                    metrics["n_strategies_in_portfolio"] = row[3] or 0
                    metrics["last_portfolio_update"] = row[4].isoformat() if row[4] else None

                # Latest capital allocation
                r = await conn.execute(text("""
                    SELECT total_exposure, leverage_cap_applied, method,
                           n_strategies, computed_at
                    FROM capital_allocation
                    ORDER BY computed_at DESC LIMIT 1
                """))
                row = r.fetchone()
                if row:
                    metrics["total_exposure"] = round(float(row[0] or 0), 2)
                    metrics["leverage_cap"] = round(float(row[1] or 0), 2)
                    metrics["allocation_method"] = row[2]

                # Drift composite
                r = await conn.execute(text("""
                    SELECT composite_severity, detected_at
                    FROM drift_detection
                    ORDER BY detected_at DESC LIMIT 1
                """))
                row = r.fetchone()
                if row:
                    metrics["drift_composite"] = round(float(row[0] or 0), 3)

        except Exception as e:
            metrics["error"] = str(e)
        return metrics

    # ─────────────────────────────────────────────
    # Meta-Layer
    # ─────────────────────────────────────────────

    async def _capture_meta_metrics(self) -> dict:
        """Hypothesis decay, reasoning stability, drift escalation."""
        metrics = {}
        try:
            from sqlalchemy.sql import text
            async with self.db.engine.connect() as conn:
                # Hypothesis registry
                try:
                    r = await conn.execute(text("""
                        SELECT status, COUNT(*), AVG(confidence)
                        FROM hypothesis_registry
                        GROUP BY status
                    """))
                    hypothesis_status = {}
                    for row in r.fetchall():
                        hypothesis_status[row[0]] = {
                            "count": row[1],
                            "avg_confidence": round(float(row[2] or 0), 3),
                        }
                    metrics["hypothesis_status"] = hypothesis_status
                except Exception:
                    metrics["hypothesis_status"] = {}

                # Meta reasoning log
                try:
                    r = await conn.execute(text("""
                        SELECT COUNT(*), AVG(confidence)
                        FROM meta_reasoning_log
                        WHERE created_at > NOW() - INTERVAL '1 hour'
                    """))
                    row = r.fetchone()
                    if row:
                        metrics["meta_advisories_last_hour"] = row[0] or 0
                        metrics["avg_advisory_confidence"] = round(float(row[1] or 0), 3)
                except Exception:
                    pass

                # Scout synthesis log
                try:
                    r = await conn.execute(text("""
                        SELECT COUNT(*), AVG(confidence)
                        FROM scout_synthesis_log
                        WHERE created_at > NOW() - INTERVAL '1 hour'
                    """))
                    row = r.fetchone()
                    if row:
                        metrics["scout_syntheses_last_hour"] = row[0] or 0
                except Exception:
                    pass

        except Exception as e:
            metrics["error"] = str(e)
        return metrics

    # ─────────────────────────────────────────────
    # Persistence
    # ─────────────────────────────────────────────

    def _persist_snapshot(self, snapshot: dict):
        """Append snapshot to JSONL file."""
        try:
            with open(self.output_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(snapshot, default=str) + "\n")
        except Exception as e:
            logger.error(f"[SOAK MONITOR] Failed to persist snapshot: {e}")
