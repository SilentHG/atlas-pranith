"""
monitoring_fabric.py — Real-time distributed monitoring fabric.

Capabilities:
- Distributed metrics collection and aggregation
- Event throughput tracking (execution, scout, mutation, replay)
- Execution latency monitoring
- System-wide metric point-in-time snapshots
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text


class MonitoringFabric:
    """
    Real-time monitoring fabric for distributed metrics across all ATLAS subsystems.

    Provides:
    - Centralized metric collection
    - Throughput tracking
    - Latency monitoring
    - Metric snapshots to DB
    """

    def __init__(self, db, redis_client):
        self.db = db
        self.redis = redis_client
        self._counters: dict[str, int] = defaultdict(int)
        self._latencies: dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=1000))
        self._lock = asyncio.Lock()
        self._flush_interval = 60  # Flush to DB every 60 seconds
        self._flush_task: Optional[asyncio.Task] = None

    async def start(self):
        """Start the periodic flush loop."""
        self._flush_task = asyncio.create_task(self._periodic_flush())
        logger.info("MonitoringFabric started")

    async def stop(self):
        """Stop the flush loop and flush remaining metrics."""
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        await self.flush()
        logger.info("MonitoringFabric stopped")

    async def increment(self, metric: str, value: int = 1):
        """Increment a counter metric."""
        async with self._lock:
            self._counters[metric] += value

            # Also update Redis for real-time visibility
            await self.redis.hincrby("metrics:counters", metric, value)

    async def record_latency(self, metric: str, latency_ms: float):
        """Record a latency measurement."""
        async with self._lock:
            self._latencies[metric].append(latency_ms)

            # P50/P95/P99 in Redis
            key = f"metrics:latency:{metric}"
            await self.redis.rpush(key, str(latency_ms))
            await self.redis.ltrim(key, 0, 999)  # Keep last 1000

    async def flush(self):
        """Flush accumulated metrics to the database."""
        async with self._lock:
            if not self._counters and not self._latencies:
                return

            now = datetime.now(timezone.utc)
            snapshot = {
                "timestamp": now.isoformat(),
                "counters": dict(self._counters),
                "latencies": {},
            }

            for metric, values in self._latencies.items():
                if values:
                    sample_values = list(values)
                    snapshot["latencies"][metric] = {
                        "count": len(sample_values),
                        "min": round(min(sample_values), 2),
                        "max": round(max(sample_values), 2),
                        "avg": round(sum(sample_values) / len(sample_values), 2),
                        "p50": round(sorted(sample_values)[len(sample_values) // 2], 2),
                        "p95": round(sorted(sample_values)[int(len(sample_values) * 0.95)], 2),
                    }

            # Persist to DB
            await self.db._execute_insert(
                """
                INSERT INTO monitoring_metrics
                    (id, recorded_at, counters, latencies)
                VALUES
                    (:id, :recorded_at, CAST(:counters AS jsonb), CAST(:latencies AS jsonb))
                """,
                {
                    "id": uuid.uuid4().hex[:16],
                    "recorded_at": now,
                    "counters": json.dumps(snapshot["counters"]),
                    "latencies": json.dumps(snapshot["latencies"]),
                },
            )

            # Reset
            self._counters.clear()
            self._latencies.clear()

            logger.debug(f"MonitoringFabric flushed: {len(snapshot['counters'])} counters")

    async def _periodic_flush(self):
        """Periodically flush metrics to DB."""
        try:
            while True:
                await asyncio.sleep(self._flush_interval)
                await self.flush()
        except asyncio.CancelledError:
            pass

    async def get_throughput(
        self,
        hours: int = 1,
    ) -> dict:
        """Get throughput metrics for the time window."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT counters, recorded_at
                    FROM monitoring_metrics
                    WHERE recorded_at > NOW() - INTERVAL ':hours hours'
                    ORDER BY recorded_at ASC
                """),
                {"hours": hours},
            )
            rows = r.fetchall()

        if not rows:
            return {}

        # Aggregate counters
        total_counters: dict = {}
        for row in rows:
            counters = row[0]
            if isinstance(counters, str):
                try:
                    counters = json.loads(counters)
                except Exception:
                    counters = {}
            for k, v in counters.items():
                total_counters[k] = total_counters.get(k, 0) + v

        return {
            "counters": total_counters,
            "n_snapshots": len(rows),
            "time_range_hours": hours,
        }

    async def get_latency_summary(
        self,
        hours: int = 1,
    ) -> dict:
        """Get latency summary for the time window."""
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT latencies
                    FROM monitoring_metrics
                    WHERE recorded_at > NOW() - INTERVAL ':hours hours'
                """),
                {"hours": hours},
            )
            rows = r.fetchall()

        if not rows:
            return {}

        all_latencies: dict = {}
        for row in rows:
            latencies = row[0]
            if isinstance(latencies, str):
                try:
                    latencies = json.loads(latencies)
                except Exception:
                    latencies = {}
            for metric, stats in latencies.items():
                if metric not in all_latencies:
                    all_latencies[metric] = []
                all_latencies[metric].append(stats)

        # Average across snapshots
        summary = {}
        for metric, snapshots in all_latencies.items():
            if snapshots:
                summary[metric] = {
                    "avg_avg": round(
                        sum(s["avg"] for s in snapshots) / len(snapshots), 2
                    ),
                    "avg_p95": round(
                        sum(s["p95"] for s in snapshots) / len(snapshots), 2
                    ),
                    "avg_p99": round(
                        sum(
                            s.get("p99", s["p95"] * 1.2) for s in snapshots
                        )
                        / len(snapshots),
                        2,
                    ),
                    "max_max": max(s["max"] for s in snapshots),
                    "n_snapshots": len(snapshots),
                }

        return summary
