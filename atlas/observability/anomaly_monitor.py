"""
anomaly_monitor.py — Anomaly detection observability for ATLAS subsystems.

Detects:
- Abnormal strategy generation patterns
- Abnormal execution behavior
- Abnormal scout behavior
- Abnormal drift escalation
- Abnormal retirement clustering
"""

from __future__ import annotations

import json
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Optional

from loguru import logger
from sqlalchemy.sql import text


class AnomalyMonitor:
    """
    Observability agent that detects anomalous behavior patterns across all subsystems.
    """

    def __init__(self, db, redis_client):
        self.db = db
        self.redis = redis_client

    async def run_check(self) -> dict:
        """Run a comprehensive anomaly check across all subsystems."""
        anomalies = []

        strategy_anomaly = await self._check_strategy_anomalies()
        if strategy_anomaly:
            anomalies.extend(strategy_anomaly)

        execution_anomaly = await self._check_execution_anomalies()
        if execution_anomaly:
            anomalies.extend(execution_anomaly)

        scout_anomaly = await self._check_scout_anomalies()
        if scout_anomaly:
            anomalies.extend(scout_anomaly)

        drift_anomaly = await self._check_drift_anomalies()
        if drift_anomaly:
            anomalies.extend(drift_anomaly)

        retirement_anomaly = await self._check_retirement_anomalies()
        if retirement_anomaly:
            anomalies.extend(retirement_anomaly)

        # Persist anomalies
        if anomalies:
            await self.db._execute_insert(
                """
                INSERT INTO anomaly_observations
                    (id, observed_at, n_anomalies, anomalies, severity)
                VALUES
                    (:id, NOW(), :n_anomalies, CAST(:anomalies AS jsonb), :severity)
                """,
                {
                    "id": uuid.uuid4().hex[:16],
                    "n_anomalies": len(anomalies),
                    "anomalies": json.dumps(anomalies),
                    "severity": max(a.get("severity", 0) for a in anomalies),
                },
            )

        return {
            "n_anomalies": len(anomalies),
            "anomalies": anomalies,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }

    async def _check_strategy_anomalies(self) -> list[dict]:
        """Detect abnormal strategy generation patterns."""
        anomalies = []
        async with self.db.engine.connect() as conn:
            # Sudden spike in strategy generation
            r = await conn.execute(
                text("""
                    SELECT
                        COUNT(*) as total_last_hour,
                        AVG(COUNT(*)) OVER () as avg_hourly
                    FROM strategies
                    WHERE created_at > NOW() - INTERVAL '1 hour'
                    GROUP BY DATE_TRUNC('hour', created_at)
                    LIMIT 1
                """)
            )
            row = r.fetchone()
            if row:
                count = row[0]
                avg = float(row[1] or count)

                if count > avg * 3 and count > 20:
                    anomalies.append({
                        "type": "strategy_generation_spike",
                        "severity": 0.6,
                        "detail": f"Strategy generation spike: {count} in last hour (avg={avg:.0f})",
                        "value": count,
                        "threshold": avg * 3,
                    })

        return anomalies

    async def _check_execution_anomalies(self) -> list[dict]:
        """Detect abnormal execution behavior."""
        anomalies = []
        async with self.db.engine.connect() as conn:
            # Unusual failure rate
            r = await conn.execute(
                text("""
                    SELECT
                        COUNT(*) FILTER (WHERE state LIKE '%FAILED%' OR state LIKE '%ERROR%') as failures,
                        COUNT(*) as total
                    FROM execution_log
                    WHERE created_at > NOW() - INTERVAL '1 hour'
                """)
            )
            row = r.fetchone()
            if row:
                failures = row[0] or 0
                total = row[1] or 1
                failure_rate = failures / total

                if failure_rate > 0.3:
                    anomalies.append({
                        "type": "execution_failure_rate",
                        "severity": min(1.0, failure_rate),
                        "detail": f"Execution failure rate: {failure_rate:.1%} ({failures}/{total})",
                        "value": failure_rate,
                        "threshold": 0.3,
                    })

        return anomalies

    async def _check_scout_anomalies(self) -> list[dict]:
        """Detect abnormal scout behavior."""
        anomalies = []
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT source, COUNT(*) as cnt
                    FROM external_scout_memory
                    WHERE timestamp > NOW() - INTERVAL '1 hour'
                    GROUP BY source
                    ORDER BY cnt DESC
                """)
            )
            by_source = {str(row[0]): row[1] for row in r.fetchall()}

            for source, count in by_source.items():
                if count > 100:  # More than 100 signals in an hour
                    anomalies.append({
                        "type": "scout_signal_flood",
                        "severity": 0.5,
                        "detail": f"Scout {source} emitted {count} signals in last hour",
                        "value": count,
                        "threshold": 100,
                    })

        return anomalies

    async def _check_drift_anomalies(self) -> list[dict]:
        """Detect abnormal drift escalation."""
        anomalies = []
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT composite_severity, feature_drift_score,
                           strategy_drift_score, regime_drift_score
                    FROM drift_detection
                    ORDER BY detected_at DESC LIMIT 1
                """)
            )
            row = r.fetchone()
            if row:
                composite = float(row[0] or 0)
                if composite > 0.8:
                    anomalies.append({
                        "type": "drift_escalation",
                        "severity": composite,
                        "detail": f"Drift composite severity critical: {composite:.2f}",
                        "value": composite,
                        "threshold": 0.8,
                    })

        return anomalies

    async def _check_retirement_anomalies(self) -> list[dict]:
        """Detect abnormal retirement clustering."""
        anomalies = []
        async with self.db.engine.connect() as conn:
            r = await conn.execute(
                text("""
                    SELECT COUNT(*) as retired_last_hour
                    FROM strategies
                    WHERE status = 'retired'
                      AND updated_at > NOW() - INTERVAL '1 hour'
                """)
            )
            row = r.fetchone()
            count = row[0] or 0
            if count > 10:
                anomalies.append({
                    "type": "retirement_cluster",
                    "severity": 0.7,
                    "detail": f"{count} strategies retired in last hour",
                    "value": count,
                    "threshold": 10,
                })

        return anomalies
