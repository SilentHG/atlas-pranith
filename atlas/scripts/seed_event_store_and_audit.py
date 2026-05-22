"""
seed_event_store_and_audit.py
==============================
Populate event_store and audit_ledger tables from existing historical data.

Seeds:
  - event_store:    Events converted from lifecycle_events with proper hash chaining,
                    aggregate grouping (by trace_id), and sequence numbering.
  - audit_ledger:   Strategy lifecycle audit entries with global cryptographic hash chain.
  - event_snapshots: Point-in-time snapshots from backtest_results for fast replay.

Usage:
  cd /path/to/ATLAS
  python atlas/scripts/seed_event_store_and_audit.py
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from atlas.config.settings import settings
from atlas.data.storage.timescale_client import TimescaleClient
from loguru import logger
from sqlalchemy.sql import text

BATCH_SIZE = 500


def _compute_hash(content: dict) -> str:
    """SHA-256 hex digest of a dict (sorted keys, JSON-serialized)."""
    return hashlib.sha256(
        json.dumps(content, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _ensure_utc(dt: datetime | None) -> datetime:
    if dt is None:
        return datetime.now(timezone.utc)
    if hasattr(dt, "tzinfo") and dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _stage_to_agg_type(stage: str) -> str:
    s = stage.lower()
    if s in ("ideator", "coder", "mutator"):
        return "strategy"
    elif s in ("backtest", "validation"):
        return "backtest"
    elif s in ("execution", "fill", "order"):
        return "trade"
    elif s in ("scout", "market_regime", "liquidity"):
        return "scout_observation"
    elif s in ("portfolio", "allocation", "risk"):
        return "portfolio"
    elif s in ("deployment", "governance"):
        return "governance"
    elif s in ("retirement", "drift"):
        return "lifecycle"
    else:
        return "system"


# ── Step 1: Seed event_store from lifecycle_events ──────────────
async def seed_event_store(db: TimescaleClient) -> dict:
    logger.info("=" * 60)
    logger.info("STEP 1: Seeding event_store from lifecycle_events")
    logger.info("=" * 60)

    async with db.engine.connect() as conn:
        r = await conn.execute(
            text("""
                SELECT id, trace_id, strategy_id, stage, status, actor,
                       parent_event_id, metadata, created_at
                FROM lifecycle_events
                ORDER BY trace_id, created_at ASC
            """),
        )
        rows = r.fetchall()
        total = len(rows)
        logger.info(f"  Read {total} lifecycle events from DB")
        if total == 0:
            return {"events_written": 0, "aggregates_created": 0}

    from collections import OrderedDict

    aggregates: OrderedDict[str, list[dict]] = OrderedDict()
    for row in rows:
        tid = str(row[1])
        if tid not in aggregates:
            aggregates[tid] = []
        aggregates[tid].append({
            "id": str(row[0]),
            "trace_id": tid,
            "strategy_id": str(row[2]) if row[2] else None,
            "stage": str(row[3]),
            "status": str(row[4]),
            "actor": str(row[5]),
            "parent_event_id": str(row[6]) if row[6] else None,
            "metadata": row[7] if row[7] else {},
            "created_at": _ensure_utc(row[8]),
        })

    event_rows: list[dict] = []
    aggregates_created = 0
    events_written = 0

    for trace_id, events in aggregates.items():
        if not events:
            continue
        aggregates_created += 1
        agg_type = _stage_to_agg_type(events[0]["stage"])
        prev_hash: str | None = None
        seq = 1

        for ev in events:
            created_at = ev["created_at"]
            created_iso = created_at.isoformat()

            metadata = ev["metadata"]
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except Exception:
                    metadata = {}

            event_id = uuid.uuid4().hex[:16]

            # Content for hash — uses ISO strings for deterministic serialization
            content = {
                "id": event_id,
                "event_type": ev["stage"],
                "version": "1.0",
                "trace_id": trace_id,
                "parent_event_id": ev["parent_event_id"],
                "aggregate_id": trace_id,
                "aggregate_type": agg_type,
                "data": {
                    "lifecycle_id": ev["id"],
                    "strategy_id": ev["strategy_id"],
                    "actor": ev["actor"],
                    "status": ev["status"],
                    "stage": ev["stage"],
                },
                "metadata": metadata,
                "hash_prev": prev_hash,
                "sequence": seq,
                "created_at": created_iso,
            }
            hash_self = _compute_hash(content)

            # Row for DB INSERT — JSONB cols use json.dumps() + ::jsonb cast,
            # TIMESTAMPTZ cols use datetime objects (no cast needed)
            event_rows.append({
                "id": event_id,
                "event_type": ev["stage"],
                "version": "1.0",
                "trace_id": trace_id,
                "parent_event_id": ev["parent_event_id"],
                "aggregate_id": trace_id,
                "aggregate_type": agg_type,
                "data": json.dumps(content["data"]),
                "metadata": json.dumps(metadata),
                "hash_prev": prev_hash,
                "hash_self": hash_self,
                "created_at": created_at,   # datetime object — NO ::timestamptz cast
                "sequence": seq,
            })

            prev_hash = hash_self
            seq += 1
            events_written += 1

    logger.info(f"  Inserting {events_written} event_store rows in {BATCH_SIZE}-row batches...")
    async with db.engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE event_store CASCADE"))
        await conn.execute(text("TRUNCATE TABLE event_snapshots CASCADE"))

        for i in range(0, len(event_rows), BATCH_SIZE):
            batch = event_rows[i : i + BATCH_SIZE]
            vals = []
            params: dict[str, object] = {}
            for j, row in enumerate(batch):
                p = f"e{i+j}_"
                vals.append(
                    f"(:{p}id, :{p}event_type, :{p}version, :{p}trace_id, "
                    f":{p}parent_event_id, :{p}aggregate_id, :{p}aggregate_type, "
                    f"CAST(:{p}data AS jsonb), CAST(:{p}metadata AS jsonb), :{p}hash_prev, "
                    f":{p}hash_self, :{p}created_at, :{p}sequence)"
                )
                for k, v in row.items():
                    params[f"{p}{k}"] = v

            sql = f"""
                INSERT INTO event_store
                    (id, event_type, version, trace_id, parent_event_id,
                     aggregate_id, aggregate_type, data, metadata,
                     hash_prev, hash_self, created_at, sequence)
                VALUES {', '.join(vals)}
            """
            await conn.execute(text(sql), params)

    logger.info(f"  [OK] Wrote {events_written} events across {aggregates_created} aggregates")
    return {"events_written": events_written, "aggregates_created": aggregates_created}


# ── Step 2: Seed audit_ledger from strategies lifecycle ─────────
async def seed_audit_ledger(db: TimescaleClient) -> dict:
    logger.info("=" * 60)
    logger.info("STEP 2: Seeding audit_ledger from strategies & lifecycle")
    logger.info("=" * 60)

    async with db.engine.connect() as conn:
        r = await conn.execute(
            text("""
                SELECT id, name, status, author_agent, created_at, trace_id
                FROM strategies ORDER BY created_at ASC
            """),
        )
        strategies = r.fetchall()
        logger.info(f"  Read {len(strategies)} strategies from DB")

        r2 = await conn.execute(
            text("""
                SELECT strategy_id, sharpe, win_rate, passed_validation,
                       total_trades, created_at
                FROM backtest_results ORDER BY created_at ASC
            """),
        )
        backtests = r2.fetchall()
        bt_map: dict[str, list[dict]] = {}
        for bt in backtests:
            sid = str(bt[0])
            bt_map.setdefault(sid, []).append({
                "sharpe": float(bt[1]) if bt[1] is not None else None,
                "win_rate": float(bt[2]) if bt[2] is not None else None,
                "passed": bool(bt[3]) if bt[3] is not None else None,
                "total_trades": int(bt[4]) if bt[4] is not None else None,
                "created_at": _ensure_utc(bt[5]),
            })
        logger.info(f"  Mapped {len(backtests)} backtest results to strategies")

    audit_rows: list[dict] = []
    entries_written = 0

    # Ensure sequence column exists on audit_ledger
    async with db.engine.begin() as conn:
        await conn.execute(
            text("ALTER TABLE audit_ledger ADD COLUMN IF NOT EXISTS sequence INT DEFAULT 1")
        )
        await conn.execute(
            text("DROP INDEX IF EXISTS idx_audit_sequence")
        )
        await conn.execute(
            text("CREATE INDEX IF NOT EXISTS idx_audit_trace_sequence ON audit_ledger (trace_id, sequence)")
        )

    for st in strategies:
        strategy_id = str(st[0])
        strategy_name = str(st[1]) if st[1] else strategy_id[:12]
        status = str(st[2])
        author = str(st[3]) if st[3] else "system"
        created_at = _ensure_utc(st[4])
        created_iso = created_at.isoformat()
        trace_id = str(st[5]) if st[5] else uuid.uuid4().hex[:16]

        # Use per-strategy hash chain (like event_store aggregates)
        # Sequence numbers ensure deterministic ordering within each strategy
        # This avoids ordering issues when backtest timestamps differ from strategy timestamps
        prev_hash: str | None = None
        seq: int = 1

        # Entry 1: Strategy creation
        eid = uuid.uuid4().hex[:16]
        c1 = {"id": eid, "event_type": "strategy_created", "actor": author,
              "action": f"Created strategy '{strategy_name}'",
              "resource_type": "strategy", "resource_id": strategy_id,
              "details": {"status": status, "name": strategy_name},
              "severity": "info", "trace_id": trace_id,
              "sequence": seq,
              "hash_prev": prev_hash, "created_at": created_iso}
        h1 = _compute_hash(c1)
        audit_rows.append({"id": eid, "event_type": "strategy_created",
            "actor": author, "action": f"Created strategy '{strategy_name}'",
            "resource_type": "strategy", "resource_id": strategy_id,
            "details": json.dumps({"status": status, "name": strategy_name}),
            "severity": "info", "trace_id": trace_id,
            "hash_prev": prev_hash, "hash_self": h1,
            "created_at": created_at, "sequence": seq})
        prev_hash = h1
        seq += 1
        entries_written += 1

        # Entry 2: Code generation
        if status in ("backtest_ready", "backtest_failed", "pending_backtest",
                      "failed_validation", "repair_candidate"):
            eid2 = uuid.uuid4().hex[:16]
            c2 = {"id": eid2, "event_type": "code_generated", "actor": "CoderAgent",
                  "action": f"Generated code for strategy '{strategy_name}'",
                  "resource_type": "strategy", "resource_id": strategy_id,
                  "details": {"status": status},
                  "severity": "info", "trace_id": trace_id,
                  "sequence": seq,
                  "hash_prev": prev_hash, "created_at": created_iso}
            h2 = _compute_hash(c2)
            audit_rows.append({"id": eid2, "event_type": "code_generated",
                "actor": "CoderAgent",
                "action": f"Generated code for strategy '{strategy_name}'",
                "resource_type": "strategy", "resource_id": strategy_id,
                "details": json.dumps({"status": status}),
                "severity": "info", "trace_id": trace_id,
                "hash_prev": prev_hash, "hash_self": h2,
                "created_at": created_at, "sequence": seq})
            prev_hash = h2
            seq += 1
            entries_written += 1

        # Entry 3: Backtest results
        if strategy_id in bt_map:
            for bt in bt_map[strategy_id]:
                bt_ts = bt["created_at"]
                bt_iso = bt_ts.isoformat()
                eid3 = uuid.uuid4().hex[:16]
                pstr = "passed" if bt["passed"] else "failed"
                c3 = {"id": eid3, "event_type": "backtest_completed",
                      "actor": "BacktestRunner",
                      "action": f"Backtest {pstr} for strategy '{strategy_name}'",
                      "resource_type": "strategy", "resource_id": strategy_id,
                      "details": {"sharpe": bt["sharpe"], "win_rate": bt["win_rate"],
                                  "passed": bt["passed"], "total_trades": bt["total_trades"]},
                      "severity": "warning" if not bt["passed"] else "info",
                      "trace_id": trace_id,
                      "sequence": seq,
                      "hash_prev": prev_hash,
                      "created_at": bt_iso}
                h3 = _compute_hash(c3)
                audit_rows.append({"id": eid3, "event_type": "backtest_completed",
                    "actor": "BacktestRunner",
                    "action": f"Backtest {pstr} for strategy '{strategy_name}'",
                    "resource_type": "strategy", "resource_id": strategy_id,
                    "details": json.dumps({"sharpe": bt["sharpe"], "win_rate": bt["win_rate"],
                                           "passed": bt["passed"], "total_trades": bt["total_trades"]}),
                    "severity": "warning" if not bt["passed"] else "info",
                    "trace_id": trace_id, "hash_prev": prev_hash, "hash_self": h3,
                    "created_at": bt_ts, "sequence": seq})
                prev_hash = h3
                seq += 1
                entries_written += 1

        # Entry 4: Lifecycle transition
        sev = "info"
        at = f"Strategy '{strategy_name}' entered lifecycle status: {status}"
        if status == "failed_validation":
            sev = "warning"
        elif status == "repair_candidate":
            sev = "warning"
        elif status == "backtest_failed":
            sev = "error"

        eid4 = uuid.uuid4().hex[:16]
        c4 = {"id": eid4, "event_type": "lifecycle_transition",
              "actor": "ValidatorAgent", "action": at,
              "resource_type": "strategy", "resource_id": strategy_id,
              "details": {"status": status, "final_state": True},
              "severity": sev, "trace_id": trace_id,
              "sequence": seq,
              "hash_prev": prev_hash, "created_at": created_iso}
        h4 = _compute_hash(c4)
        audit_rows.append({"id": eid4, "event_type": "lifecycle_transition",
            "actor": "ValidatorAgent", "action": at,
            "resource_type": "strategy", "resource_id": strategy_id,
            "details": json.dumps({"status": status, "final_state": True}),
            "severity": sev, "trace_id": trace_id,
            "hash_prev": prev_hash, "hash_self": h4,
            "created_at": created_at, "sequence": seq})
        prev_hash = h4
        seq += 1
        entries_written += 1

    logger.info(f"  Inserting {len(audit_rows)} audit_ledger rows in {BATCH_SIZE}-row batches...")
    async with db.engine.begin() as conn:
        await conn.execute(text("TRUNCATE TABLE audit_ledger CASCADE"))

        for i in range(0, len(audit_rows), BATCH_SIZE):
            batch = audit_rows[i : i + BATCH_SIZE]
            vals = []
            params: dict[str, object] = {}
            for j, row in enumerate(batch):
                p = f"a{i+j}_"
                vals.append(
                    f"(:{p}id, :{p}event_type, :{p}actor, :{p}action, "
                    f":{p}resource_type, :{p}resource_id, CAST(:{p}details AS jsonb), "
                    f":{p}severity, :{p}trace_id, :{p}hash_prev, :{p}hash_self, "
                    f":{p}created_at, :{p}sequence)"
                )
                for k, v in row.items():
                    params[f"{p}{k}"] = v

            sql = f"""
                INSERT INTO audit_ledger
                    (id, event_type, actor, action, resource_type, resource_id,
                     details, severity, trace_id, hash_prev, hash_self,
                     created_at, sequence)
                VALUES {', '.join(vals)}
            """
            await conn.execute(text(sql), params)

    logger.info(f"  [OK] Wrote {entries_written} audit_ledger entries with per-strategy hash chains")
    return {"entries_written": entries_written}


# ── Step 3: Seed event_snapshots from backtest_results ──────────
async def seed_event_snapshots(db: TimescaleClient) -> dict:
    logger.info("=" * 60)
    logger.info("STEP 3: Seeding event_snapshots from backtest_results")
    logger.info("=" * 60)

    async with db.engine.connect() as conn:
        r = await conn.execute(
            text("""
                SELECT DISTINCT ON (strategy_id)
                    strategy_id, sharpe, win_rate, cagr, max_drawdown,
                    total_trades, passed_validation, results, created_at
                FROM backtest_results
                ORDER BY strategy_id, created_at DESC
            """),
        )
        snapshots_data = r.fetchall()
        logger.info(f"  Read {len(snapshots_data)} latest backtest results (one per strategy)")

        r2 = await conn.execute(
            text("""
                SELECT aggregate_id, MAX(sequence) as max_seq
                FROM event_store
                WHERE aggregate_type = 'backtest'
                GROUP BY aggregate_id
            """),
        )
        seq_map: dict[str, int] = {}
        for row in r2.fetchall():
            seq_map[str(row[0])] = int(row[1]) if row[1] else 0

    snapshot_rows: list[dict] = []
    for sd in snapshots_data:
        strategy_id = str(sd[0])
        aggregate_id = strategy_id
        version = seq_map.get(aggregate_id, 1)
        created_at = _ensure_utc(sd[8])

        snapshot_rows.append({
            "id": uuid.uuid4().hex[:16],
            "aggregate_id": aggregate_id,
            "version": version,
            "state": json.dumps({
                "strategy_id": strategy_id,
                "sharpe": float(sd[1]) if sd[1] is not None else None,
                "win_rate": float(sd[2]) if sd[2] is not None else None,
                "cagr": float(sd[3]) if sd[3] is not None else None,
                "max_drawdown": float(sd[4]) if sd[4] is not None else None,
                "total_trades": int(sd[5]) if sd[5] is not None else None,
                "passed_validation": bool(sd[6]) if sd[6] is not None else None,
                "backtest_status": "passed" if sd[6] else "failed",
            }),
            "created_at": created_at,
        })

    if snapshot_rows:
        async with db.engine.begin() as conn:
            for i in range(0, len(snapshot_rows), BATCH_SIZE):
                batch = snapshot_rows[i : i + BATCH_SIZE]
                vals = []
                params: dict[str, object] = {}
                for j, row in enumerate(batch):
                    p = f"s{i+j}_"
                    vals.append(
                        f"(:{p}id, :{p}aggregate_id, :{p}version, "
                        f"CAST(:{p}state AS jsonb), :{p}created_at)"
                    )
                    for k, v in row.items():
                        params[f"{p}{k}"] = v

                sql = f"""
                    INSERT INTO event_snapshots
                        (id, aggregate_id, version, state, created_at)
                    VALUES {', '.join(vals)}
                """
                await conn.execute(text(sql), params)

    logger.info(f"  [OK] Created {len(snapshot_rows)} event_snapshots")
    return {"snapshots_created": len(snapshot_rows)}


# ── Step 4: Verify hash chain integrity ─────────────────────────
async def verify_integrity(db: TimescaleClient) -> dict:
    logger.info("=" * 60)
    logger.info("STEP 4: Verifying hash chain integrity")
    logger.info("=" * 60)

    results: dict[str, dict] = {
        "event_store": {"valid": True, "checked": 0, "violations": []},
        "audit_ledger": {"valid": True, "checked": 0, "violations": []},
    }

    # 4a. Event store aggregate hash chains
    async with db.engine.connect() as conn:
        r = await conn.execute(
            text("""
                SELECT aggregate_id, aggregate_type, COUNT(*) as cnt
                FROM event_store GROUP BY aggregate_id, aggregate_type ORDER BY cnt DESC
            """),
        )
        aggregates = r.fetchall()
        logger.info(f"  Verifying {len(aggregates)} aggregates...")

        for agg in aggregates:
            agg_id = str(agg[0])
            r2 = await conn.execute(
                text("""
                    SELECT id, event_type, version, trace_id, parent_event_id,
                           aggregate_id, aggregate_type, data, metadata,
                           hash_prev, hash_self, created_at, sequence
                    FROM event_store WHERE aggregate_id = :aid ORDER BY sequence ASC
                """),
                {"aid": agg_id},
            )
            events = r2.fetchall()
            prev_hash: str | None = None

            for ev in events:
                raw_created = ev[11]
                created_str = raw_created.isoformat() if hasattr(raw_created, "isoformat") else str(raw_created)

                content = {
                    "id": str(ev[0]),
                    "event_type": str(ev[1]),
                    "version": str(ev[2]),
                    "trace_id": str(ev[3]),
                    "parent_event_id": str(ev[4]) if ev[4] else None,
                    "aggregate_id": agg_id,
                    "aggregate_type": str(ev[6]),
                    "data": json.loads(ev[7]) if isinstance(ev[7], str) else ev[7],
                    "metadata": json.loads(ev[8]) if isinstance(ev[8], str) else ev[8],
                    "hash_prev": str(ev[9]) if ev[9] else None,
                    "sequence": int(ev[12]),
                    "created_at": created_str,
                }
                # Column mapping: 0=id,1=event_type,2=version,3=trace_id,
                # 4=parent_event_id,5=aggregate_id,6=aggregate_type,7=data,
                # 8=metadata,9=hash_prev,10=hash_self,11=created_at,12=sequence
                expected = _compute_hash(content)
                actual = str(ev[10])

                if expected != actual:
                    results["event_store"]["violations"].append(
                        f"Aggregate {agg_id[:12]} seq={ev[12]}: self-hash mismatch"
                    )
                if prev_hash is not None:
                    actual_prev = str(ev[9]) if ev[9] else None
                    if actual_prev != prev_hash:
                        results["event_store"]["violations"].append(
                            f"Aggregate {agg_id[:12]} seq={ev[12]}: prev-hash broken"
                        )
                prev_hash = actual
                results["event_store"]["checked"] += 1

    # 4b. Audit ledger per-strategy hash chains (grouped by trace_id)
    async with db.engine.connect() as conn:
        r2 = await conn.execute(
            text("""
                SELECT trace_id, COUNT(*) as cnt
                FROM audit_ledger WHERE trace_id IS NOT NULL
                GROUP BY trace_id ORDER BY cnt DESC
            """),
        )
        audit_aggregates = r2.fetchall()
        logger.info(f"  Verifying {len(audit_aggregates)} audit aggregates (trace_id groups)...")

        for agg in audit_aggregates:
            ag = str(agg[0])
            r3 = await conn.execute(
                text("""
                    SELECT id, event_type, actor, action, resource_type, resource_id,
                           details, severity, trace_id, sequence, hash_prev, hash_self, created_at
                    FROM audit_ledger WHERE trace_id = :tid ORDER BY sequence ASC
                """),
                {"tid": ag},
            )
            strategy_entries = r3.fetchall()
            prev_hash: str | None = None

            for entry in strategy_entries:
                details = entry[6]
                if isinstance(details, str):
                    try:
                        details = json.loads(details)
                    except Exception:
                        details = {}

                raw_created = entry[12]
                created_str = raw_created.isoformat() if hasattr(raw_created, "isoformat") else str(raw_created)

                content = {
                    "id": str(entry[0]),
                    "event_type": str(entry[1]),
                    "actor": str(entry[2]),
                    "action": str(entry[3]),
                    "resource_type": str(entry[4]),
                    "resource_id": str(entry[5]) if entry[5] else None,
                    "details": details,
                    "severity": str(entry[7]),
                    "trace_id": str(entry[8]) if entry[8] else None,
                    "sequence": int(entry[9]) if entry[9] is not None else 0,
                    "hash_prev": str(entry[10]) if entry[10] else None,
                    "created_at": created_str,
                }
                expected = _compute_hash(content)
                actual = str(entry[11])

                if expected != actual:
                    results["audit_ledger"]["violations"].append(
                        f"Aggregate {ag[:12]} ({entry[0][:12]}): self-hash mismatch"
                    )
                if prev_hash is not None:
                    actual_prev = str(entry[10]) if entry[10] else None
                    if actual_prev != prev_hash:
                        results["audit_ledger"]["violations"].append(
                            f"Aggregate {ag[:12]} ({entry[0][:12]}): chain broken"
                        )
                prev_hash = actual
                results["audit_ledger"]["checked"] += 1

    for name, res in results.items():
        if res["violations"]:
            results[name]["valid"] = False
            logger.warning(f"  !! {name}: {len(res['violations'])} violations found!")
            for v in res["violations"][:5]:
                logger.warning(f"       {v}")
        else:
            logger.info(f"  [OK] {name}: {res['checked']} entries verified - hash chain INTACT")

    return results


# ── Main ────────────────────────────────────────────────────────
async def main() -> None:
    logger.info("")
    logger.info("=" * 58)
    logger.info("   ATLAS - Seed Event Store & Audit Ledger")
    logger.info("=" * 58)
    logger.info("")

    db = TimescaleClient(settings.database_url)
    await db.connect()
    logger.info("Database connected")

    try:
        es_result = await seed_event_store(db)
        al_result = await seed_audit_ledger(db)
        snap_result = await seed_event_snapshots(db)
        integrity = await verify_integrity(db)

        logger.info("")
        logger.info("=" * 58)
        logger.info("SEED COMPLETE - FINAL SUMMARY")
        logger.info("=" * 58)
        logger.info(f"  event_store:      {es_result['events_written']:>6} events "
                     f"({es_result['aggregates_created']} aggregates)")
        logger.info(f"  audit_ledger:     {al_result['entries_written']:>6} entries "
                     f"(hash chain {'INTACT' if integrity['audit_ledger']['valid'] else 'BROKEN'})")
        logger.info(f"  event_snapshots:  {snap_result['snapshots_created']:>6} snapshots")
        es_status = "INTACT" if integrity["event_store"]["valid"] else "VIOLATIONS"
        al_status = "INTACT" if integrity["audit_ledger"]["valid"] else "VIOLATIONS"
        logger.info(f"  event_store integrity:   {es_status}")
        logger.info(f"  audit_ledger integrity:  {al_status}")
        if integrity["event_store"]["violations"]:
            logger.warning(f"  event_store violations: {integrity['event_store']['violations'][:3]}")
        if integrity["audit_ledger"]["violations"]:
            logger.warning(f"  audit_ledger violations: {integrity['audit_ledger']['violations'][:3]}")
        logger.info("=" * 58)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
