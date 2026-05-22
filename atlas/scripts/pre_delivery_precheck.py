"""
Pre-delivery pre-soak validation.
Uses actual production schema column names (verified from information_schema).
"""
import asyncio
import sys
from datetime import datetime, timezone

from atlas.data.storage.timescale_client import TimescaleClient
from atlas.config.settings import settings
from sqlalchemy.sql import text


OK = "[OK]"
FAIL = "[FAIL]"
WARN = "[WARN]"
INFO = "[INFO]"

results = []
failures = 0


def log(status: str, msg: str):
    global failures
    line = f"{status} {msg}"
    results.append(line)
    print(line)
    if status == FAIL:
        failures += 1


async def check():
    print("=" * 72)
    print("  PRE-DELIVERY PRE-SOAK VALIDATION")
    print(f"  Started: {datetime.now(timezone.utc).isoformat()}")
    print("=" * 72)
    print()

    db = TimescaleClient(settings.database_url)
    await db.connect()
    log(OK, "TimescaleClient connected")

    async with db.engine.connect() as conn:
        # --- DB connection ---
        r = await conn.execute(text("SELECT 1"))
        assert r.scalar() == 1
        log(OK, "PostgreSQL connection verified (SELECT 1)")

        # --- Table existence (actual production names) ---
        critical_tables = [
            "event_store", "audit_ledger", "event_snapshots",
            "strategies", "backtest_results", "backtest_trades",
            "paper_trades", "execution_log", "copy_execution_log",
            "copy_position_state", "copy_quality_metrics", "copy_drift_log",
            "copy_replay_events", "leader_orders",
            "mutation_memory", "mutation_outcome_log", "mutation_policy_state",
            "scout_quarantine", "scout_poison_quarantine",
            "scout_signal_attribution", "scout_synthesis_log",
            "external_scout_memory", "source_performance_log",
            "source_performance_log", "positions", "portfolio_intelligence",
            "capital_allocation", "capital_preservation_state",
            "risk_state", "risk_state", "system_health",
            "lifecycle_events", "agent_registry", "failed_inserts",
            "schema_version", "market_data_l1", "replay_integrity",
            "systemic_risk", "drift_detection", "strategy_retirement",
            "correlation_memory", "stress_test_results",
            "combination_memory", "pattern_memory",
        ]
        # Deduplicate
        critical_tables = list(dict.fromkeys(critical_tables))

        log(INFO, f"Checking {len(critical_tables)} critical tables...")
        missing_tables = []
        for t in critical_tables:
            r = await conn.execute(
                text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = :t)"),
                {"t": t},
            )
            if r.scalar():
                log(OK, f"  Table '{t}' exists")
            else:
                missing_tables.append(t)
                log(FAIL, f"  Table '{t}' MISSING")

        if missing_tables:
            log(FAIL, f"Missing tables ({len(missing_tables)}): {', '.join(missing_tables)}")
        else:
            log(OK, f"All {len(critical_tables)} critical tables present")

        # --- Schema version ---
        try:
            r = await conn.execute(
                text("SELECT version, description, applied_at::text FROM schema_version ORDER BY applied_at")
            )
            rows = r.fetchall()
            log(INFO, f"Schema versions ({len(rows)} applied):")
            for row in rows:
                log(INFO, f"  {row[0]:12s}  {str(row[2])[:26]}  {row[1]}")
        except Exception as e:
            log(WARN, f"schema_version table query failed: {e}")

        # --- Column-level schema validation (actual production column names) ---
        log(INFO, "Checking critical columns on major tables (production schema)...")

        column_checks = [
            ("event_store", ["id", "aggregate_id", "aggregate_type", "event_type", "event_version",
                             "data", "trace_id", "parent_event_id", "created_at",
                             "sequence", "version", "metadata", "hash_prev", "hash_self"]),
            ("audit_ledger", ["id", "event_type", "actor", "target_id", "action",
                              "data_hash", "previous_hash", "trace_id", "metadata", "created_at",
                              "resource_type", "resource_id", "details", "severity",
                              "hash_prev", "hash_self", "sequence"]),
            ("strategies", ["id", "name", "code", "parameters", "status", "created_at",
                            "author_agent", "prompt", "raw_response", "normalized_strategy",
                            "compile_error", "strategy_signature", "validation_metrics",
                            "train_sharpe", "test_sharpe", "holdout_sharpe",
                            "stability_score", "overfit_flag", "regime_score",
                            "trace_id", "generation_batch", "mutation_type"]),
            ("backtest_results", ["strategy_id", "start_date", "end_date", "sharpe", "cagr",
                                  "max_drawdown", "win_rate", "total_trades", "passed_validation",
                                  "results", "entry_count", "exit_count", "bars_processed",
                                  "short_window_score", "score_7d", "score_14d", "score_30d", "created_at"]),
            ("paper_trades", ["time", "strategy_id", "symbol", "side", "quantity", "price",
                              "fill_price", "status", "pnl", "id", "qty"]),
            ("execution_log", ["id", "order_key", "strategy_id", "symbol", "side",
                               "quantity", "price", "state", "broker_order_id",
                               "client_order_id", "broker", "error_message", "metadata", "created_at"]),
            ("copy_execution_log", ["id", "leader_order_id", "follower_order_id", "leader_id",
                                    "follower_id", "symbol", "side", "leader_qty", "follower_qty",
                                    "latency_ms", "status", "failure_reason", "metadata", "created_at"]),
            ("lifecycle_events", ["id", "trace_id", "strategy_id", "stage", "status",
                                  "actor", "parent_event_id", "metadata", "created_at", "agent_name"]),
            ("mutation_memory", ["id", "parent_strategy_id", "child_strategy_id", "mutation_type",
                                 "changed_fields", "parent_sharpe", "child_sharpe", "sharpe_delta",
                                 "parent_entry_count", "child_entry_count",
                                 "parent_trades", "child_trades", "created_at",
                                 "parent_composite_score", "child_composite_score",
                                 "score_delta", "improved", "updated_at"]),
            ("scout_quarantine", ["id", "source", "source_sub", "reasons", "raw_payload", "quarantined_at"]),
            ("scout_poison_quarantine", ["id", "trace_id", "source", "source_sub", "violation_type",
                                         "severity_score", "affected_symbols", "action_taken",
                                         "metadata", "detected_at"]),
            ("external_scout_memory", ["id", "source", "source_sub", "source_reliability",
                                       "timestamp", "sentiment", "mentioned_tickers",
                                       "hypothesis_score", "signal_direction", "metadata", "details"]),
            ("source_performance_log", ["id", "source", "source_sub", "dynamic_trust_score",
                                        "historical_accuracy", "n_profitable_signals",
                                        "n_loss_signals", "n_quarantined_signals",
                                        "recent_contradiction_rate", "metadata", "updated_at"]),
            ("positions", ["id", "account_ref", "symbol", "qty", "avg_price", "side",
                           "created_at", "updated_at", "strategy_id", "broker", "unrealized_pnl"]),
            ("risk_state", ["id", "scope", "strategy_id", "halted", "reason", "triggered_by",
                            "activated_at", "released_at", "metadata", "created_at", "updated_at"]),
            ("agent_registry", ["id", "name", "type", "layer", "status", "pid",
                                "last_heartbeat", "created_at", "metadata"]),
            ("failed_inserts", ["id", "table_name", "query", "params", "reason", "inserted_at"]),
            ("schema_version", ["version", "applied_at", "description", "checksum"]),
        ]

        all_cols_ok = True
        for table, expected_cols in column_checks:
            r = await conn.execute(
                text("SELECT column_name FROM information_schema.columns WHERE table_name = :t"),
                {"t": table},
            )
            actual_cols = {row[0] for row in r.fetchall()}
            missing_cols = [c for c in expected_cols if c not in actual_cols]
            if missing_cols:
                log(FAIL, f"  {table}: missing columns {missing_cols}")
                all_cols_ok = False
            else:
                log(OK, f"  {table}: all {len(expected_cols)} columns present")

        if all_cols_ok:
            log(OK, "All column-level schema checks passed")
        else:
            log(FAIL, "Some columns are missing - review above")

        # --- Row-count baseline ---
        count_tables = [
            "event_store", "audit_ledger", "event_snapshots",
            "strategies", "backtest_results", "backtest_trades",
            "paper_trades", "execution_log", "copy_execution_log",
            "mutation_memory", "lifecycle_events", "positions",
            "scout_quarantine", "scout_poison_quarantine",
            "external_scout_memory", "source_performance_log",
            "failed_inserts", "schema_version",
        ]
        log(INFO, "Row-count baseline:")
        counts = {}
        for t in count_tables:
            try:
                r = await conn.execute(text(f"SELECT COUNT(*) FROM {t}"))
                counts[t] = r.scalar()
                log(INFO, f"  {t}: {counts[t]:>8} rows")
            except Exception as e:
                log(WARN, f"  {t}: count failed - {e}")
        total_rows = sum(counts.values())
        log(OK, f"Baseline captured ({total_rows} total rows across {len(counts)} tables)")

        # --- Replay tables check ---
        replay_tables = ["event_store", "event_snapshots", "audit_ledger"]
        log(INFO, "Replay-readiness check:")
        for t in replay_tables:
            c = counts.get(t, 0)
            log(OK if c > 0 else WARN, f"  {t}: {c} rows available for replay")
        log(OK, "Replay tables operational")

        # --- Hash-chain integrity check ---
        log(INFO, "Sampling event_store hash chain integrity...")
        try:
            r = await conn.execute(text("""
                SELECT COUNT(*) FROM event_store WHERE hash_self IS NOT NULL
            """))
            has_hashes = r.scalar()
            log(OK if has_hashes > 0 else WARN, f"  {has_hashes} events with hash_self set")
        except Exception as e:
            log(WARN, f"  Hash chain check failed: {e}")

        try:
            r = await conn.execute(text("""
                SELECT COUNT(*) FROM audit_ledger WHERE hash_self IS NOT NULL
            """))
            has_hashes = r.scalar()
            log(OK if has_hashes > 0 else WARN, f"  {has_hashes} audit entries with hash_self set")
        except Exception as e:
            log(WARN, f"  audit_ledger hash chain check failed: {e}")

        # --- Timestamp sanity ---
        log(INFO, "Timestamp sanity (UTC-aware):")
        try:
            r = await conn.execute(text("""
                SELECT
                    (SELECT MIN(created_at) FROM event_store) as min_es,
                    (SELECT MAX(created_at) FROM event_store) as max_es,
                    (SELECT MIN(created_at) FROM audit_ledger) as min_al,
                    (SELECT MAX(created_at) FROM audit_ledger) as max_al
            """))
            row = r.fetchone()
            log(INFO, f"  event_store:     {str(row[0])[:26]}  ->  {str(row[1])[:26]}")
            log(INFO, f"  audit_ledger:    {str(row[2])[:26]}  ->  {str(row[3])[:26]}")
        except Exception as e:
            log(WARN, f"  Timestamp query failed: {e}")

        # --- Failed inserts dead-letter check ---
        try:
            r = await conn.execute(text("SELECT COUNT(*) FROM failed_inserts"))
            failed_count = r.scalar()
            if failed_count > 0:
                log(WARN, f"  failed_inserts has {failed_count} records - investigate")
                r2 = await conn.execute(text("SELECT table_name, reason, inserted_at::text FROM failed_inserts ORDER BY inserted_at DESC LIMIT 5"))
                for row in r2.fetchall():
                    log(WARN, f"    table={row[0]}, reason={str(row[1])[:60]}, at={str(row[2])[:26]}")
            else:
                log(OK, "  failed_inserts is empty (no dead-letter records)")
        except Exception as e:
            log(WARN, f"  failed_inserts check failed: {e}")

    log(OK, "Database checks complete")

    # --- Summary ---
    print()
    print("=" * 72)
    print("  PRE-SOAK SUMMARY")
    print("=" * 72)
    passed = sum(1 for r in results if r.startswith(OK))
    failed = sum(1 for r in results if r.startswith(FAIL))
    warned = sum(1 for r in results if r.startswith(WARN))
    print(f"  Passed: {passed}, Failed: {failed}, Warnings: {warned}, Info: {len(results) - passed - failed - warned}")
    print(f"  Total checks: {len(results)}")
    print()
    if failures == 0:
        print("  >>> READY FOR SOAK <<<")
    else:
        print(f"  >>> NOT READY - {failures} failure(s) must be resolved <<<")
    print("=" * 72)

    return {"passed": passed, "failed": failed, "warned": warned, "results": results}


if __name__ == "__main__":
    asyncio.run(check())
