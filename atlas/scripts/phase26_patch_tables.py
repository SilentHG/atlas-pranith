"""
Phase 26: Patch timescale_client.py with scout_influence_log table and helper methods.
Adds:
1. scout_influence_log table in check_schema()
2. scout_economic_attribution table for tracking full causal chains
3. Helper: log_scout_influence()
4. Helper: log_economic_attribution()
5. Helper: get_scout_influence_summary()
6. Helper: get_economic_attribution()
"""
import re
import sys
import os

# Read the file
atlas_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
target = os.path.join(atlas_dir, "data", "storage", "timescale_client.py")
with open(target, "r", encoding="utf-8") as f:
    content = f.read()

# ============================================================
# 1. ADD scout_influence_log and scout_economic_attribution tables
# Insert AFTER the scout_mirror_debug_log section (before PHASE 24)
# ============================================================

phase26_tables = """
            # ================================================================
            # PHASE 26 -- SCOUT-INFLUENCE TRACKING TABLES
            # ================================================================

            # scout_influence_log -- records every scout influence event on agent behavior
            await conn.execute(text(\"""
                CREATE TABLE IF NOT EXISTS scout_influence_log (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    trace_id TEXT,
                    source_scout TEXT NOT NULL,
                    target_agent TEXT NOT NULL,
                    influence_type TEXT NOT NULL,
                    influence_metric TEXT NOT NULL,
                    before_value NUMERIC,
                    after_value NUMERIC,
                    delta NUMERIC,
                    confidence NUMERIC DEFAULT 0.0,
                    regime_context TEXT,
                    entropy_context NUMERIC,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            \"""))
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_scout_influence_source ON scout_influence_log (source_scout)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_scout_influence_target ON scout_influence_log (target_agent)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_scout_influence_created ON scout_influence_log (created_at DESC)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_scout_influence_type ON scout_influence_log (influence_type)")
            )

            # scout_economic_attribution -- full causal chain tracking
            await conn.execute(text(\"""
                CREATE TABLE IF NOT EXISTS scout_economic_attribution (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    trace_id TEXT NOT NULL,
                    source_scout TEXT NOT NULL,
                    influence_type TEXT NOT NULL,
                    target_agent TEXT NOT NULL,
                    strategy_id TEXT,
                    strategy_name TEXT,
                    sharpe_contribution NUMERIC DEFAULT 0.0,
                    drawdown_contribution NUMERIC DEFAULT 0.0,
                    pnl_contribution NUMERIC DEFAULT 0.0,
                    win_rate_contribution NUMERIC DEFAULT 0.0,
                    attribution_weight NUMERIC DEFAULT 0.0,
                    survived_validation BOOLEAN DEFAULT FALSE,
                    regime_at_time TEXT,
                    entropy_at_time NUMERIC,
                    metadata JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            \"""))
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_economic_attr_source ON scout_economic_attribution (source_scout)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_economic_attr_trace ON scout_economic_attribution (trace_id)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_economic_attr_strategy ON scout_economic_attribution (strategy_id)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_economic_attr_created ON scout_economic_attribution (created_at DESC)")
            )

            # Record schema version
            await conn.execute(
                text("INSERT INTO schema_version (version, description) VALUES ('v26.0', 'Phase 26: Scout influence tracking, economic attribution, entropy governance') ON CONFLICT DO NOTHING")
            )

"""

# Find the insertion point: after scout_mirror_debug_log index creation, before PHASE 24
insert_point = "idx_debug_log_table ON scout_mirror_debug_log (table_name)"
insert_idx = content.find(insert_point)
if insert_idx != -1:
    # Find end of this CREATE INDEX block + the blank line after it
    after_idx = content.find("\n", insert_idx)
    after_idx = content.find("\n            # ===========", after_idx)
    content = content[:after_idx] + phase26_tables + content[after_idx:]
    print("[OK] Inserted PHASE 26 table definitions after scout_mirror_debug_log section")
else:
    print("[FAIL] Could not find insertion point for PHASE 26 tables")
    sys.exit(1)

# ============================================================
# 2. ADD helper methods to TimescaleClient class
# ============================================================

helpers_code = """

    # ================================================================
    # PHASE 26 -- SCOUT INFLUENCE TRACKING HELPERS
    # ================================================================

    async def log_scout_influence(
        self,
        source_scout: str,
        target_agent: str,
        influence_type: str,
        influence_metric: str,
        before_value: float | None = None,
        after_value: float | None = None,
        delta: float | None = None,
        confidence: float = 0.0,
        regime_context: str | None = None,
        entropy_context: float | None = None,
        metadata: dict | None = None,
        trace_id: str | None = None,
    ) -> None:
        \"\"\"Log a scout influence event for Phase 26 coupling analysis.\"\"\"
        import uuid
        try:
            await self._execute_insert(
                \"\"\"
                INSERT INTO scout_influence_log
                    (trace_id, source_scout, target_agent, influence_type, influence_metric,
                     before_value, after_value, delta, confidence, regime_context,
                     entropy_context, metadata)
                VALUES
                    (:trace_id, :source, :target, :itype, :imetric,
                     :before, :after, :delta, :conf, :regime,
                     :entropy, CAST(:meta AS jsonb))
                \"\"\",
                {
                    "trace_id": trace_id or uuid.uuid4().hex[:16],
                    "source": source_scout,
                    "target": target_agent,
                    "itype": influence_type,
                    "imetric": influence_metric,
                    "before": before_value,
                    "after": after_value,
                    "delta": delta,
                    "conf": confidence,
                    "regime": regime_context,
                    "entropy": entropy_context,
                    "meta": safe_json_dumps(metadata or {}),
                },
            )
        except Exception as e:
            from loguru import logger
            logger.debug(f"log_scout_influence failed: {e}")

    async def log_economic_attribution(
        self,
        source_scout: str,
        influence_type: str,
        target_agent: str,
        strategy_id: str | None = None,
        strategy_name: str | None = None,
        sharpe_contribution: float = 0.0,
        drawdown_contribution: float = 0.0,
        pnl_contribution: float = 0.0,
        win_rate_contribution: float = 0.0,
        attribution_weight: float = 0.0,
        survived_validation: bool = False,
        regime_at_time: str | None = None,
        entropy_at_time: float | None = None,
        metadata: dict | None = None,
    ) -> None:
        \"\"\"Record full economic attribution for a scout-influenced decision.\"\"\"
        import uuid
        trace_id = uuid.uuid4().hex[:16]
        try:
            await self._execute_insert(
                \"\"\"
                INSERT INTO scout_economic_attribution
                    (trace_id, source_scout, influence_type, target_agent,
                     strategy_id, strategy_name,
                     sharpe_contribution, drawdown_contribution, pnl_contribution,
                     win_rate_contribution, attribution_weight,
                     survived_validation, regime_at_time, entropy_at_time, metadata)
                VALUES
                    (:trace_id, :source, :itype, :target,
                     :sid, :sname,
                     :sharpe, :dd, :pnl,
                     :wr, :weight,
                     :survived, :regime, :entropy, CAST(:meta AS jsonb))
                \"\"\",
                {
                    "trace_id": trace_id,
                    "source": source_scout,
                    "itype": influence_type,
                    "target": target_agent,
                    "sid": strategy_id,
                    "sname": strategy_name,
                    "sharpe": sharpe_contribution,
                    "dd": drawdown_contribution,
                    "pnl": pnl_contribution,
                    "wr": win_rate_contribution,
                    "weight": attribution_weight,
                    "survived": survived_validation,
                    "regime": regime_at_time,
                    "entropy": entropy_at_time,
                    "meta": safe_json_dumps(metadata or {}),
                },
            )
        except Exception as e:
            from loguru import logger
            logger.debug(f"log_economic_attribution failed: {e}")

    async def get_scout_influence_summary(self, source_scout: str | None = None) -> list[dict]:
        \"\"\"Get summary of scout influence events.\"\"\"
        import json
        async with self.engine.connect() as conn:
            if source_scout:
                r = await conn.execute(
                    text(\"\"\"
                        SELECT source_scout, target_agent, influence_type, influence_metric,
                               COUNT(*) as event_count,
                               AVG(ABS(COALESCE(delta, 0))) as avg_abs_delta,
                               AVG(COALESCE(confidence, 0)) as avg_confidence,
                               MIN(created_at) as first_event,
                               MAX(created_at) as last_event
                        FROM scout_influence_log
                        WHERE source_scout = :src
                        GROUP BY source_scout, target_agent, influence_type, influence_metric
                        ORDER BY event_count DESC
                    \"\"\"),
                    {"src": source_scout},
                )
            else:
                r = await conn.execute(
                    text(\"\"\"
                        SELECT source_scout, target_agent, influence_type, influence_metric,
                               COUNT(*) as event_count,
                               AVG(ABS(COALESCE(delta, 0))) as avg_abs_delta,
                               AVG(COALESCE(confidence, 0)) as avg_confidence
                        FROM scout_influence_log
                        GROUP BY source_scout, target_agent, influence_type, influence_metric
                        ORDER BY event_count DESC
                    \"\"\"),
                )
            results = []
            for row in r.fetchall():
                results.append({
                    "source_scout": row[0],
                    "target_agent": row[1],
                    "influence_type": row[2],
                    "influence_metric": row[3],
                    "event_count": row[4],
                    "avg_abs_delta": float(row[5] or 0),
                    "avg_confidence": float(row[6] or 0),
                })
            return results

    async def get_economic_attribution(
        self, source_scout: str | None = None, strategy_id: str | None = None
    ) -> list[dict]:
        \"\"\"Get economic attribution records.\"\"\"
        import json
        async with self.engine.connect() as conn:
            conditions = []
            params = {}
            if source_scout:
                conditions.append("source_scout = :src")
                params["src"] = source_scout
            if strategy_id:
                conditions.append("strategy_id = :sid")
                params["sid"] = strategy_id
            where_clause = " AND ".join(conditions) if conditions else "TRUE"
            r = await conn.execute(
                text(f\"\"\"
                    SELECT source_scout, influence_type, target_agent,
                           strategy_name, sharpe_contribution, drawdown_contribution,
                           pnl_contribution, win_rate_contribution, attribution_weight,
                           survived_validation, regime_at_time, entropy_at_time, created_at
                    FROM scout_economic_attribution
                    WHERE {where_clause}
                    ORDER BY created_at DESC
                    LIMIT 100
                \"\"\"),
                params,
            )
            results = []
            for row in r.fetchall():
                results.append({
                    "source_scout": row[0],
                    "influence_type": row[1],
                    "target_agent": row[2],
                    "strategy_name": row[3],
                    "sharpe_contribution": float(row[4] or 0),
                    "drawdown_contribution": float(row[5] or 0),
                    "pnl_contribution": float(row[6] or 0),
                    "win_rate_contribution": float(row[7] or 0),
                    "attribution_weight": float(row[8] or 0),
                    "survived_validation": bool(row[9]),
                    "regime_at_time": row[10],
                    "entropy_at_time": float(row[11] or 0) if row[11] else None,
                })
            return results

"""

# Find a good insertion point for helpers - after the fetchval method
fetchval_end = "result = await conn.execute(text(query), params or {})\n            return result.scalar()\n"
fetchval_idx = content.find(fetchval_end)
if fetchval_idx != -1:
    after_fetchval = content.find("\n", fetchval_idx)
    after_fetchval = content.find("\n", after_fetchval + 1)
    content = content[:after_fetchval] + helpers_code + content[after_fetchval:]
    print("[OK] Inserted PHASE 26 helper methods")
else:
    print("[FAIL] Could not find fetchval method insertion point")
    sys.exit(1)

# ============================================================
# 3. Add safe_json_dumps import if not already in the file
# ============================================================

if "safe_json_dumps" not in content:
    # Find serialization import line
    import_line = "from atlas.core.serialization import"
    idx = content.find(import_line)
    if idx != -1:
        line_end = content.find("\n", idx)
        existing = content[idx:line_end]
        content = content[:line_end] + ", safe_json_dumps" + content[line_end:]
        print("[OK] Added safe_json_dumps import")
    else:
        print("[WARN] Could not find serialization import - safe_json_dumps may be missing")

# Write back
with open(target, "w", encoding="utf-8") as f:
    f.write(content)
print(f"[OK] Successfully patched {target}")
