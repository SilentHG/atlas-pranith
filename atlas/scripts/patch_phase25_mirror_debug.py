"""
Phase 25 Step 3: Add debug signal pipeline mode to timescale_client.py.
- Adds scout_mirror_debug_log table in auto-migration section
- Adds comprehensive debug logging to _mirror_to_scout_signals()
"""
import re
import ast

FILE = "data/storage/timescale_client.py"

with open(FILE, "r", encoding="utf-8") as f:
    content = f.read()

# ── Edit 1: Add debug table creation after scout_signals indexes ──
old_table_section = '''            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_scout_signals_created ON scout_signals (created_at DESC)")
            )

            # ================================================================
            # PHASE 24 — SCHEMA DRIFT FIXES (post-soak audit remediation)
            # ================================================================'''

new_table_section = '''            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_scout_signals_created ON scout_signals (created_at DESC)")
            )

            # ================================================================
            # PHASE 25 — SCOUT MIRROR DEBUG LOG (signal pipeline observability)
            # ================================================================
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS scout_mirror_debug_log (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    table_name TEXT,
                    source TEXT,
                    symbol TEXT,
                    signal_type TEXT,
                    confidence_score NUMERIC DEFAULT 0.0,
                    success BOOLEAN DEFAULT FALSE,
                    error_message TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_debug_log_created ON scout_mirror_debug_log (created_at DESC)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_debug_log_table ON scout_mirror_debug_log (table_name)")
            )

            # ================================================================
            # PHASE 24 — SCHEMA DRIFT FIXES (post-soak audit remediation)
            # ================================================================'''

if old_table_section not in content:
    # Try without trailing whitespace variations
    idx = content.find('idx_scout_signals_created ON scout_signals (created_at DESC)')
    if idx >= 0:
        print("Found scout_signals index at position", idx)
        # Find the enclosing lines
        line_start = content.rfind('\n', 0, idx)
        line_end = content.find('\n', idx)
        print(f"Context: ...{content[line_start:line_end]}...")
    else:
        print("ERROR: Could not find scout_signals index at all!")
    raise SystemExit(1)

content = content.replace(old_table_section, new_table_section, 1)
print("[PASS] Edit 1: Added scout_mirror_debug_log table creation")

# ── Edit 2: Add debug logging to _mirror_to_scout_signals() ──
old_mirror_body = '''        async with self.engine.begin() as conn:
            try:
                await conn.execute(
                    text(insert_query),
                    {
                        "source": str(raw_source),
                        "symbol": str(symbol) if symbol else None,
                        "signal_type": config["signal_type"],
                        "confidence_score": confidence,
                        "signal_data": safe_json_dumps(signal_data),
                    },
                )
            except Exception:
                # Mirror failures are non-fatal -- do not poison the primary insert
                pass'''

new_mirror_body = '''        async with self.engine.begin() as conn:
            success = False
            error_msg = None
            try:
                await conn.execute(
                    text(insert_query),
                    {
                        "source": str(raw_source),
                        "symbol": str(symbol) if symbol else None,
                        "signal_type": config["signal_type"],
                        "confidence_score": confidence,
                        "signal_data": safe_json_dumps(signal_data),
                    },
                )
                success = True
            except Exception as e:
                error_msg = str(e)[:500]
                # Mirror failures are non-fatal -- do not poison the primary insert

            # Phase 25 Step 3: Debug mode — log every mirror attempt
            try:
                await conn.execute(
                    text("""
                        INSERT INTO scout_mirror_debug_log
                            (table_name, source, symbol, signal_type, confidence_score, success, error_message)
                        VALUES (:tn, :src, :sym, :st, :conf, :ok, :err)
                    """),
                    {
                        "tn": table_name,
                        "src": str(raw_source),
                        "sym": str(symbol) if symbol else None,
                        "st": config["signal_type"],
                        "conf": confidence,
                        "ok": success,
                        "err": error_msg,
                    },
                )
            except Exception as log_e:
                from loguru import logger
                logger.debug(f"Mirror debug log insertion failed: {log_e}")'''

if old_mirror_body not in content:
    print("ERROR: Could not find mirror function body to replace!")
    print("Searching for partial match...")
    idx = content.find('async with self.engine.begin() as conn:')
    if idx >= 0:
        print(f"Found 'async with self.engine.begin() as conn:' at position {idx}")
        print(repr(content[idx:idx+400]))
    raise SystemExit(1)

content = content.replace(old_mirror_body, new_mirror_body, 1)
print("[PASS] Edit 2: Added debug logging to _mirror_to_scout_signals()")

# ── Validate syntax ──
try:
    ast.parse(content)
    print("[PASS] Syntax check: PASSED")
except SyntaxError as e:
    print(f"[FAIL] Syntax error after patch: {e}")
    raise SystemExit(1)

# ── Write back ──
with open(FILE, "w", encoding="utf-8") as f:
    f.write(content)

print(f"[PASS] {FILE} patched successfully")
