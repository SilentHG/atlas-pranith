"""Add scout_signals table to the database migration in timescale_client.py."""
import os

filepath = os.path.join(os.path.dirname(__file__), "..", "data", "storage", "timescale_client.py")
filepath = os.path.normpath(filepath)

with open(filepath, "r", encoding="utf-8") as f:
    content = f.read()

code_to_add = '''
            # ---------------------------------------------------------------
            # SCOUT_SIGNALS TABLE — anti_poisoning_engine dependency
            # ---------------------------------------------------------------
            await conn.execute(text("""
                CREATE TABLE IF NOT EXISTS scout_signals (
                    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    source TEXT,
                    symbol TEXT,
                    signal_type TEXT,
                    confidence_score NUMERIC DEFAULT 0.0,
                    signal_data JSONB DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_scout_signals_source ON scout_signals (source)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_scout_signals_symbol ON scout_signals (symbol)")
            )
            await conn.execute(
                text("CREATE INDEX IF NOT EXISTS idx_scout_signals_created ON scout_signals (created_at DESC)")
            )
'''

# Find insertion point: after scout_poison_quarantine index creation
insert_marker = 'CREATE INDEX IF NOT EXISTS idx_scout_poison_source ON scout_poison_quarantine (source)'

if insert_marker in content:
    content = content.replace(insert_marker, insert_marker + code_to_add, 1)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    print("SCOUT_SIGNALS TABLE ADDED successfully")
else:
    print("INSERTION POINT NOT FOUND")
    # Try alternative marker
    alt_marker = 'idx_scout_poison_source'
    if alt_marker in content:
        print("Found alternative marker")
