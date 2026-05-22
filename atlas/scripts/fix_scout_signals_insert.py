"""Fix the broken scout_signals insertion in timescale_client.py."""
import os

filepath = os.path.join(os.path.dirname(__file__), "..", "data", "storage", "timescale_client.py")
filepath = os.path.normpath(filepath)

with open(filepath, "r", encoding="utf-8") as f:
    content = f.read()

# The problem: the marker match replaced the closing of a `text()` call.
# Find the broken section and fix it.

old = '''CREATE INDEX IF NOT EXISTS idx_scout_poison_source ON scout_poison_quarantine (source)
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
\"))'''

new = '''CREATE INDEX IF NOT EXISTS idx_scout_poison_source ON scout_poison_quarantine (source)"))
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
            )'''

if old in content:
    content = content.replace(old, new, 1)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)
    print("FIX APPLIED successfully")
else:
    print("OLD STRING NOT FOUND - trying to diagnose...")
    # Find the area around scout_signals
    idx = content.find("SCOUT_SIGNALS TABLE")
    if idx != -1:
        print(f"Found at index {idx}")
        print("Context (200 chars before, 300 after):")
        print(repr(content[idx-200:idx+300]))
    else:
        print("SCOUT_SIGNALS TABLE not found either")
