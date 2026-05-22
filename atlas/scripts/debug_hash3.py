"""
Deep debug: show raw DB values for first 6 audit entries to spot the hash chain issue.
"""
import asyncio
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql import text
from atlas.config.settings import settings


async def debug():
    engine = create_async_engine(settings.database_url)
    async with engine.connect() as conn:
        r = await conn.execute(
            text("""
                SELECT id, event_type, actor, action, resource_type, resource_id,
                       details, severity, trace_id, hash_prev, hash_self, created_at
                FROM audit_ledger ORDER BY created_at ASC LIMIT 8
            """)
        )
        entries = r.fetchall()
        
        print("=== FIRST 8 AUDIT ENTRIES ===\n")
        
        prev_stored = None
        for i, e in enumerate(entries):
            print(f"--- Entry {i} ---")
            print(f"  id:           {e[0]}")
            print(f"  event_type:   {e[1]}")
            print(f"  actor:        {e[2]}")
            print(f"  hash_prev:    {e[9]}")
            print(f"  hash_self:    {e[10]}")
            print(f"  created_at:   {e[11]}")
            
            if i > 0:
                expected_prev = prev_stored
                actual_prev = str(e[9]) if e[9] else None
                match = actual_prev == expected_prev
                print(f"  expected_prev: {expected_prev}")
                print(f"  actual_prev:   {actual_prev}")
                print(f"  prev match:    {match}")
                if not match:
                    print(f"  *** CHAIN BROKEN ***")
            
            prev_stored = str(e[10])
            
            # Show the serialized hash content
            import hashlib, json
            details = e[6]
            if isinstance(details, str):
                try:
                    details = json.loads(details)
                except:
                    details = {}
            
            raw_created = e[11]
            created_str = raw_created.isoformat() if hasattr(raw_created, "isoformat") else str(raw_created)
            
            content = {
                "id": str(e[0]),
                "event_type": str(e[1]),
                "actor": str(e[2]),
                "action": str(e[3]),
                "resource_type": str(e[4]),
                "resource_id": str(e[5]) if e[5] else None,
                "details": details,
                "severity": str(e[7]),
                "trace_id": str(e[8]) if e[8] else None,
                "hash_prev": str(e[9]) if e[9] else None,
                "created_at": created_str,
            }
            serialized = json.dumps(content, sort_keys=True, default=str)
            computed = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
            stored = str(e[10])
            print(f"  serialized:   {serialized[:150]}...")
            print(f"  computed:     {computed}")
            print(f"  stored:       {stored}")
            print(f"  self match:   {computed == stored}")
            print()

    await engine.dispose()


asyncio.run(debug())
