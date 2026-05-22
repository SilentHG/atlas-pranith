"""
Debug hash chain for audit_ledger: find the first violation and
compare field-by-field between seed-time and verify-time content.
"""
import asyncio
import hashlib
import json
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.sql import text
from atlas.config.settings import settings


def _compute_hash(content: dict) -> str:
    raw = json.dumps(content, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


async def debug():
    engine = create_async_engine(settings.database_url)
    async with engine.connect() as conn:
        # Get ALL audit entries ordered
        r = await conn.execute(
            text("""
                SELECT id, event_type, actor, action, resource_type, resource_id,
                       details, severity, trace_id, hash_prev, hash_self, created_at
                FROM audit_ledger ORDER BY created_at ASC
            """)
        )
        entries = r.fetchall()
        print(f"Total audit entries: {len(entries)}")

        prev_hash = None
        violations = []
        for i, entry in enumerate(entries):
            details = entry[6]
            if isinstance(details, str):
                try:
                    details = json.loads(details)
                except Exception:
                    details = {}

            raw_created = entry[11]
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
                "hash_prev": str(entry[9]) if entry[9] else None,
                "created_at": created_str,
            }
            expected = _compute_hash(content)
            actual = str(entry[10])

            if expected != actual:
                violations.append({
                    "index": i,
                    "id": str(entry[0])[:16],
                    "event_type": str(entry[1]),
                    "expected": expected,
                    "actual": actual,
                    "content": content,
                    "raw": {
                        "entry[0]": str(entry[0]),
                        "entry[1]": str(entry[1]),
                        "entry[2]": str(entry[2]),
                        "entry[3]": str(entry[3]),
                        "entry[4]": str(entry[4]),
                        "entry[5]": repr(entry[5]),
                        "entry[6]": repr(entry[6]),
                        "entry[7]": str(entry[7]),
                        "entry[8]": repr(entry[8]),
                        "entry[9]": repr(entry[9]),
                        "entry[10]": repr(entry[10]),
                        "entry[11]": repr(entry[11]),
                    }
                })
                if len(violations) >= 3:
                    break

            if i > 0:
                actual_prev = str(entry[9]) if entry[9] else None
                if actual_prev != prev_hash:
                    violations.append({
                        "index": i,
                        "id": str(entry[0])[:16],
                        "event_type": str(entry[1]),
                        "message": "CHAIN BROKEN",
                        "expected_prev": prev_hash,
                        "actual_prev": actual_prev,
                    })
                    if len(violations) >= 5:
                        break
            prev_hash = actual

    print(f"\nFound {len(violations)} violations (showing first up to 5)")
    for v in violations:
        print(f"\n{'='*80}")
        print(f"Violation at entry {v['index']} ({v.get('event_type', '?')})")
        print(f"  ID: {v.get('id', '?')}")
        if "message" in v:
            print(f"  {v['message']}")
            print(f"  expected prev_hash: {v.get('expected_prev', '?')}")
            print(f"  actual prev_hash:   {v.get('actual_prev', '?')}")
        else:
            print(f"  expected hash: {v['expected']}")
            print(f"  actual hash:   {v['actual']}")
            # Compare serialized JSON strings to find field differences
            serialized = json.dumps(v['content'], sort_keys=True, default=str)
            print(f"\n  Serialized content:\n  {serialized[:600]}")
            print(f"\n  Raw DB values:")
            for k, val in v['raw'].items():
                print(f"    {k}: {val}")
            
            # Check if the hash_self was stored as a different type
            print(f"\n  hash_self type: {type(entry[10]).__name__}")
            print(f"  hash_self repr: {repr(entry[10])}")
            print(f"  str(hash_self): {str(entry[10])}")
            
            # Also check if content dict has correct keys compared to seed
            print(f"\n  Content keys ({len(v['content'])}): {sorted(v['content'].keys())}")
            for k, val in sorted(v['content'].items()):
                print(f"    {k}: {type(val).__name__} = {repr(val)[:80]}")

    await engine.dispose()


asyncio.run(debug())
