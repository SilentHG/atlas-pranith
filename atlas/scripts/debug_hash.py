"""
Debug script: compare hash computation between seed-time and verify-time.
Uses raw SQLAlchemy to avoid import chain issues.
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
        # Get first event from event_store
        r = await conn.execute(
            text("""
                SELECT id, event_type, version, trace_id, parent_event_id,
                       aggregate_id, aggregate_type, data, metadata,
                       hash_prev, hash_self, created_at, sequence
                FROM event_store ORDER BY aggregate_id, sequence ASC LIMIT 1
            """)
        )
        ev = r.fetchone()
        if not ev:
            print("No events found!")
            return

        print("=== EVENT_STORE FIRST EVENT ===")
        print(f"id:           {ev[0]}")
        print(f"event_type:   '{ev[1]}' ({type(ev[1]).__name__})")
        print(f"version:      '{ev[2]}' ({type(ev[2]).__name__})")
        print(f"trace_id:     '{ev[3]}' ({type(ev[3]).__name__})")
        print(f"parent_event_id: {ev[4]!r} ({type(ev[4]).__name__})")
        print(f"aggregate_id: '{ev[5]}' ({type(ev[5]).__name__})")
        print(f"aggregate_type: '{ev[6]}' ({type(ev[6]).__name__})")
        print(f"data type:    {type(ev[7]).__name__}")
        print(f"metadata type: {type(ev[8]).__name__}")
        print(f"hash_prev:    {ev[9]!r} ({type(ev[9]).__name__})")
        print(f"hash_self:    {ev[10]!r}")
        print(f"created_at:   {ev[11]!r} ({type(ev[11]).__name__})")
        print(f"sequence:     {ev[12]} ({type(ev[12]).__name__})")

        raw_created = ev[11]
        created_str = raw_created.isoformat() if hasattr(raw_created, "isoformat") else str(raw_created)

        data_val = json.loads(ev[7]) if isinstance(ev[7], str) else ev[7]
        meta_val = json.loads(ev[8]) if isinstance(ev[8], str) else ev[8]

        content = {
            "id": str(ev[0]),
            "event_type": str(ev[1]),
            "version": str(ev[2]),
            "trace_id": str(ev[3]),
            "parent_event_id": str(ev[4]) if ev[4] else None,
            "aggregate_id": str(ev[5]),
            "aggregate_type": str(ev[6]),
            "data": data_val,
            "metadata": meta_val,
            "hash_prev": str(ev[9]) if ev[9] else None,
            "sequence": int(ev[12]),
            "created_at": created_str,
        }

        serialized = json.dumps(content, sort_keys=True, default=str)
        print(f"\nSerialized content for hash:\n{serialized[:500]}")

        expected = _compute_hash(content)
        actual = str(ev[10])
        print(f"\nExpected hash: {expected}")
        print(f"Actual hash:   {actual}")
        print(f"Match: {expected == actual}")

        if expected != actual:
            # Try the seed's hash computation to see if it matches
            print("\n--- Trying alternate hash computations ---")
            # Maybe parent_event_id should be None instead of empty string?
            print(f"\nparent_event_id repr: {ev[4]!r}")
            print(f"str(ev[4]) if ev[4] else None: {str(ev[4]) if ev[4] else None!r}")
            print(f"ev[4] if ev[4] else None: {ev[4] if ev[4] else None!r}")
            # Try different treatments of parent_event_id
            for pe in [ev[4], None]:
                for h in [ev[9], None]:
                    content2 = dict(content)
                    content2["parent_event_id"] = pe
                    content2["hash_prev"] = h
                    h2 = _compute_hash(content2)
                    if h2 == actual:
                        print(f"MATCH with parent_event_id={pe!r}, hash_prev={h!r}")

        # Print DB row values for all fields to spot None vs string differences
        print("\n\n=== RAW ROW VALUES ===")
        for i, col in enumerate(["id","event_type","version","trace_id","parent_event_id",
                                  "aggregate_id","aggregate_type","data","metadata",
                                  "hash_prev","hash_self","created_at","sequence"]):
            print(f"  {col}: {ev[i]!r}")

    # Also check audit_ledger
    async with engine.connect() as conn:
        r = await conn.execute(
            text("""
                SELECT id, event_type, actor, action, resource_type, resource_id,
                       details, severity, trace_id, hash_prev, hash_self, created_at
                FROM audit_ledger ORDER BY created_at ASC LIMIT 1
            """)
        )
        entry = r.fetchone()
        if entry:
            print("\n\n=== AUDIT_LEDGER FIRST ENTRY ===")
            details = entry[6]
            if isinstance(details, str):
                try:
                    details = json.loads(details)
                except Exception:
                    details = {}
            raw_created = entry[11]
            created_str = raw_created.isoformat() if hasattr(raw_created, "isoformat") else str(raw_created)

            content2 = {
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
            expected2 = _compute_hash(content2)
            actual2 = str(entry[10])
            print(f"Expected: {expected2}")
            print(f"Actual:   {actual2}")
            print(f"Match: {expected2 == actual2}")

    await engine.dispose()


asyncio.run(debug())
