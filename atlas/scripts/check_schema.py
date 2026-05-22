"""Check column schemas for runtime error investigation."""
import asyncio
from atlas.data.storage.timescale_client import TimescaleClient
from atlas.config.settings import settings
from sqlalchemy.sql import text

async def check():
    db = TimescaleClient(settings.database_url)
    await db.connect()
    async with db.engine.connect() as conn:
        for tbl in ("paper_trades", "strategies", "mutation_memory", "feature_importance"):
            r = await conn.execute(text(f"""
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_name = '{tbl}'
                ORDER BY ordinal_position
            """))
            print(f"=== {tbl} ===")
            for row in r.fetchall():
                print(f"  {row[0]:25s} {row[1]}")
            print()

asyncio.run(check())
