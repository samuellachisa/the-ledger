#!/usr/bin/env python
"""Apply schema.sql to the target database. Reads DATABASE_URL from .env."""
import asyncio
import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))


async def main():
    from dotenv import load_dotenv
    load_dotenv()

    import asyncpg
    url = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/apex_financial")
    schema = (ROOT / "src" / "schema.sql").read_text()

    print(f"Connecting to {url} ...")
    conn = await asyncpg.connect(url)
    try:
        await conn.execute(schema)
        print("Schema applied successfully.")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
