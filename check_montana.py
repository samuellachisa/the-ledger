#!/usr/bin/env python3
import asyncio
import asyncpg

async def main():
    pool = await asyncpg.create_pool('postgresql://postgres:12345@localhost:5432/apex_financial')
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM applicant_registry.companies WHERE state = 'MT' LIMIT 1")
        print('Company data:')
        for key, value in dict(row).items():
            print(f'  {key}: {value}')
    await pool.close()

if __name__ == '__main__':
    asyncio.run(main())
