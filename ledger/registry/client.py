"""
ledger/registry/client.py
Applicant Registry Client (Read-Only)
"""
import asyncpg
from typing import List, Dict, Any
from uuid import UUID

class ApplicantRegistryClient:
    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool
        
    async def get_company(self, company_id: UUID) -> Dict[str, Any] | None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM applicant_registry.companies WHERE company_id = $1", company_id
            )
            return dict(row) if row else None
            
    async def get_financial_history(self, company_id: UUID) -> List[Dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM applicant_registry.financial_history WHERE company_id = $1 ORDER BY year DESC", company_id
            )
            return [dict(r) for r in rows]
            
    async def get_compliance_flags(self, company_id: UUID) -> List[Dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM applicant_registry.compliance_flags WHERE company_id = $1", company_id
            )
            return [dict(r) for r in rows]
            
    async def get_loan_relationships(self, company_id: UUID) -> List[Dict[str, Any]]:
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM applicant_registry.loan_relationships WHERE company_id = $1", company_id
            )
            return [dict(r) for r in rows]
