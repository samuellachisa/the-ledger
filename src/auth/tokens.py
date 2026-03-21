"""
Agent token authentication for MCP tool calls.

Problem
-------
MCP tools accept agent_id and officer_id as plain strings with no verification.
Any caller can impersonate any agent. For a financial services system this is
a real gap — there's no way to enforce that the agent calling generate_decision
is the same one that loaded context via start_agent_session.

Design
------
- Tokens are issued via issue_token() (called by an admin/bootstrap process)
- Raw token is a 32-byte random hex string (never stored)
- token_hash = SHA-256(raw_token) is stored in agent_tokens table
- MCP callers pass the raw token in the Authorization header or as a tool arg
- verify() hashes the presented token and looks it up
- AgentIdentity carries agent_id + roles for downstream authorization checks

Roles
-----
  decision_agent  — can call generate_decision, start_agent_session
  compliance_officer — can call record_compliance_check, finalize_compliance
  submitter       — can call submit_application, record_credit_analysis, record_fraud_check
  read_only       — can only read resources
  admin           — all of the above + issue/revoke tokens

Authorization
-------------
Each MCP tool declares its required role. The MCP server calls
auth.require(identity, role) before executing the tool body.
"""
from __future__ import annotations

import hashlib
import logging
import os
import secrets
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional
from uuid import UUID

import asyncpg

logger = logging.getLogger(__name__)

TOKEN_TTL_HOURS = 24


class Role(str, Enum):
    DECISION_AGENT = "decision_agent"
    COMPLIANCE_OFFICER = "compliance_officer"
    SUBMITTER = "submitter"
    READ_ONLY = "read_only"
    ADMIN = "admin"


# Maps each MCP tool name to the minimum required role
TOOL_ROLES: dict[str, Role] = {
    "submit_application": Role.SUBMITTER,
    "record_credit_analysis": Role.SUBMITTER,
    "record_fraud_check": Role.SUBMITTER,
    "request_compliance_check": Role.COMPLIANCE_OFFICER,
    "record_compliance_check": Role.COMPLIANCE_OFFICER,
    "finalize_compliance": Role.COMPLIANCE_OFFICER,
    "generate_decision": Role.DECISION_AGENT,
    "finalize_application": Role.DECISION_AGENT,
}

# Role hierarchy: higher roles include lower ones
ROLE_HIERARCHY: dict[Role, set[Role]] = {
    Role.ADMIN: {Role.DECISION_AGENT, Role.COMPLIANCE_OFFICER, Role.SUBMITTER, Role.READ_ONLY},
    Role.DECISION_AGENT: {Role.READ_ONLY},
    Role.COMPLIANCE_OFFICER: {Role.READ_ONLY},
    Role.SUBMITTER: {Role.READ_ONLY},
    Role.READ_ONLY: set(),
}


class AuthError(Exception):
    def __init__(self, message: str, code: str = "Unauthorized"):
        self.code = code
        super().__init__(message)


@dataclass
class AgentIdentity:
    token_id: UUID
    agent_id: str
    roles: list[Role]

    def has_role(self, required: Role) -> bool:
        if required in self.roles:
            return True
        # Check hierarchy
        for role in self.roles:
            if required in ROLE_HIERARCHY.get(role, set()):
                return True
        return False


class TokenStore:
    """
    Issues, verifies, and revokes agent tokens.

    Usage:
        store = TokenStore(pool)
        raw_token = await store.issue_token("agent-001", [Role.DECISION_AGENT])
        identity = await store.verify(raw_token)
        store.require(identity, "generate_decision")  # raises AuthError if unauthorized
    """

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    async def issue_token(
        self,
        agent_id: str,
        roles: list[Role],
        ttl_hours: int = TOKEN_TTL_HOURS,
    ) -> str:
        """
        Issue a new token for an agent. Returns the raw token (show once, never stored).
        """
        raw_token = secrets.token_hex(32)
        token_hash = self._hash(raw_token)
        expires_at = datetime.now(timezone.utc) + timedelta(hours=ttl_hours)
        roles_json = [r.value for r in roles]

        async with self._pool.acquire() as conn:
            async with conn.transaction():
                token_id = await conn.fetchval(
                    """
                    INSERT INTO agent_tokens (token_hash, agent_id, roles, expires_at)
                    VALUES ($1, $2, $3, $4)
                    RETURNING token_id
                    """,
                    token_hash, agent_id, roles_json, expires_at,
                )
                await conn.execute(
                    "INSERT INTO auth_audit_log (event_type, agent_id, token_id, metadata) "
                    "VALUES ('issued', $1, $2, $3)",
                    agent_id, token_id, {"roles": roles_json},
                )
        logger.info("Token issued for agent %s with roles %s", agent_id, roles_json)
        return raw_token
    async def verify(self, raw_token: str) -> AgentIdentity:
        """
        Verify a raw token and return the agent's identity.
        Raises AuthError if token is invalid, expired, or revoked.
        """
        token_hash = self._hash(raw_token)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT token_id, agent_id, roles, expires_at, revoked_at
                FROM agent_tokens
                WHERE token_hash = $1
                """,
                token_hash,
            )

        if row is None:
            # Audit failed attempt (no agent_id known)
            try:
                async with self._pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO auth_audit_log (event_type, failure_reason) VALUES ('failed', 'InvalidToken')"
                    )
            except Exception:
                pass
            raise AuthError("Invalid token", "InvalidToken")
        if row["revoked_at"] is not None:
            try:
                async with self._pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO auth_audit_log (event_type, agent_id, token_id, failure_reason) "
                        "VALUES ('failed', $1, $2, 'TokenRevoked')",
                        row["agent_id"], row["token_id"],
                    )
            except Exception:
                pass
            raise AuthError("Token has been revoked", "TokenRevoked")
        if row["expires_at"] < datetime.now(timezone.utc):
            try:
                async with self._pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO auth_audit_log (event_type, agent_id, token_id, failure_reason) "
                        "VALUES ('failed', $1, $2, 'TokenExpired')",
                        row["agent_id"], row["token_id"],
                    )
            except Exception:
                pass
            raise AuthError("Token has expired", "TokenExpired")

        # Update last_used_at (best-effort, don't fail auth if this fails)
        try:
            async with self._pool.acquire() as conn:
                await conn.execute(
                    "UPDATE agent_tokens SET last_used_at = NOW() WHERE token_id = $1",
                    row["token_id"],
                )
                await conn.execute(
                    "INSERT INTO auth_audit_log (event_type, agent_id, token_id) VALUES ('verified', $1, $2)",
                    row["agent_id"], row["token_id"],
                )
        except Exception:
            pass

        roles = [Role(r) for r in (row["roles"] or [])]
        return AgentIdentity(
            token_id=row["token_id"],
            agent_id=row["agent_id"],
            roles=roles,
        )

    async def refresh(self, raw_token: str, ttl_hours: int = TOKEN_TTL_HOURS) -> str:
        """
        Exchange a valid (non-expired, non-revoked) token for a fresh one.

        The old token is revoked atomically. The new token inherits the same
        agent_id and roles. Returns the new raw token.

        Raises AuthError if the presented token is invalid, expired, or revoked.
        """
        identity = await self.verify(raw_token)  # validates current token

        # Issue new token with same agent_id and roles
        new_raw = await self.issue_token(
            agent_id=identity.agent_id,
            roles=identity.roles,
            ttl_hours=ttl_hours,
        )

        # Record parent relationship and revoke old token atomically
        new_hash = self._hash(new_raw)
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                # Set parent_token_id on the new token
                await conn.execute(
                    """
                    UPDATE agent_tokens
                    SET parent_token_id = $1
                    WHERE token_hash = $2
                    """,
                    identity.token_id, new_hash,
                )
                # Revoke the old token
                await conn.execute(
                    "UPDATE agent_tokens SET revoked_at = NOW() WHERE token_id = $1",
                    identity.token_id,
                )
                await conn.execute(
                    "INSERT INTO auth_audit_log (event_type, agent_id, token_id) VALUES ('refreshed', $1, $2)",
                    identity.agent_id, identity.token_id,
                )

        logger.info(
            "Token refreshed for agent %s (old=%s)", identity.agent_id, identity.token_id
        )
        return new_raw

    async def revoke(self, token_id: UUID, revoked_by: str) -> bool:
        """Revoke a token. Returns True if found and revoked."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT agent_id FROM agent_tokens WHERE token_id = $1", token_id
                )
                result = await conn.execute(
                    "UPDATE agent_tokens SET revoked_at = NOW() "
                    "WHERE token_id = $1 AND revoked_at IS NULL",
                    token_id,
                )
                revoked = result.split()[-1] != "0"
                if revoked and row:
                    await conn.execute(
                        "INSERT INTO auth_audit_log (event_type, agent_id, token_id, metadata) "
                        "VALUES ('revoked', $1, $2, $3)",
                        row["agent_id"], token_id, {"revoked_by": revoked_by},
                    )
        if revoked:
            logger.info("Token %s revoked by %s", token_id, revoked_by)
        return revoked

    def require(self, identity: AgentIdentity, tool_name: str) -> None:
        """
        Assert that identity has the required role for a tool.
        Raises AuthError if not authorized.
        """
        required = TOOL_ROLES.get(tool_name)
        if required is None:
            return  # Unknown tool — let the tool handler deal with it
        if not identity.has_role(required):
            raise AuthError(
                f"Agent '{identity.agent_id}' lacks role '{required.value}' "
                f"required for tool '{tool_name}'. Has: {[r.value for r in identity.roles]}",
                code="Forbidden",
            )

    @staticmethod
    def _hash(raw_token: str) -> str:
        return hashlib.sha256(raw_token.encode()).hexdigest()
