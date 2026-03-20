"""
TenantContext: Multi-tenancy support via Python contextvars.

Design
------
- tenant_id is propagated via contextvars (async-safe, no thread-local issues).
- EventStore.append() and load_stream() read from context automatically.
- TokenStore includes tenant_id in issued tokens.
- All new tables have a tenant_id column for row-level isolation.

Usage
-----
    with TenantContext("tenant-abc"):
        await event_store.append(...)   # automatically scoped to tenant-abc

    # Or manually:
    token = set_tenant_id("tenant-abc")
    try:
        ...
    finally:
        token.var.reset(token)
"""
from __future__ import annotations

from contextvars import ContextVar, Token
from typing import Optional

_tenant_id_var: ContextVar[Optional[str]] = ContextVar("tenant_id", default=None)


def get_tenant_id() -> Optional[str]:
    """Return the current tenant_id from context, or None if not set."""
    return _tenant_id_var.get()


def set_tenant_id(tenant_id: Optional[str]) -> Token:
    """Set the tenant_id for the current async context. Returns a reset token."""
    return _tenant_id_var.set(tenant_id)


class TenantContext:
    """
    Context manager that sets tenant_id for the duration of a block.

    Usage:
        with TenantContext("tenant-abc"):
            await do_something()
    """

    def __init__(self, tenant_id: str):
        self._tenant_id = tenant_id
        self._token: Optional[Token] = None

    def __enter__(self) -> "TenantContext":
        self._token = set_tenant_id(self._tenant_id)
        return self

    def __exit__(self, *_) -> None:
        if self._token is not None:
            _tenant_id_var.reset(self._token)

    async def __aenter__(self) -> "TenantContext":
        return self.__enter__()

    async def __aexit__(self, *args) -> None:
        self.__exit__(*args)
