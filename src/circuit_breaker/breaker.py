"""
CircuitBreaker: Protects the outbox relay (and any async call) from hammering
a downstream system that is unavailable.

States
------
  CLOSED   — normal operation, calls pass through
  OPEN     — downstream is down, calls fail fast with CircuitOpenError
  HALF_OPEN — probe state: one call is allowed through to test recovery

Transitions
-----------
  CLOSED  → OPEN      : failure_threshold consecutive failures
  OPEN    → HALF_OPEN : after recovery_timeout_seconds
  HALF_OPEN → CLOSED  : probe call succeeds
  HALF_OPEN → OPEN    : probe call fails (reset timer)

Usage
-----
    breaker = CircuitBreaker("sns-publisher", failure_threshold=5, recovery_timeout_seconds=30)

    async def publish():
        async with breaker:
            await sns.publish(...)

    # Or wrap a coroutine:
    result = await breaker.call(my_coroutine())
"""
from __future__ import annotations

import asyncio
import logging
import time
from enum import Enum
from typing import Any, Awaitable, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(str, Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitOpenError(Exception):
    """Raised when a call is attempted while the circuit is OPEN."""
    def __init__(self, name: str, retry_after: float):
        self.name = name
        self.retry_after = retry_after
        super().__init__(
            f"Circuit '{name}' is OPEN. Retry after {retry_after:.1f}s."
        )


class CircuitBreaker:
    """
    Async circuit breaker.

    Args:
        name: Identifier for logging/metrics.
        failure_threshold: Consecutive failures before opening.
        recovery_timeout_seconds: How long to wait before probing (HALF_OPEN).
        success_threshold: Consecutive successes in HALF_OPEN before closing.
    """

    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout_seconds: float = 30.0,
        success_threshold: int = 2,
    ):
        self.name = name
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout_seconds
        self._success_threshold = success_threshold

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._opened_at: float | None = None
        self._lock = asyncio.Lock()

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def failure_count(self) -> int:
        return self._failure_count

    async def call(self, coro: Awaitable[T]) -> T:
        """Execute a coroutine through the circuit breaker."""
        async with self._lock:
            await self._maybe_transition()
            if self._state == CircuitState.OPEN:
                retry_after = self._recovery_timeout - (time.monotonic() - self._opened_at)
                raise CircuitOpenError(self.name, max(0.0, retry_after))

        try:
            result = await coro
            async with self._lock:
                await self._on_success()
            return result
        except CircuitOpenError:
            raise
        except Exception as e:
            async with self._lock:
                await self._on_failure(e)
            raise

    async def __aenter__(self) -> "CircuitBreaker":
        async with self._lock:
            await self._maybe_transition()
            if self._state == CircuitState.OPEN:
                retry_after = self._recovery_timeout - (time.monotonic() - self._opened_at)
                raise CircuitOpenError(self.name, max(0.0, retry_after))
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        async with self._lock:
            if exc_type is None or exc_type is CircuitOpenError:
                if exc_type is None:
                    await self._on_success()
            else:
                await self._on_failure(exc_val)
        return False  # don't suppress exceptions

    def reset(self) -> None:
        """Manually reset to CLOSED (operator action)."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._opened_at = None
        logger.info("Circuit '%s' manually reset to CLOSED", self.name)

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "state": self._state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "opened_at": self._opened_at,
        }

    # -------------------------------------------------------------------------
    # Internal
    # -------------------------------------------------------------------------

    async def _maybe_transition(self) -> None:
        """Check if OPEN → HALF_OPEN transition is due."""
        if self._state == CircuitState.OPEN and self._opened_at is not None:
            elapsed = time.monotonic() - self._opened_at
            if elapsed >= self._recovery_timeout:
                self._state = CircuitState.HALF_OPEN
                self._success_count = 0
                logger.info(
                    "Circuit '%s' OPEN → HALF_OPEN after %.1fs", self.name, elapsed
                )

    async def _on_success(self) -> None:
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self._success_threshold:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._opened_at = None
                logger.info("Circuit '%s' HALF_OPEN → CLOSED", self.name)
        elif self._state == CircuitState.CLOSED:
            self._failure_count = 0  # reset on any success

    async def _on_failure(self, exc: Exception) -> None:
        self._failure_count += 1
        if self._state == CircuitState.HALF_OPEN:
            # Probe failed — reopen
            self._state = CircuitState.OPEN
            self._opened_at = time.monotonic()
            self._success_count = 0
            logger.warning(
                "Circuit '%s' HALF_OPEN → OPEN (probe failed: %s)", self.name, exc
            )
        elif self._state == CircuitState.CLOSED:
            if self._failure_count >= self._failure_threshold:
                self._state = CircuitState.OPEN
                self._opened_at = time.monotonic()
                logger.error(
                    "Circuit '%s' CLOSED → OPEN after %d failures. Last: %s",
                    self.name, self._failure_count, exc,
                )
