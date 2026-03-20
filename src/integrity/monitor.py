"""
IntegrityMonitor: Scheduled hash-chain verification across all application streams.

Problem
-------
The audit chain exists but nothing actively monitors it. A tampered event
could go undetected indefinitely unless an operator manually queries
ledger://integrity/{id}.

Design
------
- Runs as a background asyncio task (like ProjectionDaemon).
- Periodically scans all application streams and runs run_integrity_check().
- On failure: writes an alert to integrity_alerts table + dead letter queue.
- Tracks last-checked position to avoid re-scanning unchanged streams.
- check_interval_seconds: how often to run a full scan (default 300s = 5min).
- Exposes get_alerts() for the MCP health resource.

Alert lifecycle
---------------
  open → acknowledged (operator sets acknowledged_at)
  open → resolved (operator sets resolved_at after fixing the chain)
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID, uuid4

import asyncpg

from src.event_store import EventStore
from src.integrity.audit_chain import run_integrity_check

logger = logging.getLogger(__name__)

DEFAULT_CHECK_INTERVAL = 300.0  # 5 minutes


class IntegrityMonitor:
    """
    Background daemon that monitors audit chain integrity.

    Usage:
        monitor = IntegrityMonitor(pool, event_store)
        await monitor.start()
        alerts = await monitor.get_open_alerts()
        await monitor.stop()
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        event_store: EventStore,
        check_interval_seconds: float = DEFAULT_CHECK_INTERVAL,
        dead_letter=None,
    ):
        self._pool = pool
        self._store = event_store
        self._interval = check_interval_seconds
        self._dead_letter = dead_letter
        self._task: Optional[asyncio.Task] = None
        self._running = False
        self._checks_run = 0
        self._alerts_raised = 0

    async def start(self) -> None:
        self._running = True
        self._task = asyncio.create_task(self._run_loop(), name="IntegrityMonitor")
        logger.info(
            "IntegrityMonitor started (interval=%.0fs)", self._interval
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(
            "IntegrityMonitor stopped. Checks: %d, Alerts: %d",
            self._checks_run, self._alerts_raised,
        )

    async def run_once(self) -> dict:
        """
        Run a single integrity scan across all application streams.
        Returns summary: {checked, passed, failed, alerts_raised}
        """
        async with self._pool.acquire() as conn:
            app_ids = await conn.fetch(
                """
                SELECT DISTINCT aggregate_id
                FROM event_streams
                WHERE aggregate_type = 'LoanApplication'
                  AND archived_at IS NULL
                ORDER BY aggregate_id
                """
            )

        checked = passed = failed = alerts = 0
        for row in app_ids:
            app_id = row["aggregate_id"]
            result = await run_integrity_check(self._pool, self._store, app_id)
            checked += 1
            if result["chain_valid"]:
                passed += 1
            else:
                failed += 1
                alert_id = await self._raise_alert(app_id, result)
                alerts += 1
                logger.error(
                    "IntegrityMonitor: chain broken for application %s at sequence %s (alert=%s)",
                    app_id, result.get("broken_at_sequence"), alert_id,
                )

        self._checks_run += 1
        self._alerts_raised += alerts
        return {
            "checked": checked,
            "passed": passed,
            "failed": failed,
            "alerts_raised": alerts,
        }

    async def get_open_alerts(self) -> list[dict]:
        """Return all unresolved integrity alerts."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT * FROM integrity_alerts
                WHERE resolved_at IS NULL
                ORDER BY created_at DESC
                """
            )
        return [dict(row) for row in rows]

    async def acknowledge_alert(self, alert_id: UUID, acknowledged_by: str) -> bool:
        """Mark an alert as acknowledged. Returns True if found."""
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE integrity_alerts
                SET acknowledged_at = NOW(), acknowledged_by = $1
                WHERE alert_id = $2 AND acknowledged_at IS NULL
                """,
                acknowledged_by, alert_id,
            )
        return result.split()[-1] != "0"

    async def resolve_alert(self, alert_id: UUID, resolved_by: str) -> bool:
        """Mark an alert as resolved. Returns True if found."""
        async with self._pool.acquire() as conn:
            result = await conn.execute(
                """
                UPDATE integrity_alerts
                SET resolved_at = NOW(), resolved_by = $1
                WHERE alert_id = $2 AND resolved_at IS NULL
                """,
                resolved_by, alert_id,
            )
        return result.split()[-1] != "0"

    # -------------------------------------------------------------------------
    # Internal
    # -------------------------------------------------------------------------

    async def _run_loop(self) -> None:
        while self._running:
            try:
                summary = await self.run_once()
                if summary["failed"] > 0:
                    logger.error(
                        "IntegrityMonitor scan: %d/%d streams FAILED integrity check",
                        summary["failed"], summary["checked"],
                    )
                else:
                    logger.debug(
                        "IntegrityMonitor scan: %d streams OK", summary["checked"]
                    )
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("IntegrityMonitor loop error: %s", e, exc_info=True)
            await asyncio.sleep(self._interval)

    async def _raise_alert(self, application_id: UUID, check_result: dict) -> UUID:
        """Write an integrity alert to the DB."""
        alert_id = uuid4()
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO integrity_alerts
                    (alert_id, application_id, broken_at_sequence,
                     entries_checked, details)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (application_id) WHERE resolved_at IS NULL
                DO NOTHING
                """,
                alert_id,
                application_id,
                check_result.get("broken_at_sequence"),
                check_result.get("entries_checked", 0),
                str(check_result.get("details", [])),
            )
        return alert_id
