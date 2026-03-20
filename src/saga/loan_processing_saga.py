"""
LoanProcessingSaga: Durable process manager for the loan application workflow.

Problem this solves
-------------------
handle_generate_decision (and previously handle_finalize_compliance) manually
orchestrated multi-aggregate workflows inline. If the process crashed between
steps there was no recovery path — the application would be stuck in an
intermediate state forever.

Design
------
The SagaManager runs as a background task (like ProjectionDaemon) and reacts
to domain events by issuing commands. Saga state is persisted in the
`saga_instances` table so it survives restarts.

Workflow automated by this saga
--------------------------------
  FraudCheckCompleted
      └─► [saga] request_compliance_check  (UnderReview → ComplianceCheck)

  ComplianceRecordFinalized
      └─► [saga] finalize_compliance       (ComplianceCheck → PendingDecision)

The generate_decision and finalize_application steps remain explicit MCP tool
calls — they require agent judgment and cannot be automated.

Idempotency
-----------
Each saga step is guarded by a `step` field in saga_instances. The saga will
not re-issue a command for a step it has already completed. This makes the
handler safe to call multiple times (e.g. after a crash-and-replay).

Correlation
-----------
Commands issued by the saga carry the saga_id as correlation_id and the
triggering event_id as causation_id, preserving the full causal chain.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Optional
from uuid import UUID, uuid4

import asyncpg

from src.commands.handlers import (
    CommandHandler,
    FinalizeComplianceCommand,
    RequestComplianceCheckCommand,
)
from src.event_store import EventStore, StoredEvent
from src.models.events import (
    InvalidStateTransitionError,
    OptimisticConcurrencyError,
)
from src.dead_letter.queue import DeadLetterQueue

logger = logging.getLogger(__name__)

POLL_INTERVAL_SECONDS = 0.1
MAX_STEP_RETRIES = 5


class SagaStep(str, Enum):
    """Ordered steps in the loan processing saga."""
    STARTED = "started"
    COMPLIANCE_REQUESTED = "compliance_requested"
    COMPLIANCE_FINALIZED = "compliance_finalized"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class SagaConfig:
    """
    Default parameters injected by the saga when issuing automated commands.
    In production these would come from a configuration service or the
    triggering event's metadata.
    """
    default_officer_id: str = "system-saga"
    default_required_checks: list[str] | None = None

    def __post_init__(self):
        if self.default_required_checks is None:
            self.default_required_checks = ["AML", "KYC", "OFAC"]


class LoanProcessingSaga:
    """
    Handles a single loan application's saga lifecycle.

    Instantiated by SagaManager when a relevant event is observed.
    State is loaded from / persisted to the `saga_instances` table.
    """

    def __init__(
        self,
        saga_id: UUID,
        application_id: UUID,
        step: SagaStep,
        compliance_record_id: Optional[UUID],
        last_causation_id: Optional[UUID],
        retry_count: int,
    ):
        self.saga_id = saga_id
        self.application_id = application_id
        self.step = step
        self.compliance_record_id = compliance_record_id
        self.last_causation_id = last_causation_id
        self.retry_count = retry_count

    @classmethod
    def new(cls, application_id: UUID) -> "LoanProcessingSaga":
        return cls(
            saga_id=uuid4(),
            application_id=application_id,
            step=SagaStep.STARTED,
            compliance_record_id=None,
            last_causation_id=None,
            retry_count=0,
        )

    @classmethod
    def from_row(cls, row: asyncpg.Record) -> "LoanProcessingSaga":
        return cls(
            saga_id=row["saga_id"],
            application_id=row["application_id"],
            step=SagaStep(row["step"]),
            compliance_record_id=row["compliance_record_id"],
            last_causation_id=row["last_causation_id"],
            retry_count=row["retry_count"],
        )


class SagaManager:
    """
    Background daemon that drives LoanProcessingSaga instances forward.

    Polls the event store for new events and reacts to:
      - FraudCheckCompleted  → request_compliance_check
      - ComplianceRecordFinalized → finalize_compliance

    Usage:
        manager = SagaManager(pool, event_store, handler, config)
        await manager.start()
        # ... later ...
        await manager.stop()
    """

    CHECKPOINT_KEY = "LoanProcessingSaga"

    def __init__(
        self,
        pool: asyncpg.Pool,
        event_store: EventStore,
        handler: CommandHandler,
        config: Optional[SagaConfig] = None,
        dead_letter: Optional[DeadLetterQueue] = None,
    ):
        self._pool = pool
        self._store = event_store
        self._handler = handler
        self._config = config or SagaConfig()
        self._dead_letter = dead_letter
        self._task: asyncio.Task | None = None
        self._running = False
        self._last_position: int = 0
        self._batch_done = asyncio.Event()

    async def start(self) -> None:
        self._running = True
        self._batch_done.set()
        await self._load_checkpoint()
        self._task = asyncio.create_task(self._run_loop(), name="SagaManager")
        logger.info("SagaManager started from position %d", self._last_position)

    async def stop(self, drain_timeout_seconds: float = 5.0) -> None:
        """
        Gracefully stop the saga manager.
        Waits up to drain_timeout_seconds for the current batch to finish.
        """
        self._running = False
        try:
            await asyncio.wait_for(self._batch_done.wait(), timeout=drain_timeout_seconds)
        except asyncio.TimeoutError:
            logger.warning("SagaManager drain timed out after %.1fs", drain_timeout_seconds)
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("SagaManager stopped")

    # -------------------------------------------------------------------------
    # Internal loop
    # -------------------------------------------------------------------------

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self._process_batch()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("SagaManager loop error: %s", e, exc_info=True)
            await asyncio.sleep(POLL_INTERVAL_SECONDS)

    async def _process_batch(self) -> None:
        events = await self._store.load_all(
            after_position=self._last_position,
            limit=500,
        )
        if not events:
            return

        self._batch_done.clear()
        try:
            for event in events:
                await self._dispatch(event)
                await self._advance_checkpoint(event.global_position)
        finally:
            self._batch_done.set()

    async def _dispatch(self, event: StoredEvent) -> None:
        """Route an event to the appropriate saga handler."""
        if event.event_type == "FraudCheckCompleted":
            await self._on_fraud_check_completed(event)
        elif event.event_type == "ComplianceRecordFinalized":
            await self._on_compliance_record_finalized(event)
        elif event.event_type == "ApplicationWithdrawn":
            await self._on_application_withdrawn(event)

    # -------------------------------------------------------------------------
    # Saga step handlers
    # -------------------------------------------------------------------------

    async def _on_fraud_check_completed(self, event: StoredEvent) -> None:
        """
        FraudCheckCompleted → request_compliance_check.

        The application is now in UnderReview. The saga automatically
        initiates the compliance check so the agent doesn't have to.
        """
        application_id = event.aggregate_id

        saga = await self._load_or_create_saga(application_id)

        # Idempotency: skip if we already issued this step
        if saga.step != SagaStep.STARTED:
            logger.debug(
                "Saga %s already past STARTED (at %s), skipping compliance request",
                saga.saga_id, saga.step,
            )
            return

        logger.info(
            "Saga %s: FraudCheckCompleted for application %s — requesting compliance check",
            saga.saga_id, application_id,
        )

        for attempt in range(MAX_STEP_RETRIES):
            try:
                compliance_id = await self._handler.handle_request_compliance_check(
                    RequestComplianceCheckCommand(
                        application_id=application_id,
                        officer_id=self._config.default_officer_id,
                        required_checks=list(self._config.default_required_checks),
                    )
                )
                saga.compliance_record_id = compliance_id
                saga.step = SagaStep.COMPLIANCE_REQUESTED
                saga.last_causation_id = event.event_id
                saga.retry_count = 0
                await self._save_saga(saga)
                logger.info(
                    "Saga %s: compliance check requested, record_id=%s",
                    saga.saga_id, compliance_id,
                )
                return
            except InvalidStateTransitionError as e:
                # Application may have already moved past UnderReview (e.g. manual call)
                logger.warning(
                    "Saga %s: InvalidStateTransition on compliance request (attempt %d): %s",
                    saga.saga_id, attempt + 1, e,
                )
                # Mark as completed — manual path took over
                saga.step = SagaStep.COMPLETED
                await self._save_saga(saga)
                return
            except OptimisticConcurrencyError:
                if attempt < MAX_STEP_RETRIES - 1:
                    await asyncio.sleep(0.01 * (2 ** attempt))
                else:
                    saga.step = SagaStep.FAILED
                    saga.retry_count = attempt + 1
                    await self._save_saga(saga)
                    if self._dead_letter:
                        await self._dead_letter.write(
                            event=event, source="saga",
                            processor_name="LoanProcessingSaga.compliance_request",
                            retry_count=attempt + 1, error=OptimisticConcurrencyError(
                                application_id, MAX_STEP_RETRIES, -1),
                        )
                    logger.error(
                        "Saga %s: exhausted retries requesting compliance for %s",
                        saga.saga_id, application_id,
                    )

    async def _on_compliance_record_finalized(self, event: StoredEvent) -> None:
        """
        ComplianceRecordFinalized → finalize_compliance on the loan application.

        The compliance record aggregate has finished. The saga now calls
        finalize_compliance to transition the loan application to PendingDecision.
        """
        compliance_record_id = event.aggregate_id

        # Find the saga that owns this compliance record
        saga = await self._find_saga_by_compliance_record(compliance_record_id)
        if saga is None:
            # Compliance record not managed by this saga (manual path)
            logger.debug(
                "No saga found for compliance_record_id=%s, skipping auto-finalize",
                compliance_record_id,
            )
            return

        # Idempotency: skip if already finalized
        if saga.step not in (SagaStep.COMPLIANCE_REQUESTED,):
            logger.debug(
                "Saga %s already past COMPLIANCE_REQUESTED (at %s), skipping finalize",
                saga.saga_id, saga.step,
            )
            return

        logger.info(
            "Saga %s: ComplianceRecordFinalized for record %s — finalizing compliance on application %s",
            saga.saga_id, compliance_record_id, saga.application_id,
        )

        for attempt in range(MAX_STEP_RETRIES):
            try:
                await self._handler.handle_finalize_compliance(
                    FinalizeComplianceCommand(
                        compliance_record_id=compliance_record_id,
                        application_id=saga.application_id,
                        officer_id=self._config.default_officer_id,
                        notes="Auto-finalized by LoanProcessingSaga",
                    )
                )
                saga.step = SagaStep.COMPLIANCE_FINALIZED
                saga.last_causation_id = event.event_id
                saga.retry_count = 0
                await self._save_saga(saga)
                logger.info(
                    "Saga %s: compliance finalized, application %s now PendingDecision",
                    saga.saga_id, saga.application_id,
                )
                return
            except InvalidStateTransitionError as e:
                logger.warning(
                    "Saga %s: InvalidStateTransition on finalize_compliance (attempt %d): %s",
                    saga.saga_id, attempt + 1, e,
                )
                saga.step = SagaStep.COMPLETED
                await self._save_saga(saga)
                return
            except OptimisticConcurrencyError:
                if attempt < MAX_STEP_RETRIES - 1:
                    await asyncio.sleep(0.01 * (2 ** attempt))
                else:
                    saga.step = SagaStep.FAILED
                    saga.retry_count = attempt + 1
                    await self._save_saga(saga)
                    if self._dead_letter:
                        await self._dead_letter.write(
                            event=event, source="saga",
                            processor_name="LoanProcessingSaga.finalize_compliance",
                            retry_count=attempt + 1, error=OptimisticConcurrencyError(
                                saga.application_id, MAX_STEP_RETRIES, -1),
                        )
                    logger.error(
                        "Saga %s: exhausted retries finalizing compliance for %s",
                        saga.saga_id, saga.application_id,
                    )

    # -------------------------------------------------------------------------
    # Persistence
    # -------------------------------------------------------------------------

    async def _on_application_withdrawn(self, event: StoredEvent) -> None:
        """
        ApplicationWithdrawn → cancel in-flight compliance record.

        If an application is withdrawn while in ComplianceCheck, the
        ComplianceRecord aggregate is left orphaned in InProgress state.
        The saga cancels it by finalizing it with a cancellation note.
        """
        application_id = event.aggregate_id
        saga = await self._find_saga_by_application(application_id)
        if saga is None or saga.compliance_record_id is None:
            return

        if saga.step not in (SagaStep.COMPLIANCE_REQUESTED,):
            return

        logger.info(
            "Saga %s: ApplicationWithdrawn for %s — cancelling compliance record %s",
            saga.saga_id, application_id, saga.compliance_record_id,
        )

        from src.aggregates.compliance_record import ComplianceRecordAggregate
        from src.models.events import ComplianceStatus

        try:
            comp_events = await self._store.load_stream(
                ComplianceRecordAggregate.aggregate_type, saga.compliance_record_id
            )
            comp = ComplianceRecordAggregate.load(saga.compliance_record_id, comp_events)

            if comp.status != ComplianceStatus.IN_PROGRESS:
                saga.step = SagaStep.COMPLETED
                await self._save_saga(saga)
                return

            comp.finalize(
                officer_id=self._config.default_officer_id,
                notes="Cancelled: application withdrawn",
            )
            await self._store.append(
                aggregate_type=ComplianceRecordAggregate.aggregate_type,
                aggregate_id=saga.compliance_record_id,
                events=comp.pending_events,
                expected_version=comp.version - len(comp.pending_events),
            )
            saga.step = SagaStep.COMPLETED
            await self._save_saga(saga)
            logger.info(
                "Saga %s: compliance record %s cancelled due to withdrawal",
                saga.saga_id, saga.compliance_record_id,
            )
        except Exception as e:
            logger.error(
                "Saga %s: failed to cancel compliance record on withdrawal: %s",
                saga.saga_id, e, exc_info=True,
            )

    async def _find_saga_by_application(
        self, application_id: UUID
    ) -> Optional[LoanProcessingSaga]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM saga_instances WHERE application_id = $1",
                application_id,
            )
            return LoanProcessingSaga.from_row(row) if row else None

    async def _load_or_create_saga(self, application_id: UUID) -> LoanProcessingSaga:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM saga_instances WHERE application_id = $1",
                application_id,
            )
            if row:
                return LoanProcessingSaga.from_row(row)

            saga = LoanProcessingSaga.new(application_id)
            await conn.execute(
                """
                INSERT INTO saga_instances
                    (saga_id, application_id, step, compliance_record_id,
                     last_causation_id, retry_count)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (application_id) DO NOTHING
                """,
                saga.saga_id, saga.application_id, saga.step.value,
                saga.compliance_record_id, saga.last_causation_id, saga.retry_count,
            )
            # Re-fetch in case of concurrent insert
            row = await conn.fetchrow(
                "SELECT * FROM saga_instances WHERE application_id = $1",
                application_id,
            )
            return LoanProcessingSaga.from_row(row)

    async def _find_saga_by_compliance_record(
        self, compliance_record_id: UUID
    ) -> Optional[LoanProcessingSaga]:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM saga_instances WHERE compliance_record_id = $1",
                compliance_record_id,
            )
            return LoanProcessingSaga.from_row(row) if row else None

    async def _save_saga(self, saga: LoanProcessingSaga) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE saga_instances
                SET step = $1,
                    compliance_record_id = $2,
                    last_causation_id = $3,
                    retry_count = $4,
                    updated_at = NOW()
                WHERE saga_id = $5
                """,
                saga.step.value,
                saga.compliance_record_id,
                saga.last_causation_id,
                saga.retry_count,
                saga.saga_id,
            )

    async def _load_checkpoint(self) -> None:
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT last_position FROM saga_checkpoints WHERE saga_name = $1",
                self.CHECKPOINT_KEY,
            )
            self._last_position = row["last_position"] if row else 0

    async def _advance_checkpoint(self, position: int) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO saga_checkpoints (saga_name, last_position)
                VALUES ($1, $2)
                ON CONFLICT (saga_name) DO UPDATE
                    SET last_position = $2, updated_at = NOW()
                """,
                self.CHECKPOINT_KEY, position,
            )
        self._last_position = position
