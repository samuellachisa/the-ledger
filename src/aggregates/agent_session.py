"""
AgentSessionAggregate: Tracks AI agent execution context and decisions.

Gas Town Pattern:
    AgentContextLoaded events serve as checkpoints. On crash recovery,
    the session is replayed to find the last checkpoint and resume from there.

Business Rule:
    AgentContextLoaded MUST be present before any decision event.
"""
from __future__ import annotations

from uuid import UUID

from src.aggregates.base import AggregateRoot
from src.models.events import (
    AgentContextLoaded,
    AgentCreditAnalysisObserved,
    AgentDecisionRecorded,
    AgentSessionCompleted,
    AgentSessionFailed,
    AgentSessionResumed,
    AgentSessionStarted,
    AgentSessionStatus,
    BusinessRuleViolationError,
    DecisionOutcome,
    InvalidStateTransitionError,
)


class AgentSessionAggregate(AggregateRoot):
    aggregate_type = "AgentSession"

    def __init__(self, aggregate_id: UUID):
        super().__init__(aggregate_id)
        self.status: AgentSessionStatus | None = None
        self.application_id: UUID | None = None
        self.agent_model: str = ""
        self.agent_version: str = ""
        self.context_loaded: bool = False
        self.context_sources: list[str] = []
        self.context_snapshot: dict = {}
        self.loaded_stream_positions: dict[str, int] = {}  # Gas Town checkpoints
        self.context_model_version: str | None = None  # model version recorded at context load
        self.decision: DecisionOutcome | None = None
        self.confidence_score: float | None = None
        self.crash_event_id: UUID | None = None

    # -------------------------------------------------------------------------
    # Commands
    # -------------------------------------------------------------------------

    def start(
        self,
        application_id: UUID,
        agent_model: str,
        agent_version: str,
        session_config: dict | None = None,
    ) -> None:
        if self.status is not None:
            raise InvalidStateTransitionError(
                self.aggregate_type, str(self.status), "Initializing",
                "Session already started"
            )
        self._raise_event(AgentSessionStarted(
            aggregate_id=self.aggregate_id,
            application_id=application_id,
            agent_model=agent_model,
            agent_version=agent_version,
            session_config=session_config or {},
        ))

    def load_context(
        self,
        application_id: UUID,
        context_sources: list[str],
        context_snapshot: dict,
        loaded_stream_positions: dict[str, int],
        model_version: str | None = None,
    ) -> None:
        if self.status not in (AgentSessionStatus.INITIALIZING, AgentSessionStatus.PROCESSING):
            raise InvalidStateTransitionError(
                self.aggregate_type, str(self.status), "ContextLoaded"
            )
        self._raise_event(AgentContextLoaded(
            aggregate_id=self.aggregate_id,
            application_id=application_id,
            context_sources=context_sources,
            context_snapshot=context_snapshot,
            loaded_stream_positions=loaded_stream_positions,
            model_version=model_version,
        ))

    def record_decision(
        self,
        application_id: UUID,
        outcome: DecisionOutcome,
        confidence_score: float,
        reasoning: str,
        processing_duration_ms: int,
        model_version: str | None = None,
    ) -> None:
        # Gas Town: context must be loaded before any decision
        if not self.context_loaded:
            raise BusinessRuleViolationError(
                "GasTown",
                "AgentContextLoaded must be recorded before any decision event. "
                "Agent may have crashed before loading context."
            )
        if self.status not in (AgentSessionStatus.CONTEXT_LOADED, AgentSessionStatus.PROCESSING):
            raise InvalidStateTransitionError(
                self.aggregate_type, str(self.status), "Processing"
            )
        # Model version guard: decision model must match the context model
        if self.context_model_version is not None and model_version is not None:
            if model_version != self.context_model_version:
                raise BusinessRuleViolationError(
                    "ModelVersionMismatch",
                    f"Decision model '{model_version}' does not match context model "
                    f"'{self.context_model_version}'. Reload context with the correct model version.",
                )
        self._raise_event(AgentDecisionRecorded(
            aggregate_id=self.aggregate_id,
            application_id=application_id,
            outcome=outcome,
            confidence_score=confidence_score,
            reasoning=reasoning,
            processing_duration_ms=processing_duration_ms,
        ))

    def complete(self, application_id: UUID, total_duration_ms: int, events_processed: int) -> None:
        self._raise_event(AgentSessionCompleted(
            aggregate_id=self.aggregate_id,
            application_id=application_id,
            total_duration_ms=total_duration_ms,
            events_processed=events_processed,
        ))

    def fail(
        self,
        application_id: UUID,
        error_type: str,
        error_message: str,
    ) -> None:
        self._raise_event(AgentSessionFailed(
            aggregate_id=self.aggregate_id,
            application_id=application_id,
            error_type=error_type,
            error_message=error_message,
            last_checkpoint=self.loaded_stream_positions or None,
        ))

    def observe_credit_analysis(
        self,
        application_id: UUID,
        credit_score: int,
        debt_to_income_ratio: float,
        loan_application_stream_version: int,
        model_version: str | None = None,
    ) -> None:
        """
        Guard + event for when the agent session observes a credit analysis result.

        The session must be active (Initializing, ContextLoaded, or Processing) —
        it makes no sense to update a completed or crashed session's checkpoint.
        Raises InvalidStateTransitionError if the session is in a terminal state.

        Updates the Gas Town checkpoint for the LoanApplication stream so that
        on crash recovery the agent knows it already processed this credit result.
        """
        _active = (
            AgentSessionStatus.INITIALIZING,
            AgentSessionStatus.CONTEXT_LOADED,
            AgentSessionStatus.PROCESSING,
        )
        if self.status not in _active:
            raise InvalidStateTransitionError(
                self.aggregate_type,
                str(self.status),
                "Processing",
                "Cannot observe credit analysis on a session that is not active.",
            )
        self._raise_event(AgentCreditAnalysisObserved(
            aggregate_id=self.aggregate_id,
            application_id=application_id,
            credit_score=credit_score,
            debt_to_income_ratio=debt_to_income_ratio,
            model_version=model_version,
            loan_application_stream_version=loan_application_stream_version,
        ))

    def resume(self, application_id: UUID, crash_event_id: UUID | None = None) -> None:
        """Resume after crash. Replays from last checkpoint (Gas Town)."""
        if self.status != AgentSessionStatus.CRASHED:
            raise InvalidStateTransitionError(
                self.aggregate_type, str(self.status), "Processing",
                "Can only resume a crashed session"
            )
        self._raise_event(AgentSessionResumed(
            aggregate_id=self.aggregate_id,
            application_id=application_id,
            resumed_from_checkpoint=self.loaded_stream_positions,
            crash_event_id=crash_event_id,
        ))

    # -------------------------------------------------------------------------
    # Event Handlers
    # -------------------------------------------------------------------------

    def when_AgentSessionStarted(self, event: AgentSessionStarted) -> None:
        self.status = AgentSessionStatus.INITIALIZING
        self.application_id = event.application_id
        self.agent_model = event.agent_model
        self.agent_version = event.agent_version

    def when_AgentContextLoaded(self, event: AgentContextLoaded) -> None:
        self.status = AgentSessionStatus.CONTEXT_LOADED
        self.context_loaded = True
        self.context_sources = event.context_sources
        self.context_snapshot = event.context_snapshot
        self.loaded_stream_positions = event.loaded_stream_positions
        self.context_model_version = event.model_version

    def when_AgentCreditAnalysisObserved(self, event: AgentCreditAnalysisObserved) -> None:
        # Update Gas Town checkpoint: record how far we've read the LoanApplication stream
        self.loaded_stream_positions[str(event.application_id)] = event.loan_application_stream_version

    def when_AgentDecisionRecorded(self, event: AgentDecisionRecorded) -> None:
        self.status = AgentSessionStatus.PROCESSING
        self.decision = event.outcome
        self.confidence_score = event.confidence_score

    def when_AgentSessionCompleted(self, event: AgentSessionCompleted) -> None:
        self.status = AgentSessionStatus.COMPLETED

    def when_AgentSessionFailed(self, event: AgentSessionFailed) -> None:
        self.status = AgentSessionStatus.CRASHED
        if event.last_checkpoint:
            self.loaded_stream_positions = event.last_checkpoint

    def when_AgentSessionResumed(self, event: AgentSessionResumed) -> None:
        self.status = AgentSessionStatus.PROCESSING
        self.crash_event_id = event.crash_event_id

    # -------------------------------------------------------------------------
    # Reconstruction
    # -------------------------------------------------------------------------

    @classmethod
    def load(cls, aggregate_id: UUID, events: list) -> "AgentSessionAggregate":
        """Reconstruct session state by replaying *events* in order.

        Accepts the list returned by ``EventStore.load_stream()``.  After
        replay ``instance.version`` equals the number of events applied and
        ``instance.original_version`` is set to that same value so it can be
        passed directly as ``expected_version`` to ``EventStore.append()``.
        Gas Town checkpoints (``loaded_stream_positions``) are restored as
        part of normal event replay via ``when_AgentContextLoaded`` and
        ``when_AgentCreditAnalysisObserved``.
        """
        instance = cls(aggregate_id)
        for event in events:
            instance.apply(event)
        instance.original_version = instance.version
        instance.pending_events = []
        return instance
