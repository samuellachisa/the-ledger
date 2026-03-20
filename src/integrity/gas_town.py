"""
Gas Town: Agent context reconstruction after crash.

Pattern:
1. Load AgentSession stream to find last AgentContextLoaded checkpoint
2. Identify which context sources were already loaded
3. Resume from checkpoint — skip already-loaded sources
4. Verify context integrity before allowing decisions

The name "Gas Town" refers to a checkpoint/refueling station:
the agent stops here to reload its context before continuing the journey.
"""
from __future__ import annotations

import logging
from uuid import UUID

from src.aggregates.agent_session import AgentSessionAggregate
from src.event_store import EventStore
from src.models.events import AgentSessionStatus

logger = logging.getLogger(__name__)


class AgentContextReconstructor:
    """
    Reconstructs agent context after a crash by replaying session events
    and identifying the last valid checkpoint.
    """

    def __init__(self, event_store: EventStore):
        self._store = event_store

    async def reconstruct_agent_context(self, session_id: UUID) -> dict:
        """
        Reconstruct agent context from session event stream.

        Returns:
            {
                "session_id": UUID,
                "status": AgentSessionStatus,
                "context_loaded": bool,
                "context_sources": list[str],
                "context_snapshot": dict,
                "loaded_stream_positions": dict[str, int],  # Gas Town checkpoints
                "can_make_decision": bool,
                "resume_required": bool,
                "crash_detected": bool,
            }
        """
        events = await self._store.load_stream(
            AgentSessionAggregate.aggregate_type, session_id
        )

        if not events:
            return {
                "session_id": session_id,
                "status": None,
                "context_loaded": False,
                "context_sources": [],
                "context_snapshot": {},
                "loaded_stream_positions": {},
                "can_make_decision": False,
                "resume_required": False,
                "crash_detected": False,
                "error": "Session not found",
            }

        session = AgentSessionAggregate.load(session_id, events)

        crash_detected = session.status == AgentSessionStatus.CRASHED
        resume_required = crash_detected and session.context_loaded

        logger.info(
            "Reconstructed session %s: status=%s, context_loaded=%s, "
            "checkpoints=%s, crash=%s",
            session_id,
            session.status,
            session.context_loaded,
            session.loaded_stream_positions,
            crash_detected,
        )

        return {
            "session_id": session_id,
            "status": session.status,
            "context_loaded": session.context_loaded,
            "context_sources": session.context_sources,
            "context_snapshot": session.context_snapshot,
            "loaded_stream_positions": session.loaded_stream_positions,
            "can_make_decision": session.context_loaded and not crash_detected,
            "resume_required": resume_required,
            "crash_detected": crash_detected,
            "application_id": session.application_id,
        }

    async def get_missing_context_sources(
        self,
        session_id: UUID,
        required_sources: list[str],
    ) -> list[str]:
        """
        Determine which context sources still need to be loaded.
        Used during crash recovery to avoid re-fetching already-loaded context.
        """
        context = await self.reconstruct_agent_context(session_id)
        already_loaded = set(context.get("context_sources", []))
        return [s for s in required_sources if s not in already_loaded]

    async def can_resume_session(self, session_id: UUID) -> bool:
        """Check if a crashed session can be resumed."""
        context = await self.reconstruct_agent_context(session_id)
        return context.get("crash_detected", False) and context.get("context_loaded", False)

    async def simulate_crash_and_recover(
        self,
        session_id: UUID,
        event_store: EventStore,
        application_id: UUID,
    ) -> dict:
        """
        Test utility: simulate a crash mid-processing and verify recovery.
        Used in test_gas_town.py.

        Returns recovery result with before/after state comparison.
        """
        from src.aggregates.agent_session import AgentSessionAggregate
        from src.commands.handlers import CommandHandler

        # Capture state before crash
        before = await self.reconstruct_agent_context(session_id)

        # Simulate crash by recording a failure event
        handler = CommandHandler(event_store)
        events = await event_store.load_stream(
            AgentSessionAggregate.aggregate_type, session_id
        )
        session = AgentSessionAggregate.load(session_id, events)

        if session.status not in (
            AgentSessionStatus.INITIALIZING,
            AgentSessionStatus.CONTEXT_LOADED,
            AgentSessionStatus.PROCESSING,
        ):
            return {"error": "Session not in a crashable state", "before": before}

        session.fail(
            application_id=application_id,
            error_type="SimulatedCrash",
            error_message="Crash injected for testing Gas Town recovery",
        )
        await event_store.append(
            aggregate_type=AgentSessionAggregate.aggregate_type,
            aggregate_id=session_id,
            events=session.pending_events,
            expected_version=session.version - len(session.pending_events),
        )

        # Reconstruct after crash
        after_crash = await self.reconstruct_agent_context(session_id)

        # Resume session
        events = await event_store.load_stream(
            AgentSessionAggregate.aggregate_type, session_id
        )
        session = AgentSessionAggregate.load(session_id, events)
        session.resume(application_id=application_id)
        await event_store.append(
            aggregate_type=AgentSessionAggregate.aggregate_type,
            aggregate_id=session_id,
            events=session.pending_events,
            expected_version=session.version - len(session.pending_events),
        )

        after_recovery = await self.reconstruct_agent_context(session_id)

        return {
            "before_crash": before,
            "after_crash": after_crash,
            "after_recovery": after_recovery,
            "checkpoints_preserved": (
                before.get("loaded_stream_positions") ==
                after_recovery.get("loaded_stream_positions")
            ),
            "context_preserved": (
                before.get("context_sources") ==
                after_recovery.get("context_sources")
            ),
        }
