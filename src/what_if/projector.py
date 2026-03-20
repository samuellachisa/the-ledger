"""
What-If Projector: Counterfactual analysis for loan decisions.

Injects hypothetical events into a copy of the event stream and filters
causally-dependent downstream events that are no longer valid given the
counterfactual premise.

Causal dependency model:
    CreditAnalysisCompleted → DecisionGenerated (credit score influences decision)
    FraudCheckCompleted → DecisionGenerated
    ComplianceRecordFinalized → DecisionGenerated (compliance result influences decision)
    DecisionGenerated → ApplicationApproved | ApplicationDenied | ApplicationReferred

When a counterfactual replaces CreditAnalysisCompleted, all downstream events
that causally depend on the credit score (DecisionGenerated, ApplicationApproved, etc.)
are stripped from the counterfactual stream. The aggregate is then re-evaluated
to determine what the new outcome would be.

Use case: "What would have happened if the credit score was 750 instead of 620?"
"""
from __future__ import annotations

import logging
from uuid import UUID

from src.aggregates.loan_application import LoanApplicationAggregate
from src.event_store import EventStore
from src.models.events import DomainEvent

logger = logging.getLogger(__name__)

# Causal dependency graph: replacing event_type X invalidates these downstream types
CAUSAL_DEPENDENCIES: dict[str, set[str]] = {
    "CreditAnalysisCompleted": {
        "DecisionGenerated",
        "ApplicationApproved",
        "ApplicationDenied",
        "ApplicationReferred",
    },
    "FraudCheckCompleted": {
        "DecisionGenerated",
        "ApplicationApproved",
        "ApplicationDenied",
        "ApplicationReferred",
    },
    "ComplianceRecordFinalized": {
        "DecisionGenerated",
        "ApplicationApproved",
        "ApplicationDenied",
        "ApplicationReferred",
    },
    "DecisionGenerated": {
        "ApplicationApproved",
        "ApplicationDenied",
        "ApplicationReferred",
    },
    "LoanApplicationSubmitted": {
        # Replacing the submission invalidates everything
        "CreditAnalysisRequested",
        "CreditAnalysisCompleted",
        "FraudCheckCompleted",
        "ComplianceCheckRequested",
        "DecisionGenerated",
        "ApplicationApproved",
        "ApplicationDenied",
        "ApplicationReferred",
    },
}


def generate_narrative(result: dict) -> str:
    """
    Produce a human-readable explanation of a counterfactual what-if result.

    Describes: what was changed, which downstream events were causally invalidated,
    how the outcome differs (or doesn't), and what the new state reflects.
    """
    injected = result.get("injected_events", [])
    removed = result.get("causally_removed_events", [])
    baseline = result.get("baseline_outcome", "unknown")
    counterfactual = result.get("counterfactual_outcome", "unknown")
    outcome_changed = result.get("outcome_changed", False)
    b_state = result.get("baseline_state", {})
    cf_state = result.get("counterfactual_state", {})

    parts: list[str] = []

    # What was changed
    if injected:
        parts.append(
            f"Counterfactual premise: replaced {', '.join(injected)}."
        )

    # Highlight key state differences
    diffs: list[str] = []
    if b_state.get("credit_score") != cf_state.get("credit_score"):
        diffs.append(
            f"credit score changed from {b_state.get('credit_score')} "
            f"to {cf_state.get('credit_score')}"
        )
    if b_state.get("compliance_passed") != cf_state.get("compliance_passed"):
        diffs.append(
            f"compliance result changed from {b_state.get('compliance_passed')} "
            f"to {cf_state.get('compliance_passed')}"
        )
    if diffs:
        parts.append("Key input changes: " + "; ".join(diffs) + ".")

    # Causal invalidation
    if removed:
        parts.append(
            f"Causally invalidated and removed from the counterfactual stream: "
            f"{', '.join(removed)}. "
            f"These events depended on the replaced inputs and are no longer valid."
        )

    # Outcome comparison
    if outcome_changed:
        parts.append(
            f"Outcome changed: the application would have reached '{counterfactual}' "
            f"instead of '{baseline}'."
        )
    else:
        parts.append(
            f"Outcome unchanged: the application still reaches '{baseline}' "
            f"even with the counterfactual inputs."
        )

    return " ".join(parts)


class WhatIfProjector:
    def __init__(self, event_store: EventStore):
        self._store = event_store

    async def run_what_if(
        self,
        application_id: UUID,
        counterfactual_events: list[DomainEvent],
        inject_after_event_type: str | None = None,
    ) -> dict:
        """
        Run a counterfactual projection with causal dependency filtering.

        Args:
            application_id: The application to analyze
            counterfactual_events: Hypothetical replacement events
            inject_after_event_type: Inject after this event type (None = replace in-place)

        Returns:
            {
                "baseline_outcome": str,
                "counterfactual_outcome": str,
                "outcome_changed": bool,
                "baseline_state": dict,
                "counterfactual_state": dict,
                "injected_events": list[str],
                "causally_removed_events": list[str],  # events stripped due to causal deps
                "real_event_count": int,
                "counterfactual_event_count": int,
                "causal_chain": list[str],  # dependency chain that was applied
            }
        """
        real_events = await self._store.load_stream("LoanApplication", application_id)

        # Build baseline state
        baseline = LoanApplicationAggregate.load(application_id, real_events)

        # Build counterfactual stream with causal filtering
        cf_events, removed, causal_chain = self._build_counterfactual_stream(
            real_events, counterfactual_events, inject_after_event_type
        )

        # Apply counterfactual stream
        cf_aggregate = LoanApplicationAggregate(application_id)
        for event in cf_events:
            cf_aggregate.apply(event)

        baseline_state = {
            "status": str(baseline.status),
            "credit_score": baseline.credit_score,
            "compliance_passed": baseline.compliance_passed,
            "last_decision": str(baseline.last_decision) if baseline.last_decision else None,
            "last_confidence": baseline.last_confidence,
            "loan_amount": baseline.loan_amount,
        }

        cf_state = {
            "status": str(cf_aggregate.status),
            "credit_score": cf_aggregate.credit_score,
            "compliance_passed": cf_aggregate.compliance_passed,
            "last_decision": str(cf_aggregate.last_decision) if cf_aggregate.last_decision else None,
            "last_confidence": cf_aggregate.last_confidence,
            "loan_amount": cf_aggregate.loan_amount,
        }

        result = {
            "application_id": str(application_id),
            "baseline_outcome": baseline_state["status"],
            "counterfactual_outcome": cf_state["status"],
            "outcome_changed": baseline_state["status"] != cf_state["status"],
            "baseline_state": baseline_state,
            "counterfactual_state": cf_state,
            "injected_events": [e.event_type for e in counterfactual_events],
            "causally_removed_events": removed,
            "real_event_count": len(real_events),
            "counterfactual_event_count": len(cf_events),
            "causal_chain": causal_chain,
        }
        result["narrative"] = generate_narrative(result)
        return result

    def _build_counterfactual_stream(
        self,
        real_events: list[DomainEvent],
        counterfactual_events: list[DomainEvent],
        inject_after: str | None,
    ) -> tuple[list[DomainEvent], list[str], list[str]]:
        """
        Build counterfactual event stream with causal dependency filtering.

        Returns:
            (cf_stream, removed_event_types, causal_chain_description)

        Algorithm:
        1. Identify which event types are being replaced by counterfactuals
        2. Compute the full set of causally-invalidated downstream event types
        3. Build the stream: keep real events that are not replaced and not invalidated,
           inject counterfactuals at the right position
        """
        cf_types = {e.event_type for e in counterfactual_events}

        # Compute all causally invalidated event types
        invalidated: set[str] = set()
        causal_chain: list[str] = []
        for cf_type in cf_types:
            deps = CAUSAL_DEPENDENCIES.get(cf_type, set())
            if deps:
                causal_chain.append(f"{cf_type} → {', '.join(sorted(deps))}")
            invalidated.update(deps)

        result: list[DomainEvent] = []
        removed: list[str] = []
        injected = False

        for event in real_events:
            if event.event_type in cf_types:
                # Replace with counterfactual version
                cf = next((e for e in counterfactual_events if e.event_type == event.event_type), None)
                result.append(cf if cf else event)
                if not injected and inject_after is None:
                    injected = True
            elif event.event_type in invalidated:
                # Strip causally-dependent downstream event
                removed.append(event.event_type)
                logger.debug(
                    "Counterfactual: removing causally-dependent event %s", event.event_type
                )
            else:
                result.append(event)

            # Inject after a specific event type
            if inject_after and event.event_type == inject_after and not injected:
                result.extend(counterfactual_events)
                injected = True

        if inject_after and not injected:
            result.extend(counterfactual_events)

        return result, removed, causal_chain
