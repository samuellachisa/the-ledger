"""
AggregateTestHarness: Given/When/Then DSL for aggregate invariant testing.

Problem
-------
Aggregate tests are written as ad-hoc sequences of method calls with manual
assertions. This makes it hard to see the intent, easy to miss edge cases,
and verbose to write.

Design
------
The harness follows the standard event-sourcing test pattern:

    given([past_events])   — replay history to set up aggregate state
    when(command_fn)       — execute a command (may raise)
    then_events([...])     — assert pending events match expected
    then_raises(ExcType)   — assert command raised a specific exception
    then_state(fn)         — assert aggregate state after command

Usage
-----
    harness = AggregateTestHarness(LoanApplicationAggregate)

    # Happy path
    (harness
        .given([LoanApplicationSubmitted(...)])
        .when(lambda agg: agg.request_credit_analysis("system"))
        .then_events([CreditAnalysisRequested])
    )

    # Error path
    (harness
        .given([LoanApplicationSubmitted(...), CreditAnalysisCompleted(...)])
        .when(lambda agg: agg.request_credit_analysis("system"))
        .then_raises(InvalidStateTransitionError)
    )

    # State assertion
    (harness
        .given([LoanApplicationSubmitted(...)])
        .when(lambda agg: agg.request_credit_analysis("system"))
        .then_state(lambda agg: agg.status == LoanStatus.AWAITING_ANALYSIS)
    )
"""
from __future__ import annotations

import traceback
from typing import Any, Callable, Type
from uuid import uuid4

from src.aggregates.base import AggregateRoot
from src.models.events import DomainEvent


class _CommandResult:
    """Holds the outcome of a when() call."""

    def __init__(
        self,
        aggregate: AggregateRoot,
        pending_events: list[DomainEvent],
        exception: Exception | None,
    ):
        self._aggregate = aggregate
        self._pending = pending_events
        self._exception = exception

    def then_events(self, expected_types: list[type]) -> "_CommandResult":
        """
        Assert that the command raised exactly the expected event types, in order.

        Args:
            expected_types: List of DomainEvent subclasses (not instances).
        """
        if self._exception is not None:
            raise AssertionError(
                f"Expected events {[t.__name__ for t in expected_types]} "
                f"but command raised {type(self._exception).__name__}: {self._exception}"
            )

        actual_types = [type(e) for e in self._pending]
        if actual_types != expected_types:
            raise AssertionError(
                f"Event mismatch.\n"
                f"  Expected: {[t.__name__ for t in expected_types]}\n"
                f"  Actual:   {[t.__name__ for t in actual_types]}"
            )
        return self

    def then_no_events(self) -> "_CommandResult":
        """Assert that the command raised no events."""
        return self.then_events([])

    def then_raises(self, exc_type: type, message_contains: str | None = None) -> "_CommandResult":
        """
        Assert that the command raised a specific exception type.

        Args:
            exc_type: Expected exception class.
            message_contains: Optional substring that must appear in str(exception).
        """
        if self._exception is None:
            raise AssertionError(
                f"Expected {exc_type.__name__} to be raised, "
                f"but command succeeded with events: "
                f"{[type(e).__name__ for e in self._pending]}"
            )
        if not isinstance(self._exception, exc_type):
            raise AssertionError(
                f"Expected {exc_type.__name__} but got "
                f"{type(self._exception).__name__}: {self._exception}"
            )
        if message_contains and message_contains not in str(self._exception):
            raise AssertionError(
                f"Exception message '{self._exception}' does not contain '{message_contains}'"
            )
        return self

    def then_state(self, assertion: Callable[[AggregateRoot], bool], description: str = "") -> "_CommandResult":
        """
        Assert aggregate state after the command.

        Args:
            assertion: Callable that receives the aggregate and returns bool.
            description: Human-readable description for failure messages.
        """
        if self._exception is not None:
            raise AssertionError(
                f"Cannot check state — command raised {type(self._exception).__name__}: {self._exception}"
            )
        if not assertion(self._aggregate):
            raise AssertionError(
                f"State assertion failed{': ' + description if description else ''}. "
                f"Aggregate: {self._aggregate.__class__.__name__}(id={self._aggregate.aggregate_id})"
            )
        return self

    def then_event_payload(self, index: int, **expected_fields) -> "_CommandResult":
        """
        Assert specific fields on the event at position `index`.

        Example:
            .then_event_payload(0, credit_score=720, passed=True)
        """
        if self._exception is not None:
            raise AssertionError(f"Command raised {type(self._exception).__name__}: {self._exception}")
        if index >= len(self._pending):
            raise AssertionError(
                f"No event at index {index}. Only {len(self._pending)} events raised."
            )
        event = self._pending[index]
        payload = event.to_payload()
        for field, expected in expected_fields.items():
            actual = payload.get(field)
            if actual != expected:
                raise AssertionError(
                    f"Event[{index}].{field}: expected {expected!r}, got {actual!r}"
                )
        return self

    @property
    def exception(self) -> Exception | None:
        return self._exception

    @property
    def events(self) -> list[DomainEvent]:
        return self._pending

    @property
    def aggregate(self) -> AggregateRoot:
        return self._aggregate


class AggregateTestHarness:
    """
    Given/When/Then test harness for aggregates.

    Usage:
        harness = AggregateTestHarness(LoanApplicationAggregate)
        result = (
            harness
            .given([submitted_event])
            .when(lambda agg: agg.request_credit_analysis("system"))
        )
        result.then_events([CreditAnalysisRequested])
    """

    def __init__(self, aggregate_class: type, aggregate_id=None):
        self._aggregate_class = aggregate_class
        self._aggregate_id = aggregate_id or uuid4()
        self._history: list[DomainEvent] = []

    def given(self, events: list[DomainEvent]) -> "AggregateTestHarness":
        """Set up aggregate history by replaying past events."""
        self._history = list(events)
        return self

    def when(self, command_fn: Callable[[AggregateRoot], Any]) -> _CommandResult:
        """
        Execute a command against the aggregate.

        command_fn receives the aggregate instance and should call a command method.
        It may raise — the harness captures the exception for then_raises().
        """
        aggregate = self._aggregate_class.load(self._aggregate_id, self._history)
        exception = None
        try:
            command_fn(aggregate)
        except Exception as e:
            exception = e

        return _CommandResult(
            aggregate=aggregate,
            pending_events=list(aggregate.pending_events),
            exception=exception,
        )
