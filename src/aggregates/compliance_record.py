"""
ComplianceRecordAggregate: Tracks compliance checks for a loan application.

Communicates with LoanApplication only via events — never direct calls.
"""
from __future__ import annotations

from uuid import UUID

from src.aggregates.base import AggregateRoot
from src.models.events import (
    ComplianceCheckFailed,
    ComplianceCheckPassed,
    ComplianceRecordCreated,
    ComplianceRecordFinalized,
    ComplianceStatus,
    InvalidStateTransitionError,
)


class ComplianceRecordAggregate(AggregateRoot):
    aggregate_type = "ComplianceRecord"

    def __init__(self, aggregate_id: UUID):
        super().__init__(aggregate_id)
        self.status: ComplianceStatus | None = None
        self.application_id: UUID | None = None
        self.officer_id: str = ""
        self.required_checks: list[str] = []
        self.passed_checks: list[str] = []
        self.failed_checks: list[str] = []

    @property
    def all_checks_complete(self) -> bool:
        completed = set(self.passed_checks) | set(self.failed_checks)
        return set(self.required_checks).issubset(completed)

    @property
    def overall_passed(self) -> bool:
        return len(self.failed_checks) == 0 and self.all_checks_complete

    # -------------------------------------------------------------------------
    # Commands
    # -------------------------------------------------------------------------

    def create(self, application_id: UUID, officer_id: str, required_checks: list[str]) -> None:
        if self.status is not None:
            raise InvalidStateTransitionError(
                self.aggregate_type, str(self.status), "Pending", "Already created"
            )
        self._raise_event(ComplianceRecordCreated(
            aggregate_id=self.aggregate_id,
            application_id=application_id,
            officer_id=officer_id,
            required_checks=required_checks,
        ))

    def pass_check(self, check_name: str, check_result: dict, officer_id: str) -> None:
        self._assert_in_progress()
        if check_name not in self.required_checks:
            raise ValueError(f"Check '{check_name}' is not in required checks")
        self._raise_event(ComplianceCheckPassed(
            aggregate_id=self.aggregate_id,
            check_name=check_name,
            check_result=check_result,
            officer_id=officer_id,
        ))

    def fail_check(self, check_name: str, failure_reason: str, officer_id: str) -> None:
        self._assert_in_progress()
        self._raise_event(ComplianceCheckFailed(
            aggregate_id=self.aggregate_id,
            check_name=check_name,
            failure_reason=failure_reason,
            officer_id=officer_id,
        ))

    def finalize(self, officer_id: str, notes: str = "") -> None:
        self._assert_in_progress()
        overall = ComplianceStatus.PASSED if self.overall_passed else ComplianceStatus.FAILED
        self._raise_event(ComplianceRecordFinalized(
            aggregate_id=self.aggregate_id,
            overall_status=overall,
            officer_id=officer_id,
            notes=notes,
        ))

    # -------------------------------------------------------------------------
    # Event Handlers
    # -------------------------------------------------------------------------

    def when_ComplianceRecordCreated(self, event: ComplianceRecordCreated) -> None:
        self.status = ComplianceStatus.IN_PROGRESS
        self.application_id = event.application_id
        self.officer_id = event.officer_id
        self.required_checks = list(event.required_checks)

    def when_ComplianceCheckPassed(self, event: ComplianceCheckPassed) -> None:
        if event.check_name not in self.passed_checks:
            self.passed_checks.append(event.check_name)

    def when_ComplianceCheckFailed(self, event: ComplianceCheckFailed) -> None:
        if event.check_name not in self.failed_checks:
            self.failed_checks.append(event.check_name)

    def when_ComplianceRecordFinalized(self, event: ComplianceRecordFinalized) -> None:
        self.status = event.overall_status

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _assert_in_progress(self) -> None:
        if self.status != ComplianceStatus.IN_PROGRESS:
            raise InvalidStateTransitionError(
                self.aggregate_type, str(self.status), "InProgress",
                "Compliance record is not in progress"
            )
