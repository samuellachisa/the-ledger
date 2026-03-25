"""
ledger/projections/compliance_audit.py
ComplianceAuditView Projection
"""
from typing import Dict, Any, List
import copy
from datetime import datetime
from ledger.event_store import StoredEvent

class ComplianceAuditView:
    def __init__(self, event_store=None):
        self.event_store = event_store
        self.records: Dict[str, List[Dict[str, Any]]] = {}
        self.last_processed_position = 0

    async def handle_event(self, event: StoredEvent):
        self.last_processed_position = event.global_position
        
        # We only care about compliance events
        if not event.event_type.startswith("Compliance"):
            return
            
        p = event.payload
        app_id = p.get("application_id")
        if not app_id:
            # Fallback for streams
            if event.stream_id.startswith("compliance-"):
                app_id = event.stream_id.replace("compliance-", "")
        
        if not app_id: return
        app_id = str(app_id)
        
        if app_id not in self.records:
            self.records[app_id] = []
            
        current_state = {}
        if self.records[app_id]:
            # Inherit previous state
            # Deep-copy so later appends to lists (e.g. passed_checks)
            # don't mutate earlier historical snapshots used by time-travel queries.
            current_state = copy.deepcopy(self.records[app_id][-1]["state"])
            
        t = event.event_type
        if t == "ComplianceRecordCreated":
            current_state["status"] = "Created"
            current_state["required_checks"] = p.get("required_checks", [])
            current_state["passed_checks"] = []
            current_state["failed_checks"] = []
        elif t == "ComplianceCheckPassed":
            if "passed_checks" not in current_state: current_state["passed_checks"] = []
            current_state["passed_checks"].append(p.get("check_name"))
        elif t == "ComplianceCheckFailed":
            if "failed_checks" not in current_state: current_state["failed_checks"] = []
            current_state["failed_checks"].append({
                "check": p.get("check_name"),
                "reason": p.get("failure_reason")
            })
        elif t == "ComplianceRecordFinalized" or t == "ComplianceCheckCompleted":
            current_state["status"] = "Finalized"
            current_state["verdict"] = p.get("overall_status") or p.get("overall_verdict")
            
        self.records[app_id].append({
            "timestamp": event.recorded_at,
            "event_type": t,
            "state": current_state
        })

    def get_current_compliance(self, application_id: str) -> Dict[str, Any] | None:
        rec = self.records.get(application_id)
        if not rec: return None
        return rec[-1]["state"]

    def get_state_at(self, application_id: str, timestamp: datetime) -> Dict[str, Any] | None:
        """Temporal Query: get state at a specific point in time (Time-travel)."""
        rec = self.records.get(application_id)
        if not rec: return None
        
        # Binary search or linear search backwards
        for entry in reversed(rec):
            if entry["timestamp"] <= timestamp:
                return entry["state"]
        return None

    def get_projection_lag(self, daemon_db_max_pos: int) -> int:
        return max(0, daemon_db_max_pos - self.last_processed_position)

    async def rebuild_from_scratch(self):
        """Wipes the data and processes the entire EventStore stream log iteratively"""
        if not self.event_store:
            raise ValueError("EventStore must be provided to rebuild from scratch.")
        self.records.clear()
        self.last_processed_position = 0
        evts = [e async for e in self.event_store.load_all(batch_size=500)]
        for e in evts:
            await self.handle_event(e)
