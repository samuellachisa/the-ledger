"""
ledger/projections/application_summary.py
ApplicationSummary Projection
"""
from typing import Dict, Any
from ledger.event_store import StoredEvent

class ApplicationSummaryProjection:
    def __init__(self):
        self.summaries: Dict[str, Dict[str, Any]] = {}
        
    def _extract_app_id(self, event: StoredEvent) -> str | None:
        p = event.payload
        if p.get("application_id"): return str(p["application_id"])
        if event.stream_id.startswith("loan-"): return event.stream_id.replace("loan-", "")
        if event.stream_id.startswith("docpkg-"): return event.stream_id.replace("docpkg-", "")
        if event.stream_id.startswith("compliance-"): return event.stream_id.replace("compliance-", "")
        if event.stream_id.startswith("fraud-"): return event.stream_id.replace("fraud-", "")
        return None

    async def handle_event(self, event: StoredEvent):
        app_id = self._extract_app_id(event)
        if not app_id: return
            
        if app_id not in self.summaries:
            self.summaries[app_id] = {
                "application_id": app_id,
                "state": "Unknown",
                "applicant_id": event.payload.get("applicant_id"),
                "requested_amount_usd": event.payload.get("loan_amount"),
                "approved_amount_usd": None,
                "risk_tier": "Unknown",
                "fraud_score": None,
                "compliance_status": "Unknown",
                "decision": None,
                "agent_sessions_completed": [],
                "last_event_type": event.event_type,
                "last_event_at": event.recorded_at
            }
            
        view = self.summaries[app_id]
        t = event.event_type
        p = event.payload
        
        if t == "LoanApplicationSubmitted":
            view["state"] = "Submitted"
            view["requested_amount_usd"] = p.get("loan_amount")
            view["applicant_id"] = p.get("applicant_id")
        elif t == "FraudCheckCompleted":
            view["fraud_score"] = p.get("fraud_risk_score")
        elif t == "ComplianceCheckCompleted":
            view["compliance_status"] = p.get("overall_verdict")
        elif t == "DecisionGenerated":
            view["decision"] = p.get("outcome")
            view["state"] = "Decision Generated"
        elif t == "ApplicationApproved":
            view["state"] = "Approved"
            view["approved_amount_usd"] = p.get("approved_amount")
        elif t == "ApplicationDenied":
            view["state"] = "Denied"
        elif t == "ApplicationReferred":
            view["state"] = "Referred"
        elif t == "AgentSessionCompleted":
            view["agent_sessions_completed"].append(str(event.event_id))
            
        view["last_event_type"] = t
        view["last_event_at"] = event.recorded_at

    def get_summary(self, application_id: str) -> Dict[str, Any] | None:
        return self.summaries.get(application_id)
