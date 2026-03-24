"""
ledger/agents/stub_agents.py
Stubs for remainder of 5 LangGraph Agents
"""
from typing import TypedDict, List, Dict, Any
from uuid import UUID
from ledger.agents.base_agent import BaseApexAgent
from ledger.schema.events import (
    DomainEvent, FraudCheckCompleted, DecisionGenerated, DecisionOutcome,
    ApplicationApproved, ApplicationDenied, ApplicationReferred
)

# Required by Gate Test
class ExtractionCompleted(DomainEvent):
    event_type: str = "ExtractionCompleted"
    application_id: UUID
    document_ids: List[UUID]
    quality_score: float

class ComplianceCheckCompleted(DomainEvent):
    event_type: str = "ComplianceCheckCompleted"
    application_id: UUID
    overall_verdict: str
    checks_passed: int

class BaseState(TypedDict):
    application_id: UUID
    expected_version: int
    events: List[DomainEvent]
    node_sequence: int
    data: Dict[str, Any]


class DocumentProcessingAgent(BaseApexAgent):
    def __init__(self, event_store):
        super().__init__("gpt-4-vision", "v1", event_store)

    async def process(self, state: BaseState) -> BaseState:
        state["events"].append(self.start_session(state["application_id"]))
        state["events"].append(self.record_node("validate_inputs", state["node_sequence"]))
        state["node_sequence"] += 1
        
        for node in ["validate_document_formats", "extract_income_statement", "extract_balance_sheet", "assess_quality"]:
            state["events"].append(self.record_node(node, state["node_sequence"], 1000, 200, 0.05))
            state["node_sequence"] += 1

        state["events"].append(ExtractionCompleted(
            application_id=state["application_id"],
            document_ids=[],
            quality_score=0.9
        ))
        
        await self.write_output_node(state)
        return state

    async def write_output_node(self, state: BaseState):
        stream_id = f"docpkg-{state['application_id']}"
        await self.write_output(stream_id, state["events"], state["application_id"], state["expected_version"])


class FraudDetectionAgent(BaseApexAgent):
    def __init__(self, event_store):
        super().__init__("gpt-4-turbo", "v1", event_store)

    async def process(self, state: BaseState) -> BaseState:
        state["events"].append(self.start_session(state["application_id"]))
        
        nodes = ["load_facts", "cross_reference_registry", "analyze_fraud_patterns"]
        for node in nodes:
            state["events"].append(self.record_node(node, state["node_sequence"], 500, 100, 0.01))
            state["node_sequence"] += 1

        # Logic wrapper
        fraud_score = state["data"].get("fraud_score", 0.10)
        flags = []
        if fraud_score > 0.60:
            flags.append("DECLINE")
        elif fraud_score >= 0.30:
            flags.append("FLAG_FOR_REVIEW")
            
        state["events"].append(FraudCheckCompleted(
            fraud_risk_score=fraud_score,
            flags=flags,
            passed=(fraud_score <= 0.60)
        ))
        
        await self.write_output_node(state)
        return state

    async def write_output_node(self, state: BaseState):
        stream_id = f"fraud-{state['application_id']}"
        await self.write_output(stream_id, state["events"], state["application_id"], state["expected_version"])


class ComplianceAgent(BaseApexAgent):
    def __init__(self, event_store):
        # Deterministic Python (NO LLM in decision path)
        super().__init__("deterministic-python", "v1", event_store)

    async def process(self, state: BaseState) -> BaseState:
        state["events"].append(self.start_session(state["application_id"]))
        
        verdict = "PASS"
        checks = 0
        rules = ["REG-001", "REG-002", "REG-003", "REG-004", "REG-005", "REG-006"]
        
        for rule in rules:
            state["events"].append(self.record_node(rule, state["node_sequence"]))
            state["node_sequence"] += 1
            checks += 1
            if rule == "REG-003" and state["data"].get("state") == "Montana":
                verdict = "BLOCKED"
                break
                
        state["events"].append(ComplianceCheckCompleted(
            application_id=state["application_id"],
            overall_verdict=verdict,
            checks_passed=checks
        ))
        
        await self.write_output_node(state)
        return state

    async def write_output_node(self, state: BaseState):
        stream_id = f"compliance-{state['application_id']}"
        await self.write_output(stream_id, state["events"], state["application_id"], state["expected_version"])


class DecisionOrchestratorAgent(BaseApexAgent):
    def __init__(self, event_store):
        super().__init__("gpt-4-turbo", "v1", event_store)

    async def process(self, state: BaseState) -> BaseState:
        state["events"].append(self.start_session(state["application_id"]))
        
        for node in ["load_credit", "load_fraud", "load_compliance", "synthesize_decision", "apply_hard_constraints"]:
            state["events"].append(self.record_node(node, state["node_sequence"]))
            state["node_sequence"] += 1

        d = state["data"]
        compliance_verdict = d.get("compliance_verdict", "PASS")
        confidence = d.get("confidence", 0.90)
        fraud = d.get("fraud_score", 0.0)
        
        if compliance_verdict == "BLOCKED":
            outcome = DecisionOutcome.DENY
            decision_evt = ApplicationDenied(denial_reasons=["Compliance BLOCKED"], denied_by="orchestrator")
        elif confidence < 0.60 or fraud > 0.60:
            outcome = DecisionOutcome.REFER
            decision_evt = ApplicationReferred(referral_reason="Threshold criteria failed", referred_to="human")
        else:
            outcome = DecisionOutcome.APPROVE
            decision_evt = ApplicationApproved(approved_amount=500000, interest_rate=5.0, approved_by="orchestrator")

        state["events"].append(DecisionGenerated(
            outcome=outcome,
            confidence_score=confidence,
            reasoning="Aggregated signals",
            model_version="v1",
            agent_session_id=state["events"][0].event_id # SessionStarted id
        ))
        state["events"].append(decision_evt)
        
        await self.write_output_node(state)
        return state

    async def write_output_node(self, state: BaseState):
        stream_id = f"loan-{state['application_id']}"
        await self.write_output(stream_id, state["events"], state["application_id"], state["expected_version"])
