"""
ledger/agents/credit_analysis_agent.py
Implementation of CreditAnalysisAgent (Reference)
"""
from typing import TypedDict, List, Dict, Any
from uuid import UUID
from ledger.agents.base_agent import BaseApexAgent
from ledger.schema.events import DomainEvent, CreditAnalysisCompleted, DecisionOutcome

class CreditState(TypedDict):
    application_id: UUID
    company_id: UUID
    expected_version: int
    events: List[DomainEvent]
    node_sequence: int
    data: Dict[str, Any]
    analysis_result: Dict[str, Any]

class CreditAnalysisAgent(BaseApexAgent):
    def __init__(self, event_store, registry_client):
        super().__init__("gpt-4-turbo", "v1", event_store)
        self.registry = registry_client
        
    async def process(self, state: CreditState) -> CreditState:
        """Standard compilation wrapper simulating LangGraph flow."""
        state = await self.validate_inputs(state)
        state = await self.open_aggregate_record(state)
        state = await self.load_external_data(state)
        state = await self.analyze_credit_risk(state)
        await self.write_output_node(state)
        return state

    async def validate_inputs(self, state: CreditState) -> CreditState:
        # Gas Town Session
        state["events"].append(self.start_session(state["application_id"]))
        state["events"].append(self.record_node("validate_inputs", state["node_sequence"]))
        state["node_sequence"] += 1
        return state

    async def open_aggregate_record(self, state: CreditState) -> CreditState:
        # Usually loads stream from EventStore
        state["events"].append(self.record_node("open_aggregate_record", state["node_sequence"]))
        state["node_sequence"] += 1
        return state

    async def load_external_data(self, state: CreditState) -> CreditState:
        company = await self.registry.get_company(state["company_id"])
        state["events"].append(self.record_tool("registry.get_company", str(state["company_id"]), "Company data loaded"))
        financials = await self.registry.get_financial_history(state["company_id"])
        flags = await self.registry.get_compliance_flags(state["company_id"])
        
        state["data"] = {
            "company": company,
            "financials": financials,
            "flags": flags
        }
        
        state["events"].append(self.record_node("load_external_data", state["node_sequence"]))
        state["node_sequence"] += 1
        return state

    async def analyze_credit_risk(self, state: CreditState) -> CreditState:
        """
        Policy Rules (Python enforced):
        - Max loan-to-revenue ratio: 35%.
        - Prior default -> risk_tier = HIGH.
        - Active HIGH compliance flag -> confidence <= 0.50.
        - confidence < 0.60 -> REFER.
        """
        data = state["data"]
        confidence = 0.90
        risk_tier = "LOW"
        outcome = DecisionOutcome.APPROVE
        
        # In a real scenario, this involves 1 LLM call to get baseline analysis parameters.
        # We track LLM usage metrics:
        tokens_in = 1500
        tokens_out = 400
        cost = 0.05
        
        # 1. Active HIGH compliance flag
        has_high_flag = any(f["severity"] == "HIGH" for f in data.get("flags", []))
        if has_high_flag:
            confidence = min(confidence, 0.50)
            
        # 2. Prior default (simulated flag or status)
        has_prior_default = any("Default" in f.get("description", "") for f in data.get("flags", []))
        if has_prior_default:
            risk_tier = "HIGH"
            
        # 3. Max loan-to-revenue ratio: 35%
        # Assuming we need application volume; mock 500k for demonstration vs revenue
        recent_revenue = data["financials"][0]["revenue"] if data.get("financials") else 1_000_000
        mock_loan_amount = 400_000 
        if (mock_loan_amount / max(1, recent_revenue)) > 0.35:
            confidence = min(confidence, 0.40) # Drives decision to REFER or DENY
            
        # 4. confidence < 0.60 -> REFER
        if confidence < 0.60:
            outcome = DecisionOutcome.REFER
            
        # Add core event
        state["events"].append(CreditAnalysisCompleted(
            credit_score=720,
            debt_to_income_ratio=0.30,
            model_version=self.agent_version,
            analysis_duration_ms=1200
        ))
        
        state["analysis_result"] = {
            "outcome": outcome,
            "confidence": confidence,
            "risk_tier": risk_tier
        }
        
        state["events"].append(self.record_node("analyze_credit_risk", state["node_sequence"], tokens_in, tokens_out, cost))
        state["node_sequence"] += 1
        return state

    async def write_output_node(self, state: CreditState):
        stream_id = f"credit-{state['application_id']}"
        await self.write_output(
            stream_id=stream_id,
            events=state["events"],
            application_id=state["application_id"],
            expected_version=state["expected_version"]
        )
