"""
ledger/projections/agent_performance.py
AgentPerformanceLedger read-model
"""
from typing import Dict, Any
from ledger.event_store import StoredEvent

class AgentPerformanceLedger:
    def __init__(self):
        self.agents: Dict[str, Dict[str, Any]] = {}

    def _init_agent(self, agent_id: str, version: str):
        if agent_id not in self.agents:
            self.agents[agent_id] = {
                "agent_id": agent_id,
                "model_version": version,
                "analyses_completed": 0,
                "decisions_generated": 0,
                "avg_confidence_score": 0.0,
                "avg_duration_ms": 0.0,
                "approve_rate": 0.0,
                "decline_rate": 0.0,
                "refer_rate": 0.0,
                "human_override_rate": 0.0,
                "_total_conf": 0.0,
                "_total_duration": 0.0,
                "_approves": 0,
                "_declines": 0,
                "_refers": 0
            }

    async def handle_event(self, event: StoredEvent):
        p = event.payload
        t = event.event_type
        
        if t == "AgentSessionStarted":
            agent_id = p.get("agent_model")
            version = p.get("agent_version", "v1")
            if agent_id:
                self._init_agent(agent_id, version)

        elif t == "DecisionGenerated":
            # Assume model_version acts as agent_id for decisions coming directly from Orchestrator/LLM
            agent_id = p.get("model_version")
            if not agent_id: return
            self._init_agent(agent_id, agent_id)
            ag = self.agents[agent_id]
            
            ag["decisions_generated"] += 1
            ag["_total_conf"] += p.get("confidence_score", 0.0)
            ag["avg_confidence_score"] = ag["_total_conf"] / ag["decisions_generated"]
            
            out = p.get("outcome")
            if out == "APPROVE": ag["_approves"] += 1
            elif out == "DENY": ag["_declines"] += 1
            elif out == "REFER": ag["_refers"] += 1
            
            total = ag["_approves"] + ag["_declines"] + ag["_refers"]
            if total > 0:
                ag["approve_rate"] = ag["_approves"] / total
                ag["decline_rate"] = ag["_declines"] / total
                ag["refer_rate"] = ag["_refers"] / total

        elif t == "AgentSessionCompleted":
            if event.stream_id.startswith("agent-"):
                parts = event.stream_id.split("-")
                if len(parts) >= 2:
                    agent_id = parts[1] # e.g. agent-{type}-{session_id}
                    self._init_agent(agent_id, "v1")
                    ag = self.agents[agent_id]
                    ag["analyses_completed"] += 1
                    dur = p.get("total_duration_ms", 1000)
                    ag["_total_duration"] += dur
                    ag["avg_duration_ms"] = ag["_total_duration"] / ag["analyses_completed"]

    def get_ledger(self, agent_id: str) -> Dict[str, Any] | None:
        return self.agents.get(agent_id)
