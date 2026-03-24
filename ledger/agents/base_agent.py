"""
ledger/agents/base_agent.py
Gas Town, Node tracking, Tool Tracking, and OCC retry scaffolding for 5 agents.
"""
from typing import Any, Dict, List, Callable, Coroutine
from uuid import UUID, uuid4
from ledger.schema.events import (
    DomainEvent, AgentSessionStarted, AgentNodeExecuted, AgentToolCalled, AgentSessionCompleted
)

class BaseApexAgent:
    def __init__(self, agent_model: str, agent_version: str, event_store=None):
        self.agent_model = agent_model
        self.agent_version = agent_version
        self.event_store = event_store
        
    def start_session(self, application_id: UUID) -> AgentSessionStarted:
        """Gas Town: Append AgentSessionStarted before any work."""
        return AgentSessionStarted(
            application_id=application_id,
            agent_model=self.agent_model,
            agent_version=self.agent_version
        )
        
    def record_node(self, node_name: str, node_sequence: int, tokens_in: int = 0, tokens_out: int = 0, cost_usd: float = 0.0) -> AgentNodeExecuted:
        """Node Recording: Append AgentNodeExecuted at END of every node."""
        return AgentNodeExecuted(
            node_name=node_name,
            node_sequence=node_sequence,
            llm_tokens_input=tokens_in,
            llm_tokens_output=tokens_out,
            llm_cost_usd=cost_usd
        )

    def record_tool(self, tool_name: str, input_summary: str, output_summary: str) -> AgentToolCalled:
        """Tool Recording: Append AgentToolCalled for every registry/store query."""
        return AgentToolCalled(
            tool_name=tool_name,
            tool_input_summary=input_summary,
            tool_output_summary=output_summary
        )
        
    async def write_output(self, stream_id: str, events: List[DomainEvent], application_id: UUID, expected_version: int) -> int:
        """
        OCC Retry: Scaffold retry logic using EventStore.
        Expects self.event_store to be an instance of EventStore.
        """
        if not self.event_store:
            raise ValueError("EventStore must be provided to use write_output OCC Retry")
            
        events.append(AgentSessionCompleted(
            application_id=application_id,
            total_duration_ms=1000,
            events_processed=len(events)
        ))
        
        async def command_fn() -> tuple[List[DomainEvent], int]:
            # This is standard OCC retry design; the command computes state anew and expected_version.
            # In real agents, evaluating node logic should rerun inside command_fn.
            # For this Base wrapper scaffold, we pass the static payload + expected back to Event Store wrapper.
            return events, expected_version
            
        return await self.event_store.append_with_retry(stream_id, command_fn)
