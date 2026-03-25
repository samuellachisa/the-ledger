"""
ledger/agents/base_agent.py
Gas Town, Node tracking, Tool Tracking, and OCC retry scaffolding for 5 agents.
"""
import time
from typing import Any, Dict, List, Callable, Coroutine, Optional
from uuid import UUID, uuid4
from ledger.schema.events import (
    DomainEvent, AgentSessionStarted, AgentNodeExecuted, AgentToolCalled, AgentSessionCompleted,
    AgentSessionFailed, AgentSessionRecovered
)

class SimulatedCrashError(Exception):
    """Exception raised when simulating agent crash for testing."""
    pass

class BaseApexAgent:
    def __init__(self, agent_model: str, agent_version: str, event_store=None):
        self.agent_model = agent_model
        self.agent_version = agent_version
        self.event_store = event_store
        self.session_id: UUID = uuid4()
        
        # Crash simulation hooks for testing (NARR-03)
        self._crash_after_node: Optional[str] = None
        self._recover_from_session: Optional[UUID] = None
        self._last_successful_node: Optional[str] = None
        
    def start_session(self, application_id: UUID, context_source: str = "") -> AgentSessionStarted:
        """Gas Town: Append AgentSessionStarted before any work."""
        config = {"session_id": str(self.session_id)}
        if context_source:
            config["context_source"] = context_source
        if self._recover_from_session:
            config["recovered_from_session"] = str(self._recover_from_session)
            
        return AgentSessionStarted(
            application_id=application_id,
            agent_model=self.agent_model,
            agent_version=self.agent_version,
            session_config=config
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
        
    async def _record_node_execution(
        self, 
        node_name: str, 
        input_keys: List[str], 
        output_keys: List[str],
        duration_ms: int = 0,
        llm_tokens_input: int = 0,
        llm_tokens_output: int = 0,
        llm_cost_usd: float = 0.0
    ) -> AgentNodeExecuted:
        """Record node execution with full metrics."""
        self._last_successful_node = node_name
        return AgentNodeExecuted(
            node_name=node_name,
            node_sequence=0,  # Will be set by caller
            llm_tokens_input=llm_tokens_input,
            llm_tokens_output=llm_tokens_output,
            llm_cost_usd=llm_cost_usd
        )
    
    def _calculate_cost(self, tokens_in: int, tokens_out: int, model: str = "gpt-4-turbo") -> float:
        """Calculate LLM API cost based on token usage.
        
        Pricing (per 1M tokens):
        - gpt-4-turbo: $10 input, $30 output
        - gpt-4-vision: $10 input, $30 output  
        - gpt-3.5-turbo: $0.50 input, $1.50 output
        """
        pricing = {
            "gpt-4-turbo": (10.0, 30.0),
            "gpt-4-vision": (10.0, 30.0),
            "gpt-3.5-turbo": (0.50, 1.50),
        }
        input_price, output_price = pricing.get(model, (10.0, 30.0))
        
        cost = (tokens_in / 1_000_000 * input_price) + (tokens_out / 1_000_000 * output_price)
        return round(cost, 6)
    
    async def _call_llm(self, system_prompt: str, user_message: str, model: str = None) -> Any:
        """Call LLM with token tracking. Stub implementation - replace with actual LLM client."""
        # This is a stub - in production, integrate with OpenAI, Anthropic, etc.
        # For now, return a mock response structure
        model = model or self.agent_model
        
        # Simulate token counts
        tokens_in = len(system_prompt.split()) + len(user_message.split())
        tokens_out = 200  # Mock response size
        
        class MockResponse:
            def __init__(self, content, usage):
                self.content = content
                self.usage = usage
                
        class MockUsage:
            def __init__(self, input_tokens, output_tokens):
                self.input_tokens = input_tokens
                self.output_tokens = output_tokens
        
        # Return mock response - subclasses should override for real LLM integration
        return MockResponse(
            content='{}',
            usage=MockUsage(tokens_in, tokens_out)
        )
        
    async def write_output(
        self, 
        aggregate_type: str, 
        aggregate_id: UUID,
        events: List[DomainEvent], 
        expected_version: int
    ) -> int:
        """
        OCC Retry: Scaffold retry logic using EventStore.
        Supports both ledger.event_store.EventStore (stream_id-based) and
        src.event_store.EventStore (aggregate_type/aggregate_id-based).
        """
        if not self.event_store:
            raise ValueError("EventStore must be provided to use write_output OCC Retry")
            
        events.append(AgentSessionCompleted(
            application_id=aggregate_id,
            total_duration_ms=1000,
            events_processed=len(events)
        ))
        
        # Detect which EventStore API is in use
        import inspect
        append_sig = inspect.signature(self.event_store.append)
        params = list(append_sig.parameters.keys())
        
        if "aggregate_type" in params:
            # src.event_store.EventStore API: append(aggregate_type, aggregate_id, events, expected_version)
            # On retry, reload the current version so we append at the right position
            # Capture a snapshot of the events to avoid mutation issues
            import copy
            events_snapshot = events[:]
            
            async def command_fn():
                current_version = await self.event_store.stream_version(aggregate_type, aggregate_id)
                # Regenerate event_ids to avoid duplicate key violations on retry
                fresh_events = []
                for ev in events_snapshot:
                    ev_copy = ev.model_copy(update={"event_id": uuid4()})
                    fresh_events.append(ev_copy)
                return fresh_events, current_version
            return await self.event_store.append_with_retry(aggregate_type, aggregate_id, command_fn)
        else:
            # ledger.event_store.EventStore API: append(stream_id, events, expected_version)
            prefix_map = {
                "LoanApplication": "loan",
                "DocumentPackage": "docpkg",
                "AgentSession": "agent",
                "CreditAnalysis": "credit",
                "FraudScreening": "fraud",
                "ComplianceCheck": "compliance",
                "AuditLedger": "audit",
            }
            prefix = prefix_map.get(aggregate_type, aggregate_type.lower())
            stream_id = f"{prefix}-{aggregate_id}"
            
            async def command_fn_ledger():
                current_version = await self.event_store.stream_version(stream_id)
                return events, current_version
            return await self.event_store.append_with_retry(stream_id, command_fn_ledger)
