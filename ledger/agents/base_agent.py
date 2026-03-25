"""
ledger/agents/base_agent.py
Gas Town, Node tracking, Tool Tracking, and OCC retry scaffolding for 5 agents.
"""
import os
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


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _llm_should_use_real() -> bool:
    """
    Real vs stub is controlled by environment (see `.env.example`).

    - `LLM_MODE=stub|real` (preferred). When unset, falls back to `USE_REAL_LLM`.
    """
    mode = (os.getenv("LLM_MODE") or "").strip().lower()
    if mode == "stub":
        return False
    if mode == "real":
        return True
    return _env_bool("USE_REAL_LLM", False)


def _llm_openai_compatible_config(model_arg: Optional[str], agent_model: str) -> Dict[str, Any]:
    """
    Resolve OpenAI-compatible chat-completions settings from env.

    Canonical keys (preferred):
      LLM_API_KEY, LLM_BASE_URL, LLM_MODEL, LLM_TEMPERATURE

    Backward-compatible fallbacks:
      GROQ_* / OPENAI_* and per-call `model` / `agent_model`.
    """
    api_key = (
        (os.getenv("LLM_API_KEY") or "").strip()
        or (os.getenv("GROQ_API_KEY") or "").strip()
        or (os.getenv("OPENAI_API_KEY") or "").strip()
    )
    base_url = (
        (os.getenv("LLM_BASE_URL") or "").strip()
        or (os.getenv("GROQ_BASE_URL") or "").strip()
        or "https://api.groq.com/openai/v1/chat/completions"
    )
    llm_model = (
        (os.getenv("LLM_MODEL") or "").strip()
        or (os.getenv("GROQ_MODEL") or "").strip()
        or (model_arg or agent_model or "").strip()
    )
    try:
        temperature = float(os.getenv("LLM_TEMPERATURE", "0.0"))
    except ValueError:
        temperature = 0.0
    timeout_s = float(os.getenv("LLM_TIMEOUT_SECONDS", "60"))
    provider = (os.getenv("LLM_PROVIDER") or "openai_compatible").strip().lower()
    return {
        "api_key": api_key,
        "base_url": base_url,
        "model": llm_model,
        "temperature": temperature,
        "timeout_s": timeout_s,
        "provider": provider,
    }


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
        # Gas Town contract: every session explicitly declares where its context comes from.
        # - "fresh" for a new run
        # - "prior_session_replay:{id}" when recovering
        config["context_source"] = context_source or "fresh"
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
        """
        Call LLM with token tracking.

        Configuration is read from the environment (see `.env.example`):

        - `LLM_MODE=stub|real` — stub is default for deterministic tests; `real` calls an
          OpenAI-compatible `POST .../chat/completions` endpoint.
        - If `LLM_MODE` is unset, `USE_REAL_LLM=true` still enables real calls (legacy).

        Real-call settings (canonical): `LLM_API_KEY`, `LLM_BASE_URL`, `LLM_MODEL`,
        `LLM_TEMPERATURE`, `LLM_TIMEOUT_SECONDS`. Falls back to `GROQ_*` / `OPENAI_*`
        when canonical keys are empty.
        """
        use_real = _llm_should_use_real()
        if not use_real:
            # Stub response for deterministic tests / offline runs.
            tokens_in = len(system_prompt.split()) + len(user_message.split())
            tokens_out = 200  # Mock response size

            class MockUsage:
                def __init__(self, input_tokens, output_tokens):
                    self.input_tokens = input_tokens
                    self.output_tokens = output_tokens

            class MockResponse:
                def __init__(self, content, usage):
                    self.content = content
                    self.usage = usage

            return MockResponse(
                content="{}",
                usage=MockUsage(tokens_in, tokens_out),
            )

        cfg = _llm_openai_compatible_config(model, self.agent_model)
        api_key = cfg["api_key"]
        if not api_key:
            raise RuntimeError(
                "Real LLM enabled but no API key: set LLM_API_KEY "
                "(or GROQ_API_KEY / OPENAI_API_KEY)."
            )
        llm_model = cfg["model"]
        if not llm_model:
            raise RuntimeError(
                "Real LLM enabled but no model: set LLM_MODEL (or GROQ_MODEL), "
                "or pass a non-empty `model` / agent_model."
            )

        base_url = cfg["base_url"]
        timeout_s = cfg["timeout_s"]

        # Local imports to avoid dependency/network usage during stub mode.
        import httpx

        payload = {
            "model": llm_model,
            "temperature": cfg["temperature"],
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
        }

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        try:
            langsmith_tracing = _env_bool("LANGSMITH_TRACING_V2", False)

            if langsmith_tracing:
                from langsmith import traceable

                trace_name = os.getenv("LLM_LANGSMITH_RUN_NAME", "llm.chat_completions")

                @traceable(
                    run_type="llm",
                    name=trace_name,
                    project_name=os.getenv("LANGSMITH_PROJECT") or None,
                    metadata={
                        "agent_model": self.agent_model,
                        "agent_version": self.agent_version,
                        "llm_provider": cfg["provider"],
                        "llm_model": llm_model,
                        "llm_base_url": base_url,
                    },
                )
                async def _openai_compatible_chat_completions() -> dict:
                    async with httpx.AsyncClient(timeout=timeout_s) as client:
                        resp = await client.post(base_url, headers=headers, json=payload)
                        resp.raise_for_status()
                        return resp.json()

                data = await _openai_compatible_chat_completions()
            else:
                async with httpx.AsyncClient(timeout=timeout_s) as client:
                    resp = await client.post(base_url, headers=headers, json=payload)
                    resp.raise_for_status()
                    data = resp.json()

            # Normalize to the same response shape our agents expect.
            content = ""
            try:
                content = data["choices"][0]["message"]["content"] or ""
            except Exception:
                content = data.get("content", "") or ""

            usage = data.get("usage") or {}
            input_tokens = usage.get("prompt_tokens") or usage.get("input_tokens") or 0
            output_tokens = usage.get("completion_tokens") or usage.get("output_tokens") or 0
        except Exception as e:
            # Keep the pipeline resilient: if the external LLM endpoint fails,
            # fall back to deterministic token estimates.
            import logging
            status = None
            body_snippet = None
            try:
                # httpx raises HTTPStatusError with response attached for non-2xx.
                status = getattr(getattr(e, "response", None), "status_code", None)
                body = getattr(getattr(e, "response", None), "text", None)
                if isinstance(body, str) and body:
                    body_snippet = body[:500]
            except Exception:
                pass

            msg_parts = [f"Real LLM call failed; falling back to stub: {e}"]
            if status is not None:
                msg_parts.append(f"(status={status})")
            if body_snippet:
                msg_parts.append(f"(body={body_snippet})")
            logging.getLogger(__name__).warning(" ".join(msg_parts))
            tokens_in = len(system_prompt.split()) + len(user_message.split())
            input_tokens = tokens_in
            output_tokens = 200
            content = "{}"

        class RealUsage:
            def __init__(self, input_tokens, output_tokens):
                self.input_tokens = int(input_tokens)
                self.output_tokens = int(output_tokens)

        class RealResponse:
            def __init__(self, content, usage):
                self.content = content
                self.usage = usage

        return RealResponse(content=content, usage=RealUsage(input_tokens, output_tokens))
        
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
