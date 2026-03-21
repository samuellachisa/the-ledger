"""
Registered upcasters for schema evolution.

Each upcaster handles exactly one version transition.
Missing historical fields are set to null — NEVER fabricated.

Inference strategy documentation:
- CreditAnalysisCompleted v1→v2: model_version=null (cannot determine which model ran historically)
  analysis_duration_ms=null (not recorded in v1)
- DecisionGenerated v1→v2: reasoning=null (not recorded in v1; fabricating reasoning
  would be a compliance violation — auditors must see honest null)
- AgentContextLoaded v1→v2: loaded_stream_positions={} (empty dict is safe default;
  Gas Town checkpoints were not tracked in v1, so recovery will re-fetch all sources)
"""
from __future__ import annotations

from src.upcasting.registry import UpcasterRegistry


def register_all(registry: UpcasterRegistry) -> None:
    """Register all upcasters into the given registry."""

    # -------------------------------------------------------------------------
    # CreditAnalysisCompleted v1 → v2
    # Change: added model_version (which ML model ran the analysis)
    #         added analysis_duration_ms (processing time)
    # Inference strategy: null — we cannot determine which model ran historically.
    # Fabricating a model version would be a compliance violation.
    # -------------------------------------------------------------------------
    @registry.register("CreditAnalysisCompleted", from_version=1)
    def upcast_credit_v1_to_v2(payload: dict) -> dict:
        return {
            **payload,
            "model_version": payload.get("model_version", None),
            "analysis_duration_ms": payload.get("analysis_duration_ms", None),
        }

    # -------------------------------------------------------------------------
    # DecisionGenerated v1 → v2
    # Change: added reasoning field (agent's explanation for the decision)
    # Inference strategy: null — reasoning was not recorded in v1.
    # An auditor asking "why was this decision made?" must receive an honest null,
    # not a fabricated explanation.
    # -------------------------------------------------------------------------
    @registry.register("DecisionGenerated", from_version=1)
    def upcast_decision_v1_to_v2(payload: dict) -> dict:
        return {
            **payload,
            "reasoning": payload.get("reasoning", None),
        }

    # -------------------------------------------------------------------------
    # AgentContextLoaded v1 → v2
    # Change: added loaded_stream_positions (Gas Town checkpoint data)
    # Inference strategy: empty dict {} — v1 sessions did not record stream positions.
    # On crash recovery, the agent will re-fetch all context sources since no
    # checkpoint exists. This is safe (idempotent re-fetch) and honest.
    # -------------------------------------------------------------------------
    @registry.register("AgentContextLoaded", from_version=1)
    def upcast_agent_context_v1_to_v2(payload: dict) -> dict:
        return {
            **payload,
            "loaded_stream_positions": payload.get("loaded_stream_positions", {}),
        }

    # -------------------------------------------------------------------------
    # AgentContextLoaded v2 → v3
    # Change: added model_version (which model will process this context)
    # Inference strategy: null — cannot determine which model was intended historically.
    # -------------------------------------------------------------------------
    @registry.register("AgentContextLoaded", from_version=2)
    def upcast_agent_context_v2_to_v3(payload: dict) -> dict:
        return {
            **payload,
            "model_version": payload.get("model_version", None),
        }
