"""
UpcasterRegistry: Applies schema migrations to event payloads at read time.

CRITICAL: Upcasters NEVER modify the database. They transform payloads in memory.
Each upcaster handles exactly one version transition (v1→v2, v2→v3, etc.).
Missing historical fields are set to null — never fabricated.
"""
from __future__ import annotations

import logging
from typing import Callable

logger = logging.getLogger(__name__)

# Type alias: (payload: dict) -> dict
UpcasterFn = Callable[[dict], dict]


class UpcasterRegistry:
    """
    Registry of upcasters keyed by (event_type, from_version).
    On load, events are passed through the chain until they reach current version.
    """

    def __init__(self):
        # Key: (event_type, from_version) → upcaster function
        self._upcasters: dict[tuple[str, int], UpcasterFn] = {}
        self._register_all()

    def register(self, event_type: str, from_version: int) -> Callable:
        """Decorator to register an upcaster."""
        def decorator(fn: UpcasterFn) -> UpcasterFn:
            self._upcasters[(event_type, from_version)] = fn
            return fn
        return decorator

    def upcast(self, event_type: str, payload: dict, from_version: int) -> tuple[dict, int]:
        """
        Apply upcasters in sequence until no more upcasters exist for the type.

        Returns:
            (upcasted_payload, final_schema_version)

        The raw DB payload is NEVER modified — we work on a copy.
        """
        current_payload = dict(payload)  # shallow copy — never mutate original
        current_version = from_version

        while True:
            key = (event_type, current_version)
            upcaster = self._upcasters.get(key)
            if upcaster is None:
                break
            try:
                current_payload = upcaster(current_payload)
                current_version += 1
                logger.debug("Upcasted %s v%d → v%d", event_type, current_version - 1, current_version)
            except Exception as e:
                logger.error("Upcaster failed for %s v%d: %s", event_type, current_version, e)
                break

        return current_payload, current_version

    def _register_all(self):
        """Register all known upcasters."""

        # CreditAnalysisCompleted v1 → v2
        # v1 had no model_version or analysis_duration_ms fields.
        # We use null (not inferred) — we cannot know which model ran historically.
        @self.register("CreditAnalysisCompleted", from_version=1)
        def upcast_credit_v1_to_v2(payload: dict) -> dict:
            return {
                **payload,
                "model_version": payload.get("model_version", None),
                "analysis_duration_ms": payload.get("analysis_duration_ms", None),
            }

        # DecisionGenerated v1 → v2
        # v1 had no reasoning field
        @self.register("DecisionGenerated", from_version=1)
        def upcast_decision_v1_to_v2(payload: dict) -> dict:
            return {
                **payload,
                "reasoning": payload.get("reasoning", None),
            }

        # AgentContextLoaded v1 → v2
        # v1 had no loaded_stream_positions (Gas Town checkpoint data)
        @self.register("AgentContextLoaded", from_version=1)
        def upcast_agent_context_v1_to_v2(payload: dict) -> dict:
            return {
                **payload,
                "loaded_stream_positions": payload.get("loaded_stream_positions", {}),
            }

        # AgentContextLoaded v2 → v3
        # v2 had no model_version field.
        # Inference strategy: null — we cannot determine which model was intended historically.
        @self.register("AgentContextLoaded", from_version=2)
        def upcast_agent_context_v2_to_v3(payload: dict) -> dict:
            return {
                **payload,
                "model_version": payload.get("model_version", None),
            }
