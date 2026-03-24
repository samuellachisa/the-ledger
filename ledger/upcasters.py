"""
ledger/upcasters.py
Centralized registry applies version migrations automatically on event load.
"""
from typing import Tuple, Dict, Any

def upcast_credit_analysis_v1_to_v2(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload["model_version"] = payload.get("model_version", "inferred")
    payload["confidence_score"] = payload.get("confidence_score", None)
    payload["regulatory_basis"] = payload.get("regulatory_basis", "inferred")
    return payload

def upcast_decision_v1_to_v2(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload["model_versions"] = payload.get("model_versions", {})
    return payload

UPCASTER_REGISTRY = {
    "CreditAnalysisCompleted": {
        1: upcast_credit_analysis_v1_to_v2
    },
    "DecisionGenerated": {
        1: upcast_decision_v1_to_v2
    }
}

def apply_upcasters(event_type: str, event_version: int, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
    """Applies sequential upcasters to an event payload dict. Preserves original dict."""
    current_version = event_version
    current_payload = dict(payload)
    
    upcasters_for_type = UPCASTER_REGISTRY.get(event_type, {})
    
    while current_version in upcasters_for_type:
        upcaster = upcasters_for_type[current_version]
        current_payload = upcaster(current_payload)
        current_version += 1
        
    return current_version, current_payload
