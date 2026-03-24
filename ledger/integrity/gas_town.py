"""
ledger/integrity/gas_town.py
Gas Town Recovery tracking exact boundaries for node crash recoveries.
"""
from typing import Dict, Any
from ledger.event_store import EventStore

async def reconstruct_agent_context(store: EventStore, agent_id: str, session_id: str, token_budget: int = 8000) -> Dict[str, Any]:
    stream_id = f"agent-{agent_id}-{session_id}"
    events = await store.load_stream(stream_id)
    
    if not events:
        return {
            "status": "NOT_FOUND",
            "needs_reconciliation": False,
            "summary": "",
            "recent_events": []
        }
        
    is_completed = any(e.event_type == "AgentSessionCompleted" for e in events)
    needs_reconciliation = not is_completed
    
    verbatim_events = []
    summary_prose = ""
    
    if len(events) <= 3:
        verbatim_events = events
    else:
        verbatim_events = events[-3:]
        summary_source = events[:-3]
        
        parts = []
        for e in summary_source:
            if e.event_type == "AgentNodeExecuted":
                parts.append(f"{e.payload.get('node_name', 'node')} completed.")
            elif e.event_type in ["AgentSessionFailed", "NodePending", "AgentToolCalled"]:
                # Preserve ERROR/PENDING equivalent verbatim
                verbatim_events.insert(0, e)
                
        summary_prose = " ".join(parts)
        
    if len(summary_prose) > token_budget:
        summary_prose = summary_prose[:token_budget] + "..."
        
    return {
        "status": "COMPLETED" if is_completed else "IN_PROGRESS",
        "needs_reconciliation": needs_reconciliation,
        "summary": summary_prose,
        "recent_events": verbatim_events
    }
