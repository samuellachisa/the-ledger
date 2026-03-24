"""
ledger/regulatory/package.py
Regulatory Examination Package generator snapping deterministic historical aggregates.
"""
import json
import os
from datetime import datetime
import hashlib

from ledger.event_store import EventStore
from ledger.projections.application_summary import ApplicationSummaryProjection

async def generate_regulatory_package(store: EventStore, application_id: str, examination_date: datetime):
    # Fetch core streams
    loan_events = await store.load_stream(f"loan-{application_id}")
    comp_events = await store.load_stream(f"compliance-{application_id}")
    fraud_events = await store.load_stream(f"fraud-{application_id}")
    audit_events = await store.load_stream(f"audit-LoanApplication-{application_id}")
    
    all_events = sorted(loan_events + comp_events + fraud_events + audit_events, key=lambda x: x.recorded_at)
    
    # Filter boundary
    historical_events = [e for e in all_events if e.recorded_at <= examination_date]
    
    integrity_valid = True
    prev_hash = "GENESIS"
    for e in historical_events:
        canonical = json.dumps({"event_id": str(e.event_id), "payload": e.payload}, sort_keys=True, default=str)
        curr_hash = hashlib.sha256((prev_hash + canonical).encode()).hexdigest()
        e_hash = e.metadata.get("chain_hash")
        if e_hash and e_hash != curr_hash:
            integrity_valid = False
        prev_hash = curr_hash
        
    narrative = []
    models_used = []
    proj = ApplicationSummaryProjection()
    
    for e in historical_events:
        await proj.handle_event(e)
        t = e.event_type
        p = e.payload
        
        sentence = f"Event type {t} documented via system."
        if t == "LoanApplicationSubmitted": sentence = f"Application natively submitted requesting ${p.get('loan_amount')}."
        elif t == "CreditAnalysisCompleted": sentence = f"Credit metrics parsed resolving {p.get('credit_score')}."
        elif t == "FraudCheckCompleted": sentence = f"Fraud threshold checked producing score bounds of {p.get('fraud_risk_score')}."
        elif t == "DecisionGenerated": sentence = f"Orchestrator Decision explicitly generated routing action to {p.get('outcome')}."
        
        narrative.append(sentence)
        if p.get("model_version"):
            models_used.append({"model": p.get("model_version"), "confidence": p.get("confidence_score")})
            
    summary = proj.get_summary(application_id)
    
    package = {
        "application_id": application_id,
        "examination_date": str(examination_date),
        "integrity_verified_at_export": integrity_valid,
        "package_hash_chain": prev_hash,
        "projection_state": summary,
        "narrative": narrative,
        "models": models_used,
        "events": [
            {
                "id": str(e.event_id),
                "type": e.event_type,
                "payload": e.payload,
                "recorded_at": str(e.recorded_at)
            } for e in historical_events
        ]
    }
    
    os.makedirs("artifacts", exist_ok=True)
    with open("artifacts/regulatory_package_NARR05.json", "w") as f:
        json.dump(package, f, indent=2)
        
    return package
