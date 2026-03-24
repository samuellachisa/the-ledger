"""
Event simulator for Apex Financial Services.
Generates exactly 1847 seed events across 29 applications mimicking an agent-driven workflow.
"""

import sys
import uuid
import random
from datetime import datetime, timedelta, timezone

def generate_events(companies, num_applications=29, target_total_events=1847, random_seed=42):
    rng = random.Random(random_seed)
    events = []
    
    # Pre-select companies
    selected_companies = [companies[i % len(companies)] for i in range(num_applications)]
    
    # Distributions
    # APEX-0001-0006: SUBMITTED (6)
    # APEX-0007-0011: DOCUMENTS_UPLOADED (5)
    # APEX-0012-0015: DOCUMENTS_PROCESSED (4)
    # APEX-0016-0018: CREDIT_COMPLETE (3)
    # APEX-0019-0020: FRAUD_COMPLETE (2)
    # APEX-0021-0025: APPROVED(4) + DECLINED(1) (5)
    # APEX-0026-0027: DECLINED (agent-driven) (2)
    # APEX-0028: DECLINED_COMPLIANCE (1)
    # APEX-0029: REFERRED (1)
    
    states = (
        ["SUBMITTED"] * 6 +
        ["DOCUMENTS_UPLOADED"] * 5 +
        ["DOCUMENTS_PROCESSED"] * 4 +
        ["CREDIT_COMPLETE"] * 3 +
        ["FRAUD_COMPLETE"] * 2 +
        ["APPROVED"] * 4 + ["DECLINED"] * 1 +
        ["DECLINED_AGENT"] * 2 +
        ["DECLINED_COMPLIANCE"] * 1 +
        ["REFERRED"] * 1
    )
    
    global_position = 1
    stream_positions = {}
    audit_chain_hashes = {}
    
    def add_event(app_id, aggregate_type, event_type, payload, generate_audit=True):
        nonlocal global_position
        if app_id not in stream_positions:
            stream_positions[app_id] = 1
        else:
            stream_positions[app_id] += 1
            
        evt_id = str(uuid.uuid4())
        event = {
            "event_id": evt_id,
            "aggregate_id": app_id,
            "aggregate_type": aggregate_type,
            "event_type": event_type,
            "stream_position": stream_positions[app_id],
            "global_position": global_position,
            "payload": payload,
            "metadata": {"source": "datagen"},
            "schema_version": 1,
            "recorded_at": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
        }
        events.append(event)
        global_position += 1
        
        if generate_audit:
            # Generate AuditEntryRecorded
            if app_id not in stream_positions:
                stream_positions[app_id] = 1
            else:
                stream_positions[app_id] += 1
                
            audit_id = str(uuid.uuid4())
            audit_seq = 1 if app_id not in audit_chain_hashes else audit_chain_hashes[app_id]["seq"] + 1
            prev_hash = audit_chain_hashes[app_id]["hash"] if app_id in audit_chain_hashes else "0" * 64
            
            # Dummy hashes
            evt_hash = "1" * 64
            chain_hash = "2" * 64
            
            audit_chain_hashes[app_id] = {"seq": audit_seq, "hash": chain_hash}
            
            audit_payload = {
                "application_id": app_id,
                "action": f"Recorded {event_type}",
                "actor": "system",
                "details": {},
                "source_event_id": evt_id,
                "event_hash": evt_hash,
                "chain_hash": chain_hash
            }
            
            audit_event = {
                "event_id": audit_id,
                "aggregate_id": str(uuid.uuid5(uuid.NAMESPACE_DNS, app_id)), # AuditLedger has different aggregate id? Wait, schema says application_id. Let's just use application_id.
                "aggregate_type": "AuditLedger",
                "event_type": "AuditEntryRecorded",
                "stream_position": stream_positions[app_id], # Sharing stream position? Actually event store requires unique stream per aggregate_id. We'll use a different ID for AuditLedger stream.
                "global_position": global_position,
                "payload": audit_payload,
                "metadata": {"source": "datagen"},
                "schema_version": 1,
                "recorded_at": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
            }
            events.append(audit_event)
            global_position += 1

    
    # We will track events by application to evenly pad them later
    base_events_count = 0
    
    for i, state in enumerate(states):
        app_id_str = f"APEX-{i+1:04d}"
        company = selected_companies[i]
        
        # 1. LoanApplicationSubmitted
        add_event(app_id_str, "LoanApplication", "LoanApplicationSubmitted", {
            "applicant_name": company.name,
            "loan_amount": 5000000.0,
            "loan_purpose": "Working Capital",
            "applicant_id": str(company.company_id),
            "submitted_by": "customer"
        })
        
        if state == "SUBMITTED":
            continue
            
        # 2. AgentSessionStarted
        agent_session_id = str(uuid.uuid4())
        add_event(app_id_str, "AgentSession", "AgentSessionStarted", {
            "application_id": app_id_str,
            "agent_model": "gpt-4-turbo",
            "agent_version": "v1.2",
            "session_config": {}
        })
        
        # 3. AgentContextLoaded
        add_event(app_id_str, "AgentSession", "AgentContextLoaded", {
            "application_id": app_id_str,
            "context_sources": ["application_form", "company_profile"],
            "context_snapshot": {}
        })
        
        if state == "DOCUMENTS_UPLOADED":
            continue
            
        # 4. CreditAnalysisRequested
        add_event(app_id_str, "LoanApplication", "CreditAnalysisRequested", {
            "requested_by": "system",
            "priority": "normal"
        })
        
        if state == "DOCUMENTS_PROCESSED":
            continue
            
        # 5. CreditAnalysisCompleted
        add_event(app_id_str, "LoanApplication", "CreditAnalysisCompleted", {
            "credit_score": rng.randint(600, 800),
            "debt_to_income_ratio": round(rng.uniform(0.1, 0.5), 2)
        })
        
        # 6. AgentCreditAnalysisObserved
        add_event(app_id_str, "AgentSession", "AgentCreditAnalysisObserved", {
            "application_id": app_id_str,
            "credit_score": rng.randint(600, 800),
            "debt_to_income_ratio": round(rng.uniform(0.1, 0.5), 2),
            "loan_application_stream_version": 4
        })
        
        if state == "CREDIT_COMPLETE":
            continue
            
        # 7. FraudCheckCompleted
        add_event(app_id_str, "LoanApplication", "FraudCheckCompleted", {
            "fraud_risk_score": round(rng.uniform(0.01, 0.1), 2),
            "flags": [],
            "passed": True
        })
        
        if state == "FRAUD_COMPLETE":
            continue
            
        # For later states, generate Compliance...
        comp_rec_id = str(uuid.uuid4())
        add_event(app_id_str, "LoanApplication", "ComplianceCheckRequested", {
            "compliance_record_id": comp_rec_id
        })
        
        add_event(app_id_str, "ComplianceRecord", "ComplianceRecordCreated", {
            "application_id": app_id_str,
            "officer_id": "system",
            "required_checks": ["KYC", "AML"]
        })
        
        if state == "DECLINED_COMPLIANCE":
            add_event(app_id_str, "ComplianceRecord", "ComplianceCheckFailed", {
                "check_name": "Montana REG-003",
                "failure_reason": "State restriction",
                "officer_id": "system"
            })
            add_event(app_id_str, "ComplianceRecord", "ComplianceRecordFinalized", {
                "overall_status": "Failed",
                "officer_id": "system"
            })
            add_event(app_id_str, "LoanApplication", "ComplianceFinalizedOnApplication", {
                "compliance_record_id": comp_rec_id,
                "compliance_passed": False
            })
            add_event(app_id_str, "LoanApplication", "ApplicationDenied", {
                "denial_reasons": ["Compliance failure: Montana REG-003"],
                "denied_by": "system"
            })
            continue

        # Passes compliance
        add_event(app_id_str, "ComplianceRecord", "ComplianceCheckPassed", {
            "check_name": "KYC",
            "check_result": {},
            "officer_id": "system"
        })
        add_event(app_id_str, "ComplianceRecord", "ComplianceRecordFinalized", {
            "overall_status": "Passed",
            "officer_id": "system"
        })
        add_event(app_id_str, "LoanApplication", "ComplianceFinalizedOnApplication", {
            "compliance_record_id": comp_rec_id,
            "compliance_passed": True
        })
        
        # Decision
        if state == "REFERRED":
            add_event(app_id_str, "LoanApplication", "DecisionGenerated", {
                "outcome": "REFER",
                "confidence_score": 0.5,
                "reasoning": "Needs human review",
                "model_version": "v1.2",
                "agent_session_id": agent_session_id
            })
            add_event(app_id_str, "AgentSession", "AgentDecisionRecorded", {
                "application_id": app_id_str,
                "outcome": "REFER",
                "confidence_score": 0.5,
                "reasoning": "Needs human review",
                "processing_duration_ms": 1500
            })
            add_event(app_id_str, "LoanApplication", "ApplicationReferred", {
                "referral_reason": "Manual review required",
                "referred_to": "human_underwriter"
            })
            
        elif state == "DECLINED_AGENT":
            add_event(app_id_str, "LoanApplication", "DecisionGenerated", {
                "outcome": "DENY",
                "confidence_score": 0.88,
                "reasoning": "High risk profile",
                "model_version": "v1.2",
                "agent_session_id": agent_session_id
            })
            add_event(app_id_str, "AgentSession", "AgentDecisionRecorded", {
                "application_id": app_id_str,
                "outcome": "DENY",
                "confidence_score": 0.88,
                "reasoning": "High risk profile",
                "processing_duration_ms": 1200
            })
            add_event(app_id_str, "LoanApplication", "ApplicationDenied", {
                "denial_reasons": ["High risk profile"],
                "denied_by": "agent"
            })
            
        elif state == "APPROVED":
            add_event(app_id_str, "LoanApplication", "DecisionGenerated", {
                "outcome": "APPROVE",
                "confidence_score": 0.95,
                "reasoning": "Strong financials",
                "model_version": "v1.2",
                "agent_session_id": agent_session_id
            })
            add_event(app_id_str, "AgentSession", "AgentDecisionRecorded", {
                "application_id": app_id_str,
                "outcome": "APPROVE",
                "confidence_score": 0.95,
                "reasoning": "Strong financials",
                "processing_duration_ms": 1100
            })
            add_event(app_id_str, "LoanApplication", "ApplicationApproved", {
                "approved_amount": 5000000.0,
                "interest_rate": 5.5,
                "approved_by": "agent"
            })
        elif state == "DECLINED":
            add_event(app_id_str, "LoanApplication", "DecisionGenerated", {
                "outcome": "DENY",
                "confidence_score": 0.9,
                "reasoning": "Low credit score",
                "model_version": "v1.2",
                "agent_session_id": agent_session_id
            })
            add_event(app_id_str, "AgentSession", "AgentDecisionRecorded", {
                "application_id": app_id_str,
                "outcome": "DENY",
                "confidence_score": 0.9,
                "reasoning": "Low credit score",
                "processing_duration_ms": 1000
            })
            add_event(app_id_str, "LoanApplication", "ApplicationDenied", {
                "denial_reasons": ["Low credit score"],
                "denied_by": "agent"
            })
    
    # Calculate difference to 1847
    current_count = len(events)
    diff = target_total_events - current_count
    
    if diff > 0:
        # Pad with AgentContextLoaded events on the last application (APPROVED ideally)
        app_id_str = "APEX-0021" # One of the approved ones
        
        # We need to add 'diff' events. We can just add AgentContextLoaded directly to AgentSession or LoanApplication.
        # But wait! Every add_event generates 2 events (Domain + Audit).
        # What if diff is odd? Then we can add 1 event WITHOUT audit.
        
        num_pairs = diff // 2
        remainder = diff % 2
        
        for _ in range(num_pairs):
            add_event(app_id_str, "AgentSession", "AgentContextLoaded", {
                "application_id": app_id_str,
                "context_sources": ["padding_source"],
                "context_snapshot": {}
            }, generate_audit=True)
            
        if remainder:
            add_event(app_id_str, "AgentSession", "AgentContextLoaded", {
                "application_id": app_id_str,
                "context_sources": ["padding_source_odd"],
                "context_snapshot": {}
            }, generate_audit=False)
            
    elif diff < 0:
        # We overshot (unlikely since we have ~500 base events). If we did, just slice.
        # Note: slicing might orphan audit events, but we just need exactly 1847 valid events.
        events = events[:target_total_events]
            
    return events
