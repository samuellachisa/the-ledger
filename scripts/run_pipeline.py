#!/usr/bin/env python3
"""
scripts/run_pipeline.py
Process one application through the agent pipeline.

Usage:
    python scripts/run_pipeline.py --app APEX-0007 --phase document
    python scripts/run_pipeline.py --app APEX-0012 --phase credit
    python scripts/run_pipeline.py --app APEX-0016 --phase fraud
    python scripts/run_pipeline.py --app APEX-0019 --phase compliance
    python scripts/run_pipeline.py --app APEX-0021 --phase decision
    python scripts/run_pipeline.py --app APEX-NEW-01 --phase full
"""
import asyncio
import argparse
import os
import sys
from pathlib import Path
from uuid import UUID
import hashlib

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.event_store import EventStore
from ledger.registry.client import ApplicantRegistryClient
from ledger.agents.stub_agents import (
    DocumentProcessingAgent,
    FraudDetectionAgent,
    ComplianceAgent,
    DecisionOrchestratorAgent
)
from ledger.agents.credit_analysis_agent import CreditAnalysisAgent


def _uuid_from_string(s: str) -> UUID:
    """
    Matches the deterministic UUID helper used by `tests/test_narratives.py`.
    """
    h = hashlib.md5(s.encode()).hexdigest()
    return UUID(h)


def _company_for_app(app_id: str) -> str:
    """
    Best-effort mapping for the narrative demos/tests.
    If an app isn't recognized, fall back to a stable placeholder.
    """
    mapping = {
        "APEX-NARR01": "COMP-031",
        "APEX-NARR02": "COMP-044",
        "APEX-NARR03": "COMP-057",
        "APEX-NARR04": "COMP-MT-001",
        "APEX-NARR05": "COMP-068",
    }
    return mapping.get(app_id, "COMP-001")


async def run_document_phase(app_id: str, event_store: EventStore, registry_client: ApplicantRegistryClient):
    """Run DocumentProcessingAgent."""
    print(f"\n{'='*60}")
    print(f"[DocumentProcessing] Processing {app_id}...")
    print(f"{'='*60}")

    application_uuid = _uuid_from_string(app_id)
    company_id_str = _company_for_app(app_id)
    company_uuid = _uuid_from_string(company_id_str)
    
    # Prepare state
    state = {
        "application_id": application_uuid,
        "company_id": company_uuid,
        "document_paths": {
            "income_statement": f"documents/{company_id_str}/income_statement_2024.txt",
            "balance_sheet": f"documents/{company_id_str}/balance_sheet_2024.txt",
        },
        "expected_version": 0,
        "events": [],
        "node_sequence": 0,
        "extracted_facts": {},
        "field_confidence": {},
        "extraction_notes": []
    }
    
    # Run agent
    agent = DocumentProcessingAgent(event_store, registry_client)
    result = await agent.process(state)
    
    print(f"[DocumentProcessing] ✓ Complete")
    print(f"  - Extracted facts: {len(result.get('extracted_facts', {}))}")
    print(f"  - Events generated: {len(result.get('events', []))}")
    
    return result


async def run_credit_phase(app_id: str, event_store: EventStore, registry_client: ApplicantRegistryClient):
    """Run CreditAnalysisAgent."""
    print(f"\n{'='*60}")
    print(f"[CreditAnalysis] Processing {app_id}...")
    print(f"{'='*60}")

    application_uuid = _uuid_from_string(app_id)
    company_id_str = _company_for_app(app_id)
    company_uuid = _uuid_from_string(company_id_str)
    
    # Prepare state
    state = {
        "application_id": application_uuid,
        "company_id": company_uuid,
        "expected_version": 0,
        "events": [],
        "node_sequence": 0,
        "data": {},
        "analysis_result": {}
    }
    
    # Run agent
    agent = CreditAnalysisAgent(event_store, registry_client)
    result = await agent.process(state)
    
    print(f"[CreditAnalysis] ✓ Complete")
    print(f"  - Risk tier: {result.get('analysis_result', {}).get('risk_tier', 'N/A')}")
    print(f"  - Confidence: {result.get('analysis_result', {}).get('confidence', 'N/A')}")
    print(f"  - Events generated: {len(result.get('events', []))}")
    
    return result


async def run_fraud_phase(app_id: str, event_store: EventStore, registry_client: ApplicantRegistryClient):
    """Run FraudDetectionAgent."""
    print(f"\n{'='*60}")
    print(f"[FraudDetection] Processing {app_id}...")
    print(f"{'='*60}")

    application_uuid = _uuid_from_string(app_id)
    company_id_str = _company_for_app(app_id)
    company_uuid = _uuid_from_string(company_id_str)
    
    # Prepare state
    state = {
        "application_id": application_uuid,
        "company_id": company_uuid,
        "expected_version": 0,
        "events": [],
        "node_sequence": 0,
        "data": {},
        "anomalies": [],
        "fraud_score": 0.0
    }
    
    # Run agent
    agent = FraudDetectionAgent(event_store)
    result = await agent.process(state)
    
    print(f"[FraudDetection] ✓ Complete")
    print(f"  - Fraud score: {result.get('fraud_score', 0.0):.2f}")
    print(f"  - Anomalies found: {len(result.get('anomalies', []))}")
    print(f"  - Events generated: {len(result.get('events', []))}")
    
    return result


async def run_compliance_phase(app_id: str, event_store: EventStore, registry_client: ApplicantRegistryClient):
    """Run ComplianceAgent."""
    print(f"\n{'='*60}")
    print(f"[Compliance] Processing {app_id}...")
    print(f"{'='*60}")

    application_uuid = _uuid_from_string(app_id)
    company_id_str = _company_for_app(app_id)
    company_uuid = _uuid_from_string(company_id_str)

    # Best-effort: try DB, but fall back to deterministic defaults.
    try:
        company_profile = await registry_client.get_company(company_uuid)
    except Exception:
        company_profile = None
    if not company_profile:
        company_profile = {
            "company_id": str(company_uuid),
            "name": "Pipeline Demo Company",
            "jurisdiction": "CA",
            "state": "CA",
        }
    
    # Prepare state
    state = {
        "application_id": application_uuid,
        "company_id": company_uuid,
        "company_profile": company_profile,
        "expected_version": 0,
        "events": [],
        "node_sequence": 0,
        "rules_evaluated": 0,
        "hard_block": False
    }
    
    # Run agent
    agent = ComplianceAgent(event_store)
    result = await agent.process(state)
    
    print(f"[Compliance] ✓ Complete")
    print(f"  - Rules evaluated: {result.get('rules_evaluated', 0)}")
    print(f"  - Hard block: {result.get('hard_block', False)}")
    print(f"  - Events generated: {len(result.get('events', []))}")
    
    return result


async def run_decision_phase(app_id: str, event_store: EventStore, registry_client: ApplicantRegistryClient):
    """Run DecisionOrchestratorAgent."""
    print(f"\n{'='*60}")
    print(f"[DecisionOrchestrator] Processing {app_id}...")
    print(f"{'='*60}")
    
    application_uuid = _uuid_from_string(app_id)

    # Prepare state (the DecisionOrchestratorAgent reads these inputs from state).
    state = {
        "application_id": application_uuid,
        "expected_version": 0,
        "events": [],
        "node_sequence": 0,
        "credit_result": {
            "outcome": "APPROVE",
            "confidence": 0.85,
            "risk_tier": "LOW",
            "data_quality_caveats": [],
        },
        "fraud_result": {
            "fraud_score": 0.15,
            "passed": True,
            "anomalies": [],
        },
        "compliance_result": {
            "overall_verdict": "PASS",
            "checks_passed": 6,
            "hard_block": False,
        },
        "orchestrator_decision": {},
        "final_decision": {}
    }
    
    # Run agent
    agent = DecisionOrchestratorAgent(event_store)
    result = await agent.process(state)
    
    print(f"[DecisionOrchestrator] ✓ Complete")
    print(f"  - Recommendation: {result.get('final_decision', {}).get('recommendation', 'N/A')}")
    print(f"  - Confidence: {result.get('final_decision', {}).get('confidence', 'N/A')}")
    print(f"  - Events generated: {len(result.get('events', []))}")
    
    return result


async def run_full_pipeline(app_id: str, event_store: EventStore, registry_client: ApplicantRegistryClient):
    """Run all agents in sequence."""
    print(f"\n{'#'*60}")
    print(f"# FULL PIPELINE: {app_id}")
    print(f"{'#'*60}")
    
    await run_document_phase(app_id, event_store, registry_client)
    await run_credit_phase(app_id, event_store, registry_client)
    await run_fraud_phase(app_id, event_store, registry_client)
    await run_compliance_phase(app_id, event_store, registry_client)
    await run_decision_phase(app_id, event_store, registry_client)
    
    print(f"\n{'#'*60}")
    print(f"# ✓ FULL PIPELINE COMPLETE: {app_id}")
    print(f"{'#'*60}\n")


async def main():
    parser = argparse.ArgumentParser(description="Process one application through the agent pipeline")
    parser.add_argument("--app", required=True, help="Application ID (e.g., APEX-0007)")
    parser.add_argument("--phase", required=True, 
                        choices=["document", "credit", "fraud", "compliance", "decision", "full"],
                        help="Phase to run")
    parser.add_argument("--db-url", default=os.getenv("DATABASE_URL"), 
                        help="Database URL (default: from DATABASE_URL env var)")
    
    args = parser.parse_args()
    
    if not args.db_url:
        print("ERROR: DATABASE_URL not set. Provide --db-url or set DATABASE_URL environment variable.")
        sys.exit(1)
    
    # Initialize event store and registry client
    import asyncpg
    pool = await asyncpg.create_pool(args.db_url)
    event_store = EventStore(pool)
    registry_client = ApplicantRegistryClient(pool)
    
    try:
        # Run requested phase
        if args.phase == "document":
            await run_document_phase(args.app, event_store, registry_client)
        elif args.phase == "credit":
            await run_credit_phase(args.app, event_store, registry_client)
        elif args.phase == "fraud":
            await run_fraud_phase(args.app, event_store, registry_client)
        elif args.phase == "compliance":
            await run_compliance_phase(args.app, event_store, registry_client)
        elif args.phase == "decision":
            await run_decision_phase(args.app, event_store, registry_client)
        elif args.phase == "full":
            await run_full_pipeline(args.app, event_store, registry_client)
        
        print(f"\n✓ Pipeline complete for {args.app}")
        
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
