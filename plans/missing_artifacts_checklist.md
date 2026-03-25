# Missing Artifacts & Deliverables Checklist
**The Ledger Challenge — Week 5**

---

## Required Submission Structure

```
submission/
├── README.md              ✅ EXISTS (comprehensive)
├── requirements.txt       ✅ EXISTS (pinned dependencies)
├── .env.example           ✅ EXISTS
├── pytest.ini             ✅ EXISTS
├── DOMAIN_NOTES.md        ✅ EXISTS (all 6 questions answered)
├── DESIGN.md              ✅ EXISTS (all 6 sections complete)
├── DATA_GENERATION.md     ❌ MISSING
│
├── datagen/               ✅ EXISTS (complete generator code)
│
├── ledger/                ⚠️ PARTIAL (infrastructure complete, agents incomplete)
│   ├── schema/events.py   ✅ EXISTS (canonical schema)
│   ├── event_store.py     ✅ EXISTS (full implementation)
│   ├── upcasters.py       ✅ EXISTS (registry + upcasters)
│   ├── domain/aggregates/ ✅ EXISTS (all aggregates)
│   ├── projections/       ✅ EXISTS (all 3 projections + daemon)
│   ├── agents/
│   │   ├── base_agent.py         ✅ EXISTS (BaseApexAgent)
│   │   ├── credit_analysis_agent.py  ⚠️ PARTIAL (reference exists, needs completion)
│   │   └── stub_agents.py        ❌ STUBS ONLY (4 agents need full implementation)
│   ├── registry/client.py ✅ EXISTS (4 query methods)
│   └── mcp_server.py      ✅ EXISTS (8 tools + 6 resources)
│
├── tests/                 ⚠️ PARTIAL (192 tests pass, narratives missing)
│   └── test_narratives.py ❌ MISSING (5 narrative scenarios)
│
├── scripts/
│   ├── run_pipeline.py    ❌ MISSING
│   └── demo_narr05.py     ❌ MISSING
│
└── artifacts/             ⚠️ PARTIAL (1 of 6 artifacts)
    ├── test_results.txt              ❌ MISSING
    ├── narrative_test_results.txt    ❌ MISSING
    ├── occ_collision_report.txt      ❌ MISSING
    ├── projection_lag_report.txt     ❌ MISSING
    ├── api_cost_report.txt           ❌ MISSING
    └── regulatory_package_NARR05.json ✅ EXISTS
```

---

## 1. DATA_GENERATION.md ❌ CRITICAL

### What It Must Contain
Per challenge spec Section 3, this document must detail:

1. **Simulator Rules**
   - How the 29 seed applications are distributed across lifecycle states
   - Event generation logic for each agent type
   - Per-node [`AgentNodeExecuted`](ledger/schema/events.py) event simulation

2. **PDF Variants**
   - 4 income statement variants: clean (40), dense/multi-subtotal (20), missing-EBITDA (8), scanned-quality (12)
   - 6 balance sheets with intentional equity rounding discrepancy ($500–$4,500)
   - Which companies get which variant

3. **Event Counts**
   - Total events generated: 1,847
   - Breakdown by aggregate type (loan-*, docpkg-*, agent-*, credit-*, fraud-*, compliance-*)
   - Breakdown by event type

4. **API Costs Per Agent**
   - Estimated LLM API costs for generating seed data
   - Token counts per agent type
   - Model versions used in simulation

### How to Generate
```bash
# After running datagen/generate_all.py, create this document with:
python datagen/generate_all.py --applicants 80 --db-url postgresql://localhost/apex_ledger \
    --docs-dir ./documents --output-dir ./data --random-seed 42 --report-output DATA_GENERATION.md
```

### Template Structure
```markdown
# DATA_GENERATION.md

## Seed Data Summary
- Companies: 80
- Documents: 320 files (80 × 4 file types)
- Seed Events: 1,847
- Seed Applications: 29

## Application Distribution
| State | Count | Application IDs |
|-------|-------|-----------------|
| SUBMITTED | 6 | APEX-0001 – APEX-0006 |
| DOCUMENTS_UPLOADED | 5 | APEX-0007 – APEX-0011 |
| ... | ... | ... |

## PDF Variants
| Variant | Count | Purpose |
|---------|-------|---------|
| clean | 40 | Standard extraction baseline |
| dense/multi-subtotal | 20 | Tests complex table parsing |
| missing-EBITDA | 8 | Tests partial extraction (NARR-02) |
| scanned-quality | 12 | Tests OCR quality handling |

## Event Breakdown
| Aggregate Type | Event Count | Key Event Types |
|----------------|-------------|-----------------|
| loan-* | 487 | ApplicationSubmitted, DocumentUploadRequested, ... |
| docpkg-* | 312 | PackageCreated, ExtractionCompleted, ... |
| agent-* | 623 | AgentSessionStarted, AgentNodeExecuted, ... |
| credit-* | 189 | CreditRecordOpened, CreditAnalysisCompleted |
| fraud-* | 142 | FraudScreeningInitiated, FraudScreeningCompleted |
| compliance-* | 94 | ComplianceCheckInitiated, ComplianceRulePassed, ... |

## API Cost Simulation
| Agent Type | Simulated Calls | Avg Tokens/Call | Est. Cost |
|------------|-----------------|-----------------|-----------|
| DocumentProcessing | 29 | 2,100 | $1.83 |
| CreditAnalysis | 23 | 3,400 | $2.35 |
| FraudDetection | 18 | 1,800 | $0.97 |
| Compliance | 0 | 0 | $0.00 (deterministic) |
| DecisionOrchestrator | 15 | 2,900 | $1.31 |
| **Total** | **85** | **2,440 avg** | **$6.46** |
```

---

## 2. test_narratives.py ❌ CRITICAL

### File Location
`tests/test_narratives.py`

### Required Tests
```python
import pytest
from uuid import uuid4
import asyncio

@pytest.mark.asyncio
async def test_narr01_concurrent_occ_collision(event_store, registry_client):
    """Two concurrent CreditAnalysisAgent instances, exactly one succeeds per version."""
    # See narrative_scenarios_implementation_plan.md for full implementation
    pass

@pytest.mark.asyncio
async def test_narr02_missing_ebitda(event_store, registry_client):
    """DocumentProcessingAgent handles missing EBITDA gracefully."""
    pass

@pytest.mark.asyncio
async def test_narr03_crash_recovery(event_store, registry_client):
    """FraudDetectionAgent crash recovery via Gas Town pattern."""
    pass

@pytest.mark.asyncio
async def test_narr04_montana_hard_block(event_store, registry_client):
    """ComplianceAgent REG-003 hard block stops evaluation."""
    pass

@pytest.mark.asyncio
async def test_narr05_human_override(event_store, registry_client, mcp_server):
    """Full pipeline → DECLINE → human override → APPROVE."""
    pass
```

### Success Criteria
All 5 tests must pass. This is the **primary correctness gate** for the challenge.

---

## 3. scripts/run_pipeline.py ❌ CRITICAL

### Purpose
Process one application end-to-end through all 5 agents.

### Usage
```bash
python scripts/run_pipeline.py --app APEX-0007 --phase document
python scripts/run_pipeline.py --app APEX-0012 --phase credit
python scripts/run_pipeline.py --app APEX-0016 --phase fraud
python scripts/run_pipeline.py --app APEX-0019 --phase compliance
python scripts/run_pipeline.py --app APEX-0021 --phase decision
python scripts/run_pipeline.py --app APEX-NEW-01 --phase full  # All agents
```

### Implementation Template
```python
#!/usr/bin/env python3
"""
scripts/run_pipeline.py
Process one application through the agent pipeline.
"""
import asyncio
import argparse
from uuid import UUID
from ledger.event_store import EventStore
from ledger.registry.client import ApplicantRegistryClient
from ledger.agents.stub_agents import (
    DocumentProcessingAgent, FraudDetectionAgent, 
    ComplianceAgent, DecisionOrchestratorAgent
)
from ledger.agents.credit_analysis_agent import CreditAnalysisAgent

async def run_document_phase(app_id: str, event_store, registry_client):
    """Run DocumentProcessingAgent."""
    agent = DocumentProcessingAgent(event_store)
    # Load application state, prepare state dict, call agent.process()
    print(f"[DocumentProcessing] Processing {app_id}...")
    # Implementation here
    print(f"[DocumentProcessing] ✓ Complete")

async def run_credit_phase(app_id: str, event_store, registry_client):
    """Run CreditAnalysisAgent."""
    agent = CreditAnalysisAgent(event_store, registry_client)
    print(f"[CreditAnalysis] Processing {app_id}...")
    # Implementation here
    print(f"[CreditAnalysis] ✓ Complete")

async def run_fraud_phase(app_id: str, event_store, registry_client):
    """Run FraudDetectionAgent."""
    agent = FraudDetectionAgent(event_store)
    print(f"[FraudDetection] Processing {app_id}...")
    # Implementation here
    print(f"[FraudDetection] ✓ Complete")

async def run_compliance_phase(app_id: str, event_store, registry_client):
    """Run ComplianceAgent."""
    agent = ComplianceAgent(event_store)
    print(f"[Compliance] Processing {app_id}...")
    # Implementation here
    print(f"[Compliance] ✓ Complete")

async def run_decision_phase(app_id: str, event_store, registry_client):
    """Run DecisionOrchestratorAgent."""
    agent = DecisionOrchestratorAgent(event_store)
    print(f"[DecisionOrchestrator] Processing {app_id}...")
    # Implementation here
    print(f"[DecisionOrchestrator] ✓ Complete")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--app", required=True, help="Application ID (e.g., APEX-0007)")
    parser.add_argument("--phase", required=True, 
                        choices=["document", "credit", "fraud", "compliance", "decision", "full"])
    args = parser.parse_args()
    
    # Initialize event store and registry client
    # Run requested phase(s)
    
    print(f"\n✓ Pipeline complete for {args.app}")

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 4. scripts/demo_narr05.py ❌ CRITICAL

### Purpose
Required demo script for NARR-05. Must run in **< 90 seconds**.

### What It Must Do
1. Run full pipeline on APEX-NARR05 (COMP-068)
2. Show [`DecisionGenerated`](ledger/schema/events.py) with DECLINE recommendation
3. Simulate human override via MCP tool
4. Show [`ApplicationApproved`](ledger/schema/events.py) with conditions
5. Generate `artifacts/regulatory_package_NARR05.json`
6. Verify package independently

### Implementation Template
```python
#!/usr/bin/env python3
"""
scripts/demo_narr05.py
Required demo for NARR-05 — must run in < 90 seconds.
"""
import asyncio
import time
from ledger.regulatory.package import generate_regulatory_package, verify_regulatory_package

async def main():
    start = time.time()
    print("=" * 60)
    print("NARR-05 DEMO: Human Override Against Agent Recommendation")
    print("=" * 60)
    
    app_id = "APEX-NARR05"
    company_id = "COMP-068"  # Retail, 15-year customer, DECLINING -8% YoY
    
    # 1. Run full pipeline
    print("\n[1/6] Running DocumentProcessingAgent...")
    # Implementation
    
    print("[2/6] Running CreditAnalysisAgent...")
    # Implementation
    
    print("[3/6] Running FraudDetectionAgent...")
    # Implementation
    
    print("[4/6] Running ComplianceAgent...")
    # Implementation
    
    print("[5/6] Running DecisionOrchestratorAgent...")
    # Implementation
    # Show: DecisionGenerated with recommendation=DECLINE
    
    print("\n[6/6] Human Override...")
    # Call MCP tool: record_human_review_completed
    # Show: ApplicationApproved with approved_amount=750000, 2 conditions
    
    print("\n[Bonus] Generating regulatory package...")
    package = await generate_regulatory_package(pool, event_store, app_id)
    
    with open("artifacts/regulatory_package_NARR05.json", "w") as f:
        json.dump(package, f, indent=2)
    
    print("✓ Package written to artifacts/regulatory_package_NARR05.json")
    
    print("\n[Verification] Verifying package independently...")
    result = verify_regulatory_package(package)
    print(f"  Package hash valid: {result['package_hash_valid']}")
    print(f"  Chain valid: {result['chain_valid']}")
    print(f"  Overall valid: {result['overall_valid']}")
    
    elapsed = time.time() - start
    print(f"\n✓ Demo complete in {elapsed:.1f}s (SLO: < 90s)")
    
    if elapsed > 90:
        print("⚠️  WARNING: Exceeded 90-second SLO")
        exit(1)

if __name__ == "__main__":
    asyncio.run(main())
```

---

## 5. artifacts/test_results.txt ❌

### How to Generate
```bash
pytest tests/ -v --tb=short > artifacts/test_results.txt 2>&1
```

### What It Must Show
- Total tests run (should be 197+ after adding 5 narrative tests)
- All tests passing (or clearly documented failures)
- Execution time
- Coverage summary (optional but recommended)

---

## 6. artifacts/narrative_test_results.txt ❌

### How to Generate
```bash
pytest tests/test_narratives.py -v --tb=long > artifacts/narrative_test_results.txt 2>&1
```

### What It Must Show
```
tests/test_narratives.py::test_narr01_concurrent_occ_collision PASSED
tests/test_narratives.py::test_narr02_missing_ebitda PASSED
tests/test_narratives.py::test_narr03_crash_recovery PASSED
tests/test_narratives.py::test_narr04_montana_hard_block PASSED
tests/test_narratives.py::test_narr05_human_override PASSED

======================== 5 passed in 12.34s ========================
```

---

## 7. artifacts/occ_collision_report.txt ❌

### Purpose
Load generator results showing OCC conflict resolution under concurrent load.

### How to Generate
```bash
# Create load_gen/run_concurrent.py if it doesn't exist
python load_gen/run_concurrent.py --applications 15 --concurrency 6 > artifacts/occ_collision_report.txt
```

### What It Must Show
```
Load Generator Report
=====================
Applications: 15
Concurrency: 6 workers
Duration: 23.4s

OCC Conflicts Detected: 27
OCC Conflicts Resolved: 27 (100%)
Unresolved Conflicts: 0

Retry Distribution:
  1 retry: 18 conflicts
  2 retries: 7 conflicts
  3 retries: 2 conflicts
  4+ retries: 0 conflicts

Max Retries Per Conflict: 3
Avg Retry Delay: 28ms

✓ All conflicts resolved within retry budget
```

---

## 8. artifacts/projection_lag_report.txt ❌

### Purpose
Projection lag metrics under load, demonstrating < 500ms SLO.

### How to Generate
```bash
# Run load test with lag monitoring
python tests/test_projections.py::test_projection_daemon_lag_slo_under_load -v -s > artifacts/projection_lag_report.txt
```

### What It Must Show
```
Projection Lag Report
=====================
Test: 100 events appended, 3 projections updated

ApplicationSummary:
  Min lag: 12ms
  Avg lag: 45ms
  P95 lag: 89ms
  P99 lag: 142ms
  Max lag: 178ms
  ✓ SLO met (< 500ms)

AgentPerformanceLedger:
  Min lag: 8ms
  Avg lag: 38ms
  P95 lag: 76ms
  P99 lag: 124ms
  Max lag: 156ms
  ✓ SLO met (< 500ms)

ComplianceAuditView:
  Min lag: 15ms
  Avg lag: 52ms
  P95 lag: 98ms
  P99 lag: 167ms
  Max lag: 203ms
  ✓ SLO met (< 500ms)

Overall: ✓ All projections within 500ms SLO
```

---

## 9. artifacts/api_cost_report.txt ❌ CRITICAL

### Purpose
LLM API costs per agent, per application. **Costs over $50 total signal inefficient prompt design.**

### How to Generate
```bash
# After running full pipeline on all 34 applications (29 seed + 5 narrative)
python scripts/generate_cost_report.py > artifacts/api_cost_report.txt
```

### What It Must Show
```
API Cost Report
===============
Applications Processed: 34 (29 seed + 5 narrative)
Total API Cost: $22.40
Average Cost Per Application: $0.66
Cost Range: $0.18 – $1.40

Cost by Agent:
  DocumentProcessing:     $2.10  (34 calls, avg 1,850 tokens)
  CreditAnalysis:         $8.80  (28 calls, avg 3,200 tokens)
  FraudDetection:         $4.20  (24 calls, avg 1,750 tokens)
  Compliance:             $0.00  (0 calls — deterministic Python)
  DecisionOrchestrator:   $7.30  (20 calls, avg 2,900 tokens)

Most Expensive Single Call:
  APEX-0049 CreditAnalysis: $0.82 (5,200 input tokens)
  Reason: Very long historical financial context (10 years)

Cost Distribution:
  < $0.50: 18 applications
  $0.50 – $1.00: 12 applications
  > $1.00: 4 applications

✓ Total cost within budget (< $50)
```

### Implementation
Track costs using the Week 5 Sentinel [`CostAttributor`](ledger/agents/base_agent.py):
- Every LLM call tagged with `agent_type` and `workflow_id`
- Token counts from `response.usage`
- Cost calculated from model pricing (e.g., Claude Sonnet: $3/1M input, $15/1M output)

---

## 10. artifacts/regulatory_package_NARR05.json ✅ EXISTS

**Status:** Already generated (Phase 6 bonus feature implemented)

### Verification
```bash
python tests/verify_package.py artifacts/regulatory_package_NARR05.json
```

Should output:
```
✓ Package hash valid
✓ Chain valid
✓ Event count matches
✓ Overall valid
```

---

## Summary Checklist

### Documentation
- [x] README.md
- [x] DESIGN.md (all 6 sections)
- [x] DOMAIN_NOTES.md (all 6 questions)
- [ ] DATA_GENERATION.md

### Code
- [x] Event store infrastructure
- [x] Projections + daemon
- [x] MCP server
- [ ] 5 LangGraph agents (4 stubs, 1 partial)
- [ ] 5 narrative tests

### Scripts
- [ ] scripts/run_pipeline.py
- [ ] scripts/demo_narr05.py
- [x] scripts/run_whatif.py (bonus)

### Artifacts
- [ ] test_results.txt
- [ ] narrative_test_results.txt
- [ ] occ_collision_report.txt
- [ ] projection_lag_report.txt
- [ ] api_cost_report.txt
- [x] regulatory_package_NARR05.json

### Data
- [ ] Run datagen/generate_all.py
- [ ] 80 companies in database
- [ ] 160+ documents in filesystem
- [ ] 1,847 seed events

---

## Estimated Effort

| Item | Effort | Priority |
|------|--------|----------|
| Run data generator | 30 min | P0 (blocks everything) |
| Implement 5 agents | 3-4 days | P0 (core deliverable) |
| Implement 5 narrative tests | 1-2 days | P0 (correctness gate) |
| Create run_pipeline.py | 2 hours | P1 (required script) |
| Create demo_narr05.py | 2 hours | P1 (required demo) |
| Generate all artifacts | 1 day | P1 (submission requirement) |
| Write DATA_GENERATION.md | 1 hour | P2 (documentation) |

**Total:** 5-7 days of focused work
