# MASTER IMPLEMENTATION PLAN
**The Ledger Challenge — Week 5**  
**Comprehensive 7-Day Execution Plan**

---

## Executive Summary

**Current State:** Production-grade event sourcing infrastructure (192 tests passing) but **missing core challenge deliverables**

**Gap:** 5 LangGraph agents (stubs only) + 5 narrative test scenarios (don't exist) + required artifacts

**Estimated Effort:** 5-7 days of focused implementation

**Critical Path:** Data generation → Agents → Narrative tests → Artifacts → Submission

---

## 🎯 Success Criteria

To achieve a **Score 4/5** (passing grade):

1. ✅ All 5 LangGraph agents fully implemented with real LLM calls
2. ✅ All 5 narrative test scenarios passing
3. ✅ Data generator run successfully (80 companies, 1,847 events, 160+ documents)
4. ✅ All required scripts created ([`run_pipeline.py`](scripts/run_pipeline.py), [`demo_narr05.py`](scripts/demo_narr05.py))
5. ✅ All required artifacts generated (6 total)
6. ✅ [`DESIGN.md`](DESIGN.md) complete (already done ✓)
7. ✅ [`DOMAIN_NOTES.md`](DOMAIN_NOTES.md) complete (already done ✓)

To achieve **Score 5/5** (Phase 6 bonus already implemented ✓):
- WhatIfProjector ✓
- Regulatory package generation ✓
- Independent verification ✓

---

## 📅 7-Day Implementation Schedule

### **DAY 1: Data Generation & Environment Setup**

#### Morning (4 hours)
**Goal:** Get seed data in place

1. **Verify Database Setup** (30 min)
   ```bash
   # Ensure PostgreSQL is running
   psql -d apex_ledger -c "SELECT version();"
   
   # Verify schema exists
   psql -d apex_ledger -f src/schema.sql
   
   # Check applicant_registry schema
   psql -d apex_ledger -c "\dn"
   ```

2. **Run Data Generator** (2 hours)
   ```bash
   # Install any missing dependencies
   pip install reportlab faker openpyxl
   
   # Run generator
   python datagen/generate_all.py \
       --applicants 80 \
       --db-url postgresql://localhost/apex_ledger \
       --docs-dir ./documents \
       --output-dir ./data \
       --random-seed 42
   
   # Expected output: 1,847 events, 80 companies, 320 files
   ```

3. **Verify Seed Data** (1 hour)
   ```bash
   # Check companies
   psql -d apex_ledger -c "SELECT COUNT(*) FROM applicant_registry.companies;"
   # Expected: 80
   
   # Check events
   psql -d apex_ledger -c "SELECT COUNT(*) FROM events;"
   # Expected: 1,847
   
   # Check documents
   ls -R documents/ | grep -c "\.pdf\|\.xlsx\|\.csv"
   # Expected: 320+
   
   # Check application distribution
   psql -d apex_ledger -c "
       SELECT state, COUNT(*) 
       FROM (SELECT DISTINCT ON (aggregate_id) aggregate_id, 
                    payload->>'state' as state 
             FROM events 
             WHERE aggregate_type = 'LoanApplication' 
             ORDER BY aggregate_id, stream_position DESC) sub 
       GROUP BY state;"
   ```

4. **Create DATA_GENERATION.md** (30 min)
   - Document PDF variants distribution
   - Event count breakdown by aggregate type
   - Application state distribution
   - Estimated API costs for seed data

#### Afternoon (4 hours)
**Goal:** Week 3 pipeline integration prep

5. **Locate/Import Week 3 Pipeline** (2 hours)
   - If Week 3 code exists elsewhere, copy to project
   - If not, create minimal extraction stub that returns [`FinancialFacts`](ledger/schema/events.py)
   - Test extraction on one PDF: `documents/COMP-001/income_statement_2024_clean.pdf`

6. **Test Extraction Pipeline** (2 hours)
   ```python
   # Test script
   from document_refinery.pipeline import extract_financial_facts
   
   result = extract_financial_facts("documents/COMP-001/income_statement_2024_clean.pdf")
   assert result.total_revenue is not None
   assert result.net_income is not None
   ```

**Day 1 Deliverables:**
- ✅ 80 companies in database
- ✅ 1,847 seed events
- ✅ 320+ documents in filesystem
- ✅ [`DATA_GENERATION.md`](DATA_GENERATION.md) created
- ✅ Week 3 pipeline tested

---

### **DAY 2: DocumentProcessingAgent**

#### Morning (4 hours)
**Goal:** Implement full [`DocumentProcessingAgent`](ledger/agents/stub_agents.py)

1. **Build LangGraph StateGraph** (2 hours)
   ```python
   # ledger/agents/stub_agents.py
   from langgraph.graph import StateGraph
   
   class DocumentProcessingAgent(BaseApexAgent):
       def build_graph(self):
           graph = StateGraph(BaseState)
           
           graph.add_node("validate_inputs", self._node_validate_inputs)
           graph.add_node("validate_document_formats", self._node_validate_formats)
           graph.add_node("extract_income_statement", self._node_extract_income)
           graph.add_node("extract_balance_sheet", self._node_extract_balance)
           graph.add_node("assess_quality", self._node_assess_quality)
           graph.add_node("write_output", self._node_write_output)
           
           graph.set_entry_point("validate_inputs")
           graph.add_edge("validate_inputs", "validate_document_formats")
           graph.add_edge("validate_document_formats", "extract_income_statement")
           graph.add_edge("extract_income_statement", "extract_balance_sheet")
           graph.add_edge("extract_balance_sheet", "assess_quality")
           graph.add_edge("assess_quality", "write_output")
           graph.set_finish_point("write_output")
           
           return graph.compile()
   ```

2. **Implement Extraction Nodes** (2 hours)
   ```python
   async def _node_extract_income(self, state: BaseState):
       t0 = time.time()
       
       # Call Week 3 pipeline
       from document_refinery.pipeline import extract_financial_facts
       
       income_path = state["document_paths"]["income_statement"]
       facts = extract_financial_facts(income_path)
       
       # Handle missing fields
       field_confidence = {}
       extraction_notes = []
       
       for field in ["total_revenue", "net_income", "ebitda", "gross_profit"]:
           if getattr(facts, field) is None:
               field_confidence[field] = 0.0
               extraction_notes.append(f"{field} not found in document")
           else:
               field_confidence[field] = 0.95
       
       state["extracted_facts"]["income"] = facts
       state["field_confidence"] = field_confidence
       state["extraction_notes"] = extraction_notes
       
       # Record node execution
       await self._record_node_execution(
           node_name="extract_income_statement",
           input_keys=["document_paths"],
           output_keys=["extracted_facts", "field_confidence"],
           duration_ms=int((time.time()-t0)*1000)
       )
       
       return state
   ```

#### Afternoon (4 hours)
**Goal:** Quality assessment LLM call

3. **Implement Quality Assessment** (3 hours)
   ```python
   async def _node_assess_quality(self, state: BaseState):
       t0 = time.time()
       
       # Prepare LLM prompt
       system_prompt = """You are a financial document quality analyst.
       Check ONLY:
       1. Internal consistency (Gross Profit = Revenue - COGS)
       2. Implausible values (margins > 80%, negative equity)
       3. Critical missing fields
       
       Return JSON: {
           "overall_confidence": float,
           "is_coherent": bool,
           "anomalies": [str],
           "critical_missing_fields": [str],
           "reextraction_recommended": bool,
           "auditor_notes": str
       }
       
       DO NOT make credit decisions."""
       
       user_message = f"""
       Income Statement Facts: {state['extracted_facts']['income']}
       Balance Sheet Facts: {state['extracted_facts']['balance']}
       Field Confidence: {state['field_confidence']}
       Extraction Notes: {state['extraction_notes']}
       """
       
       # Call LLM
       response = await self._call_llm(system_prompt, user_message)
       quality_result = json.loads(response.content)
       
       # Record LLM call
       await self._record_node_execution(
           node_name="assess_quality",
           input_keys=["extracted_facts", "field_confidence"],
           output_keys=["quality_assessment"],
           duration_ms=int((time.time()-t0)*1000),
           llm_tokens_input=response.usage.input_tokens,
           llm_tokens_output=response.usage.output_tokens,
           llm_cost_usd=self._calculate_cost(response.usage)
       )
       
       state["quality_assessment"] = quality_result
       return state
   ```

4. **Test on APEX-0007** (1 hour)
   ```bash
   python scripts/run_pipeline.py --app APEX-0007 --phase document
   
   # Verify events in docpkg-APEX-0007 stream
   psql -d apex_ledger -c "
       SELECT event_type, payload 
       FROM events 
       WHERE stream_id = 'docpkg-APEX-0007' 
       ORDER BY stream_position;"
   ```

**Day 2 Deliverables:**
- ✅ [`DocumentProcessingAgent`](ledger/agents/stub_agents.py) fully implemented
- ✅ Week 3 pipeline integrated
- ✅ Quality assessment LLM call working
- ✅ Tested on APEX-0007

---

### **DAY 3: CreditAnalysisAgent & FraudDetectionAgent**

#### Morning (4 hours)
**Goal:** Complete [`CreditAnalysisAgent`](ledger/agents/credit_analysis_agent.py)

1. **Complete Reference Implementation** (3 hours)
   - Reference already exists, needs full LangGraph integration
   - Implement all 6 nodes with proper [`_record_node_execution()`](ledger/agents/base_agent.py)
   - Add policy constraints (max loan-to-revenue 35%, prior default → HIGH risk)
   - Test on APEX-0012

2. **Test Credit Agent** (1 hour)
   ```bash
   python scripts/run_pipeline.py --app APEX-0012 --phase credit
   
   # Verify CreditAnalysisCompleted event
   psql -d apex_ledger -c "
       SELECT payload 
       FROM events 
       WHERE stream_id = 'credit-APEX-0012' 
       AND event_type = 'CreditAnalysisCompleted';"
   ```

#### Afternoon (4 hours)
**Goal:** Implement [`FraudDetectionAgent`](ledger/agents/stub_agents.py)

3. **Build Fraud Detection Graph** (2 hours)
   ```python
   class FraudDetectionAgent(BaseApexAgent):
       def build_graph(self):
           graph = StateGraph(BaseState)
           
           graph.add_node("validate_inputs", self._node_validate_inputs)
           graph.add_node("load_facts", self._node_load_facts)
           graph.add_node("cross_reference_registry", self._node_cross_reference)
           graph.add_node("analyze_fraud_patterns", self._node_analyze_patterns)
           graph.add_node("write_output", self._node_write_output)
           
           # ... edges ...
           
           return graph.compile()
   ```

4. **Implement Cross-Reference Logic** (2 hours)
   ```python
   async def _node_cross_reference(self, state: BaseState):
       # Load 3yr financial history from registry
       history = await self.registry.get_financial_history(state["company_id"])
       
       # Compute deltas: current vs prior year
       current_revenue = state["extracted_facts"]["total_revenue"]
       prior_revenue = history[0]["revenue"] if history else current_revenue
       
       revenue_delta_pct = (current_revenue - prior_revenue) / prior_revenue * 100
       
       # Flag anomalies
       anomalies = []
       if abs(revenue_delta_pct) > 50:
           anomalies.append({
               "type": "REVENUE_SPIKE",
               "severity": 0.7,
               "description": f"Revenue changed {revenue_delta_pct:.1f}% YoY"
           })
       
       state["anomalies"] = anomalies
       state["fraud_score"] = sum(a["severity"] for a in anomalies)
       
       await self._record_node_execution(...)
       return state
   ```

**Day 3 Deliverables:**
- ✅ [`CreditAnalysisAgent`](ledger/agents/credit_analysis_agent.py) complete
- ✅ [`FraudDetectionAgent`](ledger/agents/stub_agents.py) complete
- ✅ Both tested on seed applications

---

### **DAY 4: ComplianceAgent**

#### Full Day (8 hours)
**Goal:** Implement all 6 compliance rules with hard block logic

1. **Build Compliance Graph with Conditional Edges** (3 hours)
   ```python
   class ComplianceAgent(BaseApexAgent):
       def build_graph(self):
           graph = StateGraph(BaseState)
           
           graph.add_node("validate_inputs", self._node_validate_inputs)
           graph.add_node("evaluate_reg001", self._node_evaluate_reg001)
           graph.add_node("evaluate_reg002", self._node_evaluate_reg002)
           graph.add_node("evaluate_reg003", self._node_evaluate_reg003)
           graph.add_node("evaluate_reg004", self._node_evaluate_reg004)
           graph.add_node("evaluate_reg005", self._node_evaluate_reg005)
           graph.add_node("evaluate_reg006", self._node_evaluate_reg006)
           graph.add_node("write_output", self._node_write_output)
           
           graph.set_entry_point("validate_inputs")
           graph.add_edge("validate_inputs", "evaluate_reg001")
           
           # Conditional edges — stop on hard block
           graph.add_conditional_edges(
               "evaluate_reg001",
               lambda s: "hard_block" if s.get("hard_block") else "evaluate_reg002",
               {"evaluate_reg002": "evaluate_reg002", "hard_block": "write_output"}
           )
           
           graph.add_conditional_edges(
               "evaluate_reg002",
               lambda s: "hard_block" if s.get("hard_block") else "evaluate_reg003",
               {"evaluate_reg003": "evaluate_reg003", "hard_block": "write_output"}
           )
           
           # ... repeat for all rules ...
           
           return graph.compile()
   ```

2. **Implement Each Rule** (4 hours)
   ```python
   async def _node_evaluate_reg001(self, state: BaseState):
       """REG-001: Bank Secrecy Act Check (remediable)"""
       t0 = time.time()
       
       flags = await self.registry.get_compliance_flags(state["company_id"])
       has_aml_watch = any(f["flag_type"] == "AML_WATCH" and f["is_active"] 
                           for f in flags)
       
       passes = not has_aml_watch
       
       await self._append_compliance_result(
           state, "REG-001", "Bank Secrecy Act Check", passes, is_hard=False
       )
       
       await self._record_node_execution(...)
       
       return {**state, "rules_evaluated": state.get("rules_evaluated", 0) + 1}
   
   async def _node_evaluate_reg003(self, state: BaseState):
       """REG-003: Jurisdiction Lending Eligibility (HARD BLOCK)"""
       t0 = time.time()
       
       company = state["company_profile"]
       passes = company["jurisdiction"] != "MT"  # Montana excluded
       
       await self._append_compliance_result(
           state, "REG-003", "Jurisdiction Check", passes, is_hard=True
       )
       
       await self._record_node_execution(...)
       
       return {
           **state, 
           "hard_block": not passes,
           "rules_evaluated": state.get("rules_evaluated", 0) + 1
       }
   ```

3. **Test on Montana Company** (1 hour)
   ```bash
   # Find Montana company
   psql -d apex_ledger -c "
       SELECT company_id, name 
       FROM applicant_registry.companies 
       WHERE jurisdiction = 'MT';"
   
   # Run compliance agent
   python scripts/run_pipeline.py --app APEX-NARR04 --phase compliance
   
   # Verify exactly 3 rule events (REG-001, REG-002, REG-003)
   ```

**Day 4 Deliverables:**
- ✅ [`ComplianceAgent`](ledger/agents/stub_agents.py) with all 6 rules
- ✅ Hard block logic working
- ✅ Tested on Montana company (NARR-04 scenario)

---

### **DAY 5: DecisionOrchestratorAgent & Narrative Tests Setup**

#### Morning (4 hours)
**Goal:** Implement [`DecisionOrchestratorAgent`](ledger/agents/stub_agents.py)

1. **Build Orchestrator Graph** (2 hours)
   ```python
   class DecisionOrchestratorAgent(BaseApexAgent):
       def build_graph(self):
           graph = StateGraph(BaseState)
           
           graph.add_node("validate_inputs", self._node_validate_inputs)
           graph.add_node("load_credit", self._node_load_credit)
           graph.add_node("load_fraud", self._node_load_fraud)
           graph.add_node("load_compliance", self._node_load_compliance)
           graph.add_node("synthesize_decision", self._node_synthesize)
           graph.add_node("apply_hard_constraints", self._node_apply_constraints)
           graph.add_node("write_output", self._node_write_output)
           
           # ... edges ...
           
           return graph.compile()
   ```

2. **Implement Synthesis LLM Call** (2 hours)
   ```python
   async def _node_synthesize(self, state: BaseState):
       system_prompt = """You are a loan decision synthesizer.
       Given credit analysis, fraud screening, and compliance results,
       produce an executive summary and initial recommendation.
       
       Return JSON: {
           "executive_summary": str (3-5 sentences),
           "key_risks": [str],
           "initial_recommendation": "APPROVE" | "DECLINE" | "REFER",
           "confidence": float
       }"""
       
       user_message = f"""
       Credit Analysis: {state['credit_result']}
       Fraud Screening: {state['fraud_result']}
       Compliance: {state['compliance_result']}
       """
       
       response = await self._call_llm(system_prompt, user_message)
       decision = json.loads(response.content)
       
       state["orchestrator_decision"] = decision
       
       await self._record_node_execution(...)
       return state
   
   async def _node_apply_constraints(self, state: BaseState):
       """Hard constraints override LLM recommendation"""
       decision = state["orchestrator_decision"]
       
       # Compliance BLOCKED → force DECLINE
       if state["compliance_result"]["overall_verdict"] == "BLOCKED":
           decision["initial_recommendation"] = "DECLINE"
       
       # Confidence < 0.60 → force REFER
       if decision["confidence"] < 0.60:
           decision["initial_recommendation"] = "REFER"
       
       # Fraud score > 0.60 → force REFER
       if state["fraud_result"]["fraud_score"] > 0.60:
           decision["initial_recommendation"] = "REFER"
       
       state["final_decision"] = decision
       return state
   ```

#### Afternoon (4 hours)
**Goal:** Create narrative test file structure

3. **Create tests/test_narratives.py** (2 hours)
   ```python
   # tests/test_narratives.py
   import pytest
   import asyncio
   from uuid import uuid4
   
   @pytest.mark.asyncio
   async def test_narr01_concurrent_occ_collision(event_store, registry_client):
       """Two concurrent CreditAnalysisAgent instances."""
       # Implementation from narrative_scenarios_implementation_plan.md
       pass
   
   @pytest.mark.asyncio
   async def test_narr02_missing_ebitda(event_store, registry_client):
       """DocumentProcessingAgent handles missing EBITDA."""
       pass
   
   @pytest.mark.asyncio
   async def test_narr03_crash_recovery(event_store, registry_client):
       """FraudDetectionAgent crash recovery."""
       pass
   
   @pytest.mark.asyncio
   async def test_narr04_montana_hard_block(event_store, registry_client):
       """ComplianceAgent REG-003 hard block."""
       pass
   
   @pytest.mark.asyncio
   async def test_narr05_human_override(event_store, registry_client, mcp_server):
       """Full pipeline → DECLINE → human override → APPROVE."""
       pass
   ```

4. **Implement NARR-04 (Simplest)** (2 hours)
   - Montana hard block test
   - Should pass immediately if [`ComplianceAgent`](ledger/agents/stub_agents.py) is correct

**Day 5 Deliverables:**
- ✅ [`DecisionOrchestratorAgent`](ledger/agents/stub_agents.py) complete
- ✅ All 5 agents implemented
- ✅ [`tests/test_narratives.py`](tests/test_narratives.py) created
- ✅ NARR-04 passing

---

### **DAY 6: Narrative Tests Implementation**

#### Morning (4 hours)
**Goal:** Implement NARR-01, NARR-02, NARR-03

1. **NARR-01: OCC Collision** (1.5 hours)
   - Two concurrent [`CreditAnalysisAgent`](ledger/agents/credit_analysis_agent.py) instances
   - Verify exactly 2 [`CreditAnalysisCompleted`](ledger/schema/events.py) events
   - Both agents complete without raising

2. **NARR-02: Missing EBITDA** (1.5 hours)
   - Use COMP-044 (healthcare, missing_ebitda variant)
   - Verify `ebitda = None`, confidence ≤ 0.75
   - Application continues (not blocked)

3. **NARR-03: Crash Recovery** (1 hour)
   - Simulate crash after `load_facts` node
   - Verify recovery session skips completed work
   - Exactly 1 [`FraudScreeningCompleted`](ledger/schema/events.py)

#### Afternoon (4 hours)
**Goal:** Implement NARR-05 and debug

4. **NARR-05: Human Override** (3 hours)
   - Full pipeline on COMP-068 (retail, DECLINING)
   - Verify [`DecisionGenerated`](ledger/schema/events.py) with DECLINE
   - MCP tool: `record_human_review_completed`
   - Verify [`ApplicationApproved`](ledger/schema/events.py) with conditions

5. **Debug Failing Tests** (1 hour)
   ```bash
   pytest tests/test_narratives.py -v --tb=long
   
   # Fix issues until all 5 pass
   ```

**Day 6 Deliverables:**
- ✅ All 5 narrative tests implemented
- ✅ All 5 narrative tests passing
- ✅ [`narrative_test_results.txt`](artifacts/narrative_test_results.txt) generated

---

### **DAY 7: Scripts, Artifacts & Final Validation**

#### Morning (4 hours)
**Goal:** Create required scripts

1. **scripts/run_pipeline.py** (2 hours)
   - Implement all phase handlers (document, credit, fraud, compliance, decision, full)
   - Test on multiple applications
   ```bash
   python scripts/run_pipeline.py --app APEX-0007 --phase full
   python scripts/run_pipeline.py --app APEX-0012 --phase credit
   ```

2. **scripts/demo_narr05.py** (2 hours)
   - Full pipeline on APEX-NARR05
   - Generate regulatory package
   - Verify package
   - Must run < 90 seconds
   ```bash
   time python scripts/demo_narr05.py
   # Expected: < 90s
   ```

#### Afternoon (4 hours)
**Goal:** Generate all artifacts

3. **Generate Artifacts** (3 hours)
   ```bash
   # 1. test_results.txt
   pytest tests/ -v --tb=short > artifacts/test_results.txt 2>&1
   
   # 2. narrative_test_results.txt
   pytest tests/test_narratives.py -v --tb=long > artifacts/narrative_test_results.txt 2>&1
   
   # 3. occ_collision_report.txt
   # Create load_gen/run_concurrent.py if needed
   python load_gen/run_concurrent.py --applications 15 --concurrency 6 > artifacts/occ_collision_report.txt
   
   # 4. projection_lag_report.txt
   pytest tests/test_projections.py::test_projection_daemon_lag_slo_under_load -v -s > artifacts/projection_lag_report.txt
   
   # 5. api_cost_report.txt
   # Create scripts/generate_cost_report.py
   python scripts/generate_cost_report.py > artifacts/api_cost_report.txt
   
   # 6. regulatory_package_NARR05.json (already exists from demo)
   python tests/verify_package.py artifacts/regulatory_package_NARR05.json
   ```

4. **Final Validation** (1 hour)
   ```bash
   # Run full test suite
   pytest tests/ -v
   # Expected: 197+ tests passing (192 existing + 5 narrative)
   
   # Verify all artifacts exist
   ls -lh artifacts/
   # Expected: 6 files
   
   # Verify all documentation complete
   ls -lh *.md
   # Expected: README.md, DESIGN.md, DOMAIN_NOTES.md, DATA_GENERATION.md
   ```

**Day 7 Deliverables:**
- ✅ [`scripts/run_pipeline.py`](scripts/run_pipeline.py)
- ✅ [`scripts/demo_narr05.py`](scripts/demo_narr05.py)
- ✅ All 6 artifacts in [`artifacts/`](artifacts/)
- ✅ Full test suite passing (197+ tests)
- ✅ Ready for submission

---

## 🚨 Risk Mitigation

### High-Risk Items
1. **Week 3 Pipeline Integration** — If Week 3 code doesn't exist, create minimal stub
2. **LLM API Costs** — Monitor costs, stay under $50 total
3. **NARR-03 Crash Recovery** — Complex Gas Town logic, allocate extra debug time
4. **NARR-05 Full Pipeline** — End-to-end integration, test early

### Contingency Plans
- **If Week 3 missing:** Create stub that returns hardcoded [`FinancialFacts`](ledger/schema/events.py)
- **If LLM costs high:** Reduce prompt size, use cheaper model for quality assessment
- **If narrative tests fail:** Focus on NARR-01, NARR-02, NARR-04 first (simpler)
- **If time runs short:** Prioritize narrative tests over artifacts (tests are the gate)

---

## 📊 Daily Progress Tracking

| Day | Focus | Key Deliverable | Gate Test |
|-----|-------|-----------------|-----------|
| 1 | Data generation | 1,847 events in DB | `SELECT COUNT(*) FROM events;` → 1847 |
| 2 | DocumentProcessingAgent | Agent complete | `pytest tests/test_narratives.py::test_narr02` |
| 3 | Credit + Fraud agents | Both agents complete | `python scripts/run_pipeline.py --app APEX-0012 --phase credit` |
| 4 | ComplianceAgent | All 6 rules | `pytest tests/test_narratives.py::test_narr04` |
| 5 | DecisionOrchestrator | All 5 agents done | `python scripts/run_pipeline.py --app APEX-0021 --phase full` |
| 6 | Narrative tests | All 5 passing | `pytest tests/test_narratives.py -v` → 5 passed |
| 7 | Scripts + artifacts | Submission ready | `ls artifacts/` → 6 files |

---

## ✅ Final Submission Checklist

### Code
- [ ] All 5 agents fully implemented with real LLM calls
- [ ] All agents use [`_record_node_execution()`](ledger/agents/base_agent.py) per node
- [ ] OCC retry logic in all `write_output` methods
- [ ] Gas Town crash recovery working

### Tests
- [ ] All 5 narrative tests passing
- [ ] Full test suite: 197+ tests passing
- [ ] No skipped tests (except DB tests if using in-memory)

### Scripts
- [ ] [`scripts/run_pipeline.py`](scripts/run_pipeline.py) works for all phases
- [ ] [`scripts/demo_narr05.py`](scripts/demo_narr05.py) runs < 90 seconds

### Artifacts
- [ ] [`artifacts/test_results.txt`](artifacts/test_results.txt)
- [ ] [`artifacts/narrative_test_results.txt`](artifacts/narrative_test_results.txt)
- [ ] [`artifacts/occ_collision_report.txt`](artifacts/occ_collision_report.txt)
- [ ] [`artifacts/projection_lag_report.txt`](artifacts/projection_lag_report.txt)
- [ ] [`artifacts/api_cost_report.txt`](artifacts/api_cost_report.txt) (< $50 total)
- [ ] [`artifacts/regulatory_package_NARR05.json`](artifacts/regulatory_package_NARR05.json)

### Documentation
- [ ] [`README.md`](README.md) — install & run instructions
- [ ] [`DESIGN.md`