# Apex Financial Services — Agentic Event Store
## TRP1 Week 5: The Ledger

Production-grade Event Sourcing infrastructure for AI agent loan processing.

---

## Prerequisites

- Python 3.11+
- PostgreSQL 16+
- `uv` or `pip` for package management

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Create databases

```bash
# Production
createdb apex_financial

# Test
createdb apex_financial_test
```

### 3. Run migrations

```bash
# Production
psql -d apex_financial -f src/schema.sql

# Test
psql -d apex_financial_test -f src/schema.sql
```

### 4. Configure environment

```bash
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/apex_financial"
export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/apex_financial_test"
```

---

## Running the MCP Server

```bash
python -m src.mcp.server
```

Or with explicit database URL:

```bash
DATABASE_URL="postgresql://user:pass@host:5432/apex_financial" python -m src.mcp.server
```

---

## Running Tests

```bash
# All tests (single run, no watch mode)
pytest tests/ -v

# Specific test suites
pytest tests/test_concurrency.py -v      # OCC double-decision test
pytest tests/test_upcasting.py -v        # Immutability verification
pytest tests/test_projections.py -v      # Lag SLO & rebuild
pytest tests/test_gas_town.py -v         # Crash recovery
pytest tests/test_mcp_lifecycle.py -v    # End-to-end lifecycle
```

---

## Project Structure

```
src/
├── schema.sql                    # PostgreSQL schema (events, streams, projections)
├── event_store.py                # Core EventStore class (append, load, OCC)
├── models/
│   └── events.py                 # Pydantic event models, exceptions, enums
├── aggregates/
│   ├── base.py                   # AggregateRoot base class
│   ├── loan_application.py       # LoanApplication state machine
│   ├── agent_session.py          # AgentSession + Gas Town
│   ├── compliance_record.py      # ComplianceRecord
│   └── audit_ledger.py           # AuditLedger + hash chain
├── commands/
│   └── handlers.py               # Command handlers (Load → Validate → Append)
├── projections/
│   ├── daemon.py                 # ProjectionDaemon (async polling)
│   ├── application_summary.py    # ApplicationSummary projection
│   ├── agent_performance.py      # AgentPerformanceLedger projection
│   └── compliance_audit.py       # ComplianceAuditView (temporal queries)
├── upcasting/
│   └── registry.py               # UpcasterRegistry + all upcasters
├── integrity/
│   ├── audit_chain.py            # Hash chain verification
│   └── gas_town.py               # Agent context reconstruction
├── mcp/
│   └── server.py                 # MCP Server (8 tools, 6 resources)
├── what_if/
│   └── projector.py              # Counterfactual analysis
└── regulatory/
    └── package.py                # Regulatory package generator

tests/
├── conftest.py                   # Shared fixtures (pool, schema, cleanup)
├── test_concurrency.py           # Double-decision OCC test
├── test_upcasting.py             # Immutability verification
├── test_projections.py           # Lag SLO & rebuild
├── test_gas_town.py              # Crash recovery
└── test_mcp_lifecycle.py         # End-to-end MCP lifecycle
```

---

## Key Design Decisions

See `DESIGN.md` for full architectural documentation.
See `DOMAIN_NOTES.md` for conceptual Q&A on Event Sourcing, OCC, Gas Town, and more.

---

## Demo Script (6-minute window)

### 1. Complete Decision History Query (< 60s)
```python
events = await store.load_stream("LoanApplication", application_id)
# Returns full event history in order
```

### 2. Concurrency Double-Decision Demo
```bash
pytest tests/test_concurrency.py::test_double_decision_exactly_one_succeeds -v -s
```

### 3. Temporal Compliance Query
```python
state = await get_state_at(conn, application_id, timestamp=datetime(2025, 3, 1, 14, 32))
```

### 4. Raw DB vs. Upcasted Payload Comparison
```bash
pytest tests/test_upcasting.py::test_raw_db_payload_unchanged_after_upcast -v -s
```

### 5. Agent Crash and Context Reconstruction
```bash
pytest tests/test_gas_town.py::test_context_preserved_after_crash -v -s
```

### 6. Regulatory Package Generation
```python
package = await generate_regulatory_package(pool, store, application_id)
# Returns self-contained JSON with events + projections + integrity proof
```
