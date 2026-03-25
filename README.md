# Apex Financial Services — Agentic Event Store

Production-grade Event Sourcing infrastructure for AI agent loan processing. Built on PostgreSQL with full CQRS, optimistic concurrency control, cryptographic audit chains, and an MCP interface for agent interaction.

---

## Overview

This system implements Event Sourcing (not just Event-Driven Architecture) for a multi-agent loan processing platform. Every AI agent action is immutably recorded, cryptographically verifiable, and temporally queryable. The event store is the single source of truth — all application state is derived from it.

**Core capabilities:**
- Append-only event store with optimistic concurrency control (OCC)
- Four domain aggregates with enforced state machines and business rules
- Three async projections with < 500ms lag SLO and full rebuild support
- Upcasting registry for schema evolution without mutating historical data
- Cryptographic hash chain for tamper-evident audit trails
- Gas Town pattern for agent crash recovery and context reconstruction
- MCP server (`src/mcp/server.py`): command tools + read-only resources; see **MCP Interface** below
- What-if counterfactual projector with causal dependency filtering
- Self-verifiable regulatory packages for auditors

---

## Architecture

```
Commands (MCP Tools)
       │
       ▼
Command Handlers ──► Load Aggregate ──► Validate Rules ──► Append Events
                                                                  │
                                              ┌───────────────────┤
                                              │                   │
                                         Event Store          Outbox
                                         (PostgreSQL)      (same txn)
                                              │
                                              ▼
                                     ProjectionDaemon
                                    (async polling, 100ms)
                                              │
                          ┌───────────────────┼───────────────────┐
                          ▼                   ▼                   ▼
               ApplicationSummary   AgentPerformance    ComplianceAuditView
               (current state)      (metrics/model)     (temporal queries)
                          │
                          ▼
                   MCP Resources (Queries)
```

**Consistency model:** Strong within a single aggregate stream (OCC). Eventually consistent across aggregates and projections (< 500ms SLO).

### Canonical vs. narrative code paths

| Layer | Role |
|--------|------|
| **`src/`** | Production stack: `EventStore`, command handlers, aggregates (`src/aggregates/`), `src/projections/daemon.py`, `src/mcp/server.py`, `src/integrity/` (audit chain, Gas Town), `src/schema.sql`. |
| **`ledger/`** | Week-5 narrative and demos: LangGraph-style agents, `ledger/event_store.py` (parallel API), `ledger/mcp_server.py` (FastMCP), NARR tests. Uses the same PostgreSQL ideas; **prefer `src/` for grading and production wiring.** |

**MCP for assessment:** Use **`src/mcp/server.py`** (definitions in `src/mcp/tools.py` and `src/mcp/resources.py`). The FastMCP module **`ledger/mcp_server.py`** is a compact alternate demo only.

Regulatory and MCP lifecycle tests target **`src/`**. Set `PROJECTION_DAEMON_LEADER_ELECTION=true` in production when multiple processes could run the projection daemon (PostgreSQL advisory lock; see `src/leader_election/lock.py`).

**Run the production MCP server (stdio):** `python -m src.mcp.server` from the repo root with `DATABASE_URL` set (see Setup).

---

## Prerequisites

- Python 3.11+
- PostgreSQL 16+
- pip or uv

---

## Setup

**1. Install dependencies**

```bash
pip install -r requirements.txt
```

**2. Create databases**

```bash
createdb apex_financial
createdb apex_financial_test   # for tests
```

**3. Apply schema**

```bash
psql -d apex_financial -f src/schema.sql
psql -d apex_financial_test -f src/schema.sql
```

**4. Set environment variables**

```bash
export DATABASE_URL="postgresql://postgres:postgres@localhost:5432/apex_financial"
export TEST_DATABASE_URL="postgresql://postgres:postgres@localhost:5432/apex_financial_test"
```

---

## Running the MCP Server

```bash
python -m src.mcp.server
```

The server communicates over stdio and exposes the full loan processing interface to any MCP-compatible agent.

---

## Running Tests

```bash
# Full suite
pytest tests/ -v

# Individual suites
pytest tests/test_concurrency.py -v    # OCC double-decision (exactly one of two concurrent appends wins)
pytest tests/test_upcasting.py -v      # Schema evolution — raw DB payload verified unchanged
pytest tests/test_projections.py -v    # Lag SLO, rebuild-from-scratch, temporal queries
pytest tests/test_gas_town.py -v       # Agent crash recovery and context reconstruction
pytest tests/test_mcp_lifecycle.py -v  # Full loan lifecycle driven via command handlers
pytest tests/test_invariants.py -v     # Business rules under concurrent load, counterfactual commands
pytest tests/test_regulatory.py -v     # Regulatory package generation and independent verification
```

All tests require a live PostgreSQL instance. Tables are truncated between tests for isolation.

---

## Loan Processing Lifecycle

```
submit_application
    └─► record_credit_analysis        (Submitted → AwaitingAnalysis → UnderReview)
        └─► record_fraud_check
            └─► request_compliance_check   (UnderReview → ComplianceCheck)
                └─► record_compliance_check × N
                    └─► finalize_compliance
                        └─► generate_decision      (→ PendingDecision)
                            └─► finalize_application   (→ FinalApproved | Denied | Referred)
```

**Business rules (LoanApplication)** — full table with test pointers: **`DESIGN.md` §5**.

---

## MCP Interface

**Tools (Commands)** — write operations, enforce OCC and business rules (`src/mcp/tools.py`):

| Tool | Precondition |
|---|---|
| `submit_application` | No existing application for this ID |
| `record_credit_analysis` | Application in `Submitted` status |
| `record_fraud_check` | Application in `UnderReview` status |
| `request_compliance_check` | Application in `UnderReview` status |
| `record_compliance_check` | ComplianceRecord exists and `InProgress` |
| `finalize_compliance` | All required checks recorded |
| `generate_decision` | Context loaded; compliance finalized; confidence floor respected |
| `finalize_application` | Application in `PendingDecision` status |

**Resources (Queries)** — read models and justified reads (`src/mcp/resources.py`). Most URIs hit **projections** only; `audit-trail`, `correlation`, and `causation` resources intentionally use the event store for **audit / traceability** (see file docstring).

| Resource | Description |
|---|---|
| `ledger://applications` | All applications (paginated) |
| `ledger://applications/{id}` | Current application state |
| `ledger://applications/{id}/audit-trail` | Full event history |
| `ledger://applications/{id}/compliance` | Temporal compliance history |
| `ledger://agents/performance` | Decision metrics per model version |
| `ledger://integrity/{id}` | Hash chain verification result |

**Structured errors** — tools return JSON with `error`, `message`, and type-specific fields (e.g. OCC versions, `rule` for `BusinessRuleViolationError`). Resource read failures return JSON with `error` and `message`.

```json
{
  "error": "OptimisticConcurrencyError",
  "message": "...",
  "stream_id": "...",
  "expected_version": 3,
  "actual_version": 4,
  "suggested_action": "reload_and_retry"
}
```

**End-to-end MCP-only test:** `tests/test_mcp_lifecycle.py`. **Rubric mapping:** `docs/RUBRIC_COVERAGE.md`.

---

## Concurrency Control

OCC is implemented in **`src/event_store.py`** inside a **single transaction**: new streams use `INSERT`; existing streams use `SELECT … FOR UPDATE` then `UPDATE` when `current_version == expected_version`; otherwise `OptimisticConcurrencyError`. Outbox rows are written in the **same transaction**. Full detail: **`DESIGN.md` §3**.

`append_with_retry()` retries on OCC with exponential backoff (10ms base, up to 5 attempts).

---

## Schema Evolution (Upcasting)

Historical events are **never modified**. When a schema changes, an upcaster is registered that transforms the payload in memory at read time:

```python
@registry.register("CreditAnalysisCompleted", from_version=1)
def upcast_v1_to_v2(payload: dict) -> dict:
    return {**payload, "model_version": None}  # null, not fabricated
```

The raw database payload is always the original. `SELECT payload FROM events` returns what was written. Upcasting only happens in the application layer.

---

## Projections

The `ProjectionDaemon` polls for new events every 100ms and updates all registered projections atomically (checkpoint updated in the same transaction as the projection row).

**Rebuild procedure** (zero-downtime):
```bash
# Reset checkpoint and truncate projection table
# Daemon will replay all events from position 0 on next poll
```

Programmatically:
```python
await daemon.rebuild_projection("ApplicationSummary")
```

**Lag monitoring:**
```python
lag = await daemon.get_lag("ApplicationSummary")
# {"event_lag": 0, "time_lag_ms": 42.3, "checkpoint": 1847, "head_position": 1847}
```

---

## Cryptographic Audit Chain

Every significant event is recorded in the `audit_ledger_projection` with a SHA-256 hash chain:

```
chain_hash[n] = SHA-256(chain_hash[n-1] + event_hash[n])
```

Modifying any historical event breaks all subsequent chain hashes. Run a verification:

```python
result = await run_integrity_check(pool, store, application_id)
# {"chain_valid": True, "entries_checked": 12, "broken_at_sequence": None}
```

---

## Gas Town (Agent Crash Recovery)

Agent sessions record `AgentContextLoaded` events as checkpoints. On crash recovery:

```python
context = await reconstructor.reconstruct_agent_context(session_id)
# {
#   "context_loaded": True,
#   "loaded_stream_positions": {"LoanApplication": 5, "ComplianceRecord": 2},
#   "crash_detected": True,
#   "resume_required": True
# }

missing = await reconstructor.get_missing_context_sources(session_id, required_sources)
# Only re-fetch sources not already checkpointed
```

---

## Regulatory Package

Generate a self-contained, independently verifiable audit package:

```python
package = await generate_regulatory_package(pool, store, application_id)
```

The package contains raw events, upcasted events, projection state, and an integrity proof. An auditor can verify it offline without system access:

```python
result = verify_regulatory_package(package)
# {"package_hash_valid": True, "chain_valid": True, "overall_valid": True}
```

---

## Project Structure

```
src/
├── schema.sql                    # PostgreSQL schema — all tables, indexes, constraints
├── event_store.py                # EventStore: append (OCC), load_stream, append_with_retry
├── models/
│   └── events.py                 # Pydantic event models, domain exceptions, enums
├── aggregates/
│   ├── base.py                   # AggregateRoot: load, apply, _raise_event
│   ├── loan_application.py       # State machine + rules (DESIGN.md §5)
│   ├── agent_session.py          # Gas Town checkpoint pattern
│   ├── compliance_record.py      # Compliance check lifecycle
│   └── audit_ledger.py           # Cryptographic hash chain
├── commands/
│   └── handlers.py               # Command handlers: Load → Validate → Append
├── projections/
│   ├── daemon.py                 # Async polling daemon, lag metrics, rebuild
│   ├── application_summary.py    # Current loan state (< 500ms SLO)
│   ├── agent_performance.py      # Decision metrics per model version
│   └── compliance_audit.py       # Temporal snapshots, get_state_at(timestamp)
├── upcasting/
│   └── registry.py               # Schema migration at read time, never at write time
├── integrity/
│   ├── audit_chain.py            # Hash chain record + verification
│   └── gas_town.py               # Context reconstruction, crash simulation
├── mcp/
│   └── server.py                 # MCP server: 8 tools, 6 resources
├── what_if/
│   └── projector.py              # Counterfactual analysis with causal dependency filtering
└── regulatory/
    └── package.py                # Self-verifiable regulatory package generator

tests/
├── conftest.py                   # DB pool, schema setup, per-test table truncation
├── test_concurrency.py           # OCC: double-decision, 10-way stress, structured errors
├── test_upcasting.py             # Immutability: raw DB payload unchanged after upcast
├── test_projections.py           # Lag SLO, rebuild truncation, temporal queries
├── test_gas_town.py              # Crash recovery, context preservation, Gas Town rule
├── test_mcp_lifecycle.py         # Full lifecycle: submit → approve, confidence floor, compliance
├── test_invariants.py            # Concurrent invariants, counterfactual command testing
└── test_regulatory.py            # Package generation, tamper detection, chain break detection
```

---

## Further Reading

- `DESIGN.md` — architectural decisions, schema column justifications, concurrency analysis, EventStoreDB comparison, and reflection on what the implementation got wrong
- `DOMAIN_NOTES.md` — conceptual Q&A covering Event Sourcing vs CRUD, OCC vs pessimistic locking, upcasting strategy, Gas Town pattern, and consistency guarantees
