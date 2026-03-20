# DOMAIN NOTES — TRP1 Week 5: The Ledger
## Apex Financial Services — Agentic Event Store & Enterprise Audit Infrastructure

---

## Q1: Why Event Sourcing over a Traditional CRUD Database for an AI Agent Audit System?

A traditional CRUD database stores only the *current state* of a record. When an AI agent updates a loan application, the previous state is overwritten and lost. For a financial audit system, this is unacceptable.

**Event Sourcing stores every state transition as an immutable fact.** The current state is a derived projection of all past events. This gives us:

1. **Complete Audit Trail by Design** — Not as an afterthought. Every agent decision, every confidence score, every compliance check is a first-class event. Regulators can replay the exact sequence of AI reasoning that led to a loan approval or denial.

2. **Temporal Queries** — "What did the system believe about this application at 14:32 on March 3rd?" is trivially answerable by replaying events up to that timestamp. In CRUD, this requires either a full audit log table (which is often incomplete) or point-in-time database snapshots (expensive, coarse-grained).

3. **Reproducibility** — Given the same event stream, the same aggregate state is always produced. This is critical for regulatory reproducibility: an auditor can independently reconstruct the system's state at any point.

4. **Debugging AI Agents** — When an agent makes a bad decision, we can replay the exact context it had. In CRUD, the context is gone the moment the next write happens.

5. **Decoupling via Events** — Multiple downstream systems (compliance, reporting, risk) can subscribe to the same event stream without coupling to the loan processing core.

**The tradeoff:** ES adds complexity — projections must be maintained, eventual consistency must be managed, and schema evolution requires upcasting rather than `ALTER TABLE`. For a financial AI platform where auditability is a regulatory requirement, this complexity is justified.

---

## Q2: What is the Difference Between an Event Stream and a Projection? When Should Each Be Used?

**Event Stream:** The append-only, ordered log of domain events for a specific aggregate instance (e.g., all events for `LoanApplication-abc123`). It is the *source of truth*. It is never queried directly by end-users or agents for read operations — it is the raw material.

**Projection:** A read-optimized, derived view built by processing events. It is a *materialized query result*. Examples: `ApplicationSummary` (current status of all applications), `AgentPerformanceLedger` (metrics per model version).

**When to use each:**

| Use Case | Use Event Stream | Use Projection |
|---|---|---|
| Reconstructing aggregate state to process a command | ✓ | |
| Answering "what is the current status of application X?" | | ✓ |
| Answering "what happened to application X?" (audit trail) | ✓ | |
| Dashboard: applications pending review | | ✓ |
| Compliance: state at a specific past timestamp | | ✓ (ComplianceAuditView) |
| Concurrency check before appending | ✓ (stream_version) | |

**Critical Rule (enforced in this system):** MCP Resources (queries) MUST read from projections. Only Command Handlers and the audit trail endpoint read from event streams. This enforces CQRS and ensures query performance is not coupled to stream length.

**Projection Lag:** Projections are eventually consistent. The `ProjectionDaemon` polls for new events and updates projections asynchronously. The SLO for `ApplicationSummary` is < 500ms lag. This is acceptable for a loan processing system where decisions take seconds to minutes, but must be monitored.

---

## Q3: Explain Optimistic Concurrency Control (OCC) in the Context of This Event Store. Why Not Use Pessimistic Locking?

**The Problem:** Two AI agents (e.g., a Credit Analyst agent and a Fraud Detection agent) may simultaneously attempt to append events to the same loan application stream. Without concurrency control, both could succeed, creating an inconsistent state (e.g., two conflicting decisions).

**Optimistic Concurrency Control:** Each `append` call includes an `expected_version` — the version the caller believes the stream is currently at. The database atomically checks: "Is the current stream version equal to `expected_version`?" If yes, append and increment. If no, raise `OptimisticConcurrencyError`.

```sql
-- Atomic check-and-insert in PostgreSQL
UPDATE event_streams 
SET current_version = current_version + 1
WHERE stream_id = $1 AND current_version = $expected_version
RETURNING current_version;
-- If 0 rows updated → OptimisticConcurrencyError
```

**Why Not Pessimistic Locking (`SELECT FOR UPDATE`)?**

1. **Deadlock Risk** — Multiple agents holding locks on different streams can deadlock. OCC has no locks, so no deadlocks.
2. **Scalability** — Locks serialize all writes to a stream. OCC allows concurrent attempts; only one wins, others retry. Under low contention (typical for loan processing where each application has one active agent), OCC has near-zero overhead.
3. **Distributed Systems** — Pessimistic locks don't compose across microservices or network partitions. OCC works with any database that supports atomic compare-and-swap.
4. **Failure Modes** — A crashed agent holding a pessimistic lock blocks all other agents until timeout. A crashed agent with OCC has no impact on others.

**Retry Strategy:** On `OptimisticConcurrencyError`, the agent should: reload the stream, re-apply business logic with the new state, and retry. The MCP tool returns a structured error with `suggested_action: "reload_and_retry"`. Estimated conflict rate in this domain: < 0.1% (loan applications are processed by one primary agent at a time; conflicts occur only during edge cases like timeout retries).

---

## Q4: What is Upcasting and Why is it Preferable to Migrating Historical Events?

**The Problem:** Event schemas evolve. Version 1 of `CreditAnalysisCompleted` had no `model_version` field. Version 2 adds it. We have 10,000 historical events in the database without this field.

**Option A — Migrate Historical Events (DO NOT DO THIS):**
Run an `UPDATE` on the `events` table to add `model_version: null` to all old events. This violates immutability — the event store is no longer a faithful record of what actually happened. It also creates audit risk: "Was this field always null, or was it added later?"

**Option B — Upcasting (CORRECT APPROACH):**
Leave historical events untouched in the database. When loading events at read time, pass them through an `UpcasterRegistry`. The upcaster detects `event_type = "CreditAnalysisCompleted"` and `schema_version = 1`, and transforms the payload to version 2 by adding `model_version: null`.

**Key Properties of Upcasting:**
- **Raw DB payload is NEVER modified.** `SELECT payload FROM events` always returns the original data.
- **Transformation is applied in memory at read time.** The aggregate sees a consistent v2 schema regardless of when the event was written.
- **Null vs. Inference:** For missing historical fields, use `null` (not inferred values) unless the value can be deterministically derived from other fields in the same event. Fabricating historical data (e.g., guessing `model_version = "gpt-3.5"`) is a compliance violation. `null` honestly represents "this information was not recorded at the time."

**Upcaster Chain:** If an event goes from v1 → v2 → v3, the registry applies upcasters in sequence. Each upcaster handles exactly one version transition.

---

## Q5: Describe the "Gas Town" Pattern. How Does it Apply to Agent Context Reconstruction After a Crash?

**Gas Town** is a pattern for ensuring an AI agent can resume work after a crash without losing context or re-processing events it has already handled. The name evokes a checkpoint/refueling station on a long journey.

**The Problem:** An AI agent processing a loan application loads context (credit score, fraud flags, compliance results) from multiple event streams. If the agent crashes mid-processing, a naive restart would either:
- Re-process all events from the beginning (expensive, potentially causing duplicate side effects)
- Lose all context and start fresh (incorrect, may miss critical information)

**The Solution — `reconstruct_agent_context`:**

1. **Session Events as Checkpoints:** Every significant context-loading step is recorded as an event in the `AgentSession` stream (e.g., `AgentContextLoaded`, `CreditDataFetched`, `FraudCheckCompleted`).

2. **On Crash Recovery:** The agent loads its `AgentSession` stream. It replays session events to determine: "What context had I already loaded? What was my last known position in each source stream?"

3. **Resume from Checkpoint:** The agent resumes from the last recorded checkpoint, not from the beginning. It only re-fetches context that was not yet recorded as loaded.

4. **Idempotency:** Context-loading operations must be idempotent. If `CreditDataFetched` is already in the session stream, the agent skips the credit fetch and uses the recorded data.

**Business Rule Enforcement:** The `AgentSession` aggregate enforces that `AgentContextLoaded` must be present before any decision event (`DecisionGenerated`) can be appended. This prevents a crashed-and-partially-recovered agent from making decisions without full context.

**Simulated Crash Test:** `test_gas_town.py` simulates a crash by raising an exception mid-processing, then reconstructing the agent session and verifying the agent resumes correctly with all previously loaded context intact.

---

## Q6: What are the Consistency Guarantees of This System? What Can Go Wrong and How is it Mitigated?

**Guarantees:**

1. **Within an Aggregate Stream:** Strong consistency. OCC ensures no two conflicting events are appended. The event stream for a single `LoanApplication` is always causally ordered and conflict-free.

2. **Across Aggregates:** Eventual consistency. A `LoanApplicationAggregate` and a `ComplianceRecordAggregate` communicate only via events. There is a window where one aggregate has processed an event that the other has not yet seen.

3. **Projections:** Eventually consistent with the event store. The `ProjectionDaemon` introduces lag (SLO: < 500ms for `ApplicationSummary`).

4. **Outbox → External Systems:** At-least-once delivery. The outbox table records events that need to be published to external systems. A separate relay process reads the outbox and publishes. If the relay crashes after publishing but before marking as sent, the event is re-published. Consumers must be idempotent.

**What Can Go Wrong:**

| Failure | Impact | Mitigation |
|---|---|---|
| `ProjectionDaemon` crash | Projections become stale | Daemon auto-restarts; checkpoint ensures no events are skipped on restart |
| OCC conflict storm (many agents retrying) | Throughput degradation | Exponential backoff with jitter; MCP returns `suggested_action` |
| Upcaster bug | Aggregate loads incorrect state | Upcasters are pure functions, unit-tested; raw DB data is unchanged so rollback is possible |
| Hash chain break in AuditLedger | Integrity violation detected | `run_integrity_check` identifies the exact event where chain breaks; alerts compliance team |
| Projection checkpoint corruption | Projection rebuilt from wrong position | Checkpoints are stored in DB with version; full rebuild from position 0 is always possible |
| Bad event in stream (malformed payload) | Daemon crash loop | Daemon catches exceptions per event, logs and skips after retry limit, continues processing |

**Cryptographic Integrity:** The `AuditLedger` aggregate maintains a hash chain: each event's hash includes the hash of the previous event. This makes tampering detectable — modifying any historical event breaks all subsequent hashes. This is not a substitute for database-level access controls, but provides a cryptographic audit trail that can be independently verified.
