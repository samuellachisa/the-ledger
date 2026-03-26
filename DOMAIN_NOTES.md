# DOMAIN NOTES — TRP1 Week 5: The Ledger
## Apex Financial Services — Agentic Event Store & Enterprise Audit Infrastructure

These six questions are answered in **conceptual prose only** (no code or SQL), so assessors can judge domain understanding separately from implementation detail. Implementation evidence lives in **TECHNICAL_REPORT.md** and the codebase.

---

## Q1: Why Event Sourcing over a Traditional CRUD Database for an AI Agent Audit System?

A traditional CRUD database stores only the *current state* of a record. When an AI agent updates a loan application, the previous state is overwritten and lost. For a financial audit system, this is unacceptable.

**Event Sourcing stores every state transition as an immutable fact.** The current state is a derived projection of all past events. This gives us:

1. **Complete Audit Trail by Design** — Not as an afterthought. Every agent decision, every confidence score, every compliance check is a first-class event. Regulators can replay the exact sequence of AI reasoning that led to a loan approval or denial.

2. **Temporal Queries** — "What did the system believe about this application at 14:32 on March 3rd?" is trivially answerable by replaying events up to that timestamp. In CRUD, this requires either a full audit log table (which is often incomplete) or point-in-time database snapshots (expensive, coarse-grained).

3. **Reproducibility** — Given the same event stream, the same aggregate state is always produced. This is critical for regulatory reproducibility: an auditor can independently reconstruct the system's state at any point.

4. **Debugging AI Agents** — When an agent makes a bad decision, we can replay the exact context it had. In CRUD, the context is gone the moment the next write happens.

5. **Decoupling via Events** — Multiple downstream systems (compliance, reporting, risk) can subscribe to the same event stream without coupling to the loan processing core.

**The tradeoff:** ES adds complexity — projections must be maintained, eventual consistency must be managed, and schema evolution favors read-time adaptation of old payloads instead of rewriting historical rows in place. For a financial AI platform where auditability is a regulatory requirement, this complexity is justified.

---

## Q2: What is the Difference Between an Event Stream and a Projection? When Should Each Be Used?

**Event stream:** The append-only, ordered log of domain events for one aggregate instance (for example, everything that ever happened for a single loan application). It is the *source of truth*. Casual “what is on screen?” queries should not scan the whole stream every time — that is what projections are for.

**Projection:** A read-optimized, derived view built by processing events. It is a *materialized query result*. Examples include a per-application summary row for dashboards and rolling agent-performance metrics by model version.

**When to use each:**

| Use Case | Use Event Stream | Use Projection |
|---|---|---|
| Reconstructing aggregate state to process a command | ✓ | |
| Answering "what is the current status of application X?" | | ✓ |
| Answering "what happened to application X?" (audit trail) | ✓ | |
| Dashboard: applications pending review | | ✓ |
| Compliance: state at a specific past timestamp | | ✓ (ComplianceAuditView) |
| Concurrency check before appending | ✓ (stream_version) | |

**Critical rule (enforced in this system):** MCP resources that answer “what is the current status?” read from projections. Command handling and strong audit reads use the event stream (or dedicated audit paths). That split is classic CQRS: writes append facts; reads use materialized views so dashboards stay fast as streams grow.

**Projection lag:** Projections are eventually consistent. A background process applies new events to read models on a short polling interval. The service-level objective for the main application summary read model is sub-second lag; human-scale loan decisions tolerate that window, but lag must be monitored because it defines how stale a dashboard can be relative to the log.

---

## Q3: Explain Optimistic Concurrency Control (OCC) in the Context of This Event Store. Why Not Use Pessimistic Locking?

**The Problem:** Two AI agents (e.g., a Credit Analyst agent and a Fraud Detection agent) may simultaneously attempt to append events to the same loan application stream. Without concurrency control, both could succeed, creating an inconsistent state (e.g., two conflicting decisions).

**Optimistic Concurrency Control:** Each append carries an expected stream version — what the caller believes is current. The store applies the new events only if that expectation matches the persisted version; otherwise it signals a concurrency conflict so the caller reloads the stream, reapplies domain logic, and retries. The check and the write happen in one atomic database transaction (or equivalent locking strategy), so two writers cannot both succeed at the same stream position.

**Why Not Pessimistic Locking (long-held row or table locks)?**

1. **Deadlock Risk** — Multiple agents holding locks on different streams can deadlock. OCC has no locks, so no deadlocks.
2. **Scalability** — Locks serialize all writes to a stream. OCC allows concurrent attempts; only one wins, others retry. Under low contention (typical for loan processing where each application has one active agent), OCC has near-zero overhead.
3. **Distributed Systems** — Pessimistic locks don't compose across microservices or network partitions. OCC works with any database that supports atomic compare-and-swap.
4. **Failure Modes** — A crashed agent holding a pessimistic lock blocks all other agents until timeout. A crashed agent with OCC has no impact on others.

**Retry Strategy:** On a concurrency conflict, the agent reloads the stream, reapplies business rules against the new state, and retries the append. Integration surfaces return a structured error that includes a machine-readable suggested action (for example, reload and retry) so automated agents can decide without parsing stack traces. In this domain, contention per application is low because a single primary agent usually owns a case; conflicts show up mainly on retries or rare overlapping commands, so conflict rates stay well below one percent in practice.

---

## Q4: What is Upcasting and Why is it Preferable to Migrating Historical Events?

**The problem:** Event schemas evolve. An early version of a credit-analysis event might omit a field that a later version requires (such as which model version scored the applicant). Thousands of rows may already exist in the older shape.

**Option A — Migrate historical events (do not do this):** Bulk-update stored payloads in place so every row contains the new keys. That breaks immutability: the log no longer records exactly what was persisted at the time of the business fact, and auditors cannot tell whether a value was observed historically or invented during a migration.

**Option B — Upcasting (correct approach):**
Leave historical events untouched on disk. When loading events at read time, pass them through a version-aware registry that maps each stored schema version to the current contract. For example, an older credit-analysis event without a model-version field is presented to the domain as a newer-shaped event with that field empty, without rewriting rows in storage.

**Key properties of upcasting:**
- **Persisted payloads stay as written.** What you stored years ago is still what you query literally; transformation is not a silent database edit.
- **Transformation happens in memory on the read path.** The domain always sees the current contract regardless of when the event was written.
- **Null versus inference:** For missing historical fields, use an honest absence (null) unless the value can be deterministically derived from other fields in the same event. Guessing a model name or score that was never recorded is a compliance risk; null means “not recorded then.”

**Upcaster Chain:** If an event goes from v1 → v2 → v3, the registry applies upcasters in sequence. Each upcaster handles exactly one version transition.

---

## Q5: Describe the "Gas Town" Pattern. How Does it Apply to Agent Context Reconstruction After a Crash?

**Gas Town** is a pattern for ensuring an AI agent can resume work after a crash without losing context or re-processing events it has already handled. The name evokes a checkpoint/refueling station on a long journey.

**The Problem:** An AI agent processing a loan application loads context (credit score, fraud flags, compliance results) from multiple event streams. If the agent crashes mid-processing, a naive restart would either:
- Re-process all events from the beginning (expensive, potentially causing duplicate side effects)
- Lose all context and start fresh (incorrect, may miss critical information)

**The solution — context reconstruction:**

1. **Session events as checkpoints:** Every significant context-loading step is recorded as an event on the agent-session stream (for example, context loaded, credit data fetched, fraud check completed).

2. **On crash recovery:** The agent reloads its session stream and replays those events to learn what context was already bound and how far it had read each upstream stream.

3. **Resume from checkpoint:** Work continues from the last checkpoint, not from the beginning of time. The agent only fetches context that was not yet recorded as loaded.

4. **Idempotency:** Loading steps must be safe to repeat: if the session already shows that credit data was fetched, the agent does not refetch blindly.

**Business rule enforcement:** The session aggregate requires that “context loaded” was recorded before a decision can be appended. That blocks a partially recovered agent from approving without the same evidence chain a healthy run would have had.

**Simulated crash recovery:** Automated tests interrupt processing mid-stream, then reconstruct the agent session from its own events and confirm that previously recorded checkpoints still hold and that the agent does not repeat work or skip required context before a decision.

---

## Q6: What are the Consistency Guarantees of This System? What Can Go Wrong and How is it Mitigated?

**Guarantees:**

1. **Within one aggregate stream:** Strong consistency. Optimistic concurrency ensures no two conflicting appends both win. The stream for a single loan application stays totally ordered and free of contradictory facts at the same positions.

2. **Across aggregates:** Eventual consistency. Loan and compliance lifecycles interact only through events and orchestration. There is always a possible window where one side has moved forward and another read model has not yet caught up.

3. **Projections:** Eventually consistent with the event store. The projection worker introduces bounded lag (target under half a second for the primary summary read model in normal conditions).

4. **Outbox → External Systems:** At-least-once delivery. The outbox table records events that need to be published to external systems. A separate relay process reads the outbox and publishes. If the relay crashes after publishing but before marking as sent, the event is re-published. Consumers must be idempotent.

**What Can Go Wrong:**

| Failure | Impact | Mitigation |
|---|---|---|
| Projection worker crash | Projections become stale | Process restarts; checkpoints ensure no duplicate skips on recovery |
| OCC conflict storm (many agents retrying) | Throughput degradation | Exponential backoff with jitter; APIs return structured hints so clients reload and retry |
| Upcaster bug | Aggregate loads incorrect state | Upcasters are pure, testable functions; raw stored payloads are unchanged so software rollback fixes reads |
| Hash chain break in audit ledger | Integrity violation detected | Integrity pass identifies the break position; operators investigate before trusting the chain |
| Projection checkpoint corruption | Projection rebuilt from wrong position | Checkpoints are stored in DB with version; full rebuild from position 0 is always possible |
| Bad event in stream (malformed payload) | Daemon crash loop | Daemon catches exceptions per event, logs and skips after retry limit, continues processing |

**Cryptographic integrity:** The audit ledger aggregate maintains a hash chain: each entry’s digest binds the previous digest. Altering any past entry breaks the chain forward. That does not replace database permissions, but it gives regulators an independently verifiable tamper check.
