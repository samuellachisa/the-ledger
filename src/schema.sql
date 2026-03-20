-- =============================================================================
-- Apex Financial Services — Event Store Schema
-- PostgreSQL 16+
-- =============================================================================

-- Enable UUID generation
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- =============================================================================
-- event_streams: One row per aggregate instance.
-- Tracks the current version for OCC (Optimistic Concurrency Control).
-- current_version is the sequence number of the last appended event.
-- =============================================================================
CREATE TABLE IF NOT EXISTS event_streams (
    stream_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type  VARCHAR(100) NOT NULL,   -- e.g. 'LoanApplication', 'AgentSession'
    aggregate_id    UUID NOT NULL,           -- Business identity of the aggregate
    current_version INTEGER NOT NULL DEFAULT 0,  -- Last event version; 0 = no events
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (aggregate_type, aggregate_id)    -- One stream per aggregate instance
);

-- Index for fast lookup by aggregate identity
CREATE INDEX IF NOT EXISTS idx_event_streams_aggregate
    ON event_streams (aggregate_type, aggregate_id);

-- =============================================================================
-- events: The immutable append-only event log. Source of truth.
-- global_position: monotonically increasing sequence across ALL streams.
--   Used by ProjectionDaemon to poll for new events in order.
-- stream_position: version within a specific stream (1-based).
--   Used for OCC: expected_version must equal current stream_position.
-- schema_version: event payload schema version. Used by UpcasterRegistry.
-- causation_id: the command/event that caused this event (traceability).
-- correlation_id: the top-level request/saga that this event belongs to.
-- metadata: agent context, model version, confidence scores, etc.
-- =============================================================================
CREATE SEQUENCE IF NOT EXISTS events_global_position_seq;

CREATE TABLE IF NOT EXISTS events (
    event_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stream_id       UUID NOT NULL REFERENCES event_streams(stream_id),
    aggregate_type  VARCHAR(100) NOT NULL,
    aggregate_id    UUID NOT NULL,
    event_type      VARCHAR(200) NOT NULL,   -- e.g. 'LoanApplicationSubmitted'
    schema_version  INTEGER NOT NULL DEFAULT 1,  -- For upcasting
    stream_position INTEGER NOT NULL,        -- Position within this stream (1-based)
    global_position BIGINT NOT NULL DEFAULT nextval('events_global_position_seq'),
    payload         JSONB NOT NULL,          -- Domain event data
    metadata        JSONB NOT NULL DEFAULT '{}',  -- Agent context, correlation IDs
    causation_id    UUID,                    -- Event/command that caused this
    correlation_id  UUID,                    -- Top-level saga/request ID
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Immutability constraint: no updates allowed (enforced at app layer + no UPDATE grants)
    UNIQUE (stream_id, stream_position)      -- No duplicate positions in a stream
);

-- Primary read path: load all events for a stream in order
CREATE INDEX IF NOT EXISTS idx_events_stream_position
    ON events (stream_id, stream_position);

-- ProjectionDaemon polling: find all events after a given global position
CREATE INDEX IF NOT EXISTS idx_events_global_position
    ON events (global_position);

-- Audit queries: find events by type across all streams
CREATE INDEX IF NOT EXISTS idx_events_event_type
    ON events (event_type, recorded_at);

-- Temporal queries: find events before/after a timestamp
CREATE INDEX IF NOT EXISTS idx_events_recorded_at
    ON events (recorded_at);

-- Aggregate-level queries
CREATE INDEX IF NOT EXISTS idx_events_aggregate
    ON events (aggregate_type, aggregate_id, stream_position);

-- =============================================================================
-- projection_checkpoints: Tracks the last processed global_position per projection.
-- Enables the ProjectionDaemon to resume after restart without reprocessing.
-- projection_name: unique identifier for each projection.
-- last_position: the global_position of the last successfully processed event.
-- =============================================================================
CREATE TABLE IF NOT EXISTS projection_checkpoints (
    projection_name VARCHAR(100) PRIMARY KEY,
    last_position   BIGINT NOT NULL DEFAULT 0,  -- 0 = never processed any event
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed checkpoints for all projections
INSERT INTO projection_checkpoints (projection_name, last_position)
VALUES
    ('ApplicationSummary', 0),
    ('AgentPerformanceLedger', 0),
    ('ComplianceAuditView', 0)
ON CONFLICT (projection_name) DO NOTHING;

-- =============================================================================
-- outbox: Transactional outbox for reliable event publishing to external systems.
-- Written in the SAME transaction as the event append.
-- A separate relay process reads pending rows and publishes to message broker.
-- status: 'pending' → 'published' (or 'failed' after max retries)
-- =============================================================================
CREATE TABLE IF NOT EXISTS outbox (
    outbox_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID NOT NULL REFERENCES events(event_id),
    aggregate_type  VARCHAR(100) NOT NULL,
    event_type      VARCHAR(200) NOT NULL,
    payload         JSONB NOT NULL,
    metadata        JSONB NOT NULL DEFAULT '{}',
    status          VARCHAR(20) NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending', 'published', 'failed')),
    retry_count     INTEGER NOT NULL DEFAULT 0,
    max_retries     INTEGER NOT NULL DEFAULT 5,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at    TIMESTAMPTZ,
    last_error      TEXT
);

-- Relay process polls for pending outbox entries
CREATE INDEX IF NOT EXISTS idx_outbox_pending
    ON outbox (status, created_at)
    WHERE status = 'pending';

-- =============================================================================
-- application_summary_projection: Materialized view of current loan state.
-- Rebuilt by ProjectionDaemon from events. Never written to directly by commands.
-- =============================================================================
CREATE TABLE IF NOT EXISTS application_summary_projection (
    application_id      UUID PRIMARY KEY,
    applicant_name      VARCHAR(255),
    loan_amount         NUMERIC(15, 2),
    status              VARCHAR(50) NOT NULL,
    current_version     INTEGER NOT NULL DEFAULT 0,
    submitted_at        TIMESTAMPTZ,
    last_decision_at    TIMESTAMPTZ,
    assigned_agent      VARCHAR(100),
    credit_score        INTEGER,
    confidence_score    NUMERIC(5, 4),
    compliance_passed   BOOLEAN,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- agent_performance_projection: Metrics per agent model version.
-- =============================================================================
CREATE TABLE IF NOT EXISTS agent_performance_projection (
    model_version       VARCHAR(100) PRIMARY KEY,
    total_decisions     INTEGER NOT NULL DEFAULT 0,
    approved_count      INTEGER NOT NULL DEFAULT 0,
    denied_count        INTEGER NOT NULL DEFAULT 0,
    referred_count      INTEGER NOT NULL DEFAULT 0,
    avg_confidence      NUMERIC(5, 4),
    avg_processing_ms   NUMERIC(10, 2),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- compliance_audit_projection: Temporal compliance state.
-- Stores snapshots at each compliance event for point-in-time queries.
-- =============================================================================
CREATE TABLE IF NOT EXISTS compliance_audit_projection (
    record_id           UUID NOT NULL,
    application_id      UUID NOT NULL,
    as_of_timestamp     TIMESTAMPTZ NOT NULL,  -- When this state was valid from
    status              VARCHAR(50) NOT NULL,
    checks_passed       JSONB NOT NULL DEFAULT '[]',
    checks_failed       JSONB NOT NULL DEFAULT '[]',
    officer_id          VARCHAR(100),
    notes               TEXT,
    PRIMARY KEY (record_id, as_of_timestamp)
);

CREATE INDEX IF NOT EXISTS idx_compliance_audit_temporal
    ON compliance_audit_projection (application_id, as_of_timestamp DESC);

-- =============================================================================
-- audit_ledger_projection: Cryptographic hash chain for tamper detection.
-- =============================================================================
CREATE TABLE IF NOT EXISTS audit_ledger_projection (
    ledger_entry_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id      UUID NOT NULL,
    event_id            UUID NOT NULL REFERENCES events(event_id),
    event_type          VARCHAR(200) NOT NULL,
    event_hash          VARCHAR(64) NOT NULL,   -- SHA-256 of event payload
    chain_hash          VARCHAR(64) NOT NULL,   -- SHA-256(prev_chain_hash + event_hash)
    sequence_number     INTEGER NOT NULL,
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (application_id, sequence_number)
);

CREATE INDEX IF NOT EXISTS idx_audit_ledger_application
    ON audit_ledger_projection (application_id, sequence_number);

-- =============================================================================
-- saga_instances: Durable state for the LoanProcessingSaga process manager.
-- One row per loan application. Tracks which step the saga is at so it can
-- resume after a crash without re-issuing already-completed commands.
--
-- step values: started | compliance_requested | compliance_finalized |
--              completed | failed
-- =============================================================================
CREATE TABLE IF NOT EXISTS saga_instances (
    saga_id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    application_id          UUID NOT NULL UNIQUE,   -- one saga per application
    step                    VARCHAR(50) NOT NULL DEFAULT 'started',
    compliance_record_id    UUID,                   -- set after compliance_requested
    last_causation_id       UUID,                   -- event_id that triggered last step
    retry_count             INTEGER NOT NULL DEFAULT 0,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_saga_instances_compliance_record
    ON saga_instances (compliance_record_id)
    WHERE compliance_record_id IS NOT NULL;

-- =============================================================================
-- saga_checkpoints: Tracks the last global_position processed by each saga.
-- Mirrors projection_checkpoints — same polling pattern, separate table to
-- keep saga concerns isolated from projection concerns.
-- =============================================================================
CREATE TABLE IF NOT EXISTS saga_checkpoints (
    saga_name       VARCHAR(100) PRIMARY KEY,
    last_position   BIGINT NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO saga_checkpoints (saga_name, last_position)
VALUES ('LoanProcessingSaga', 0)
ON CONFLICT (saga_name) DO NOTHING;

-- =============================================================================
-- aggregate_snapshots: Periodic state snapshots to avoid full stream replay.
-- A snapshot captures the aggregate state at a given stream_position.
-- On load: find latest snapshot, replay only events AFTER snapshot_version.
--
-- snapshot_data: serialized aggregate state (JSON)
-- stream_position: the stream version this snapshot was taken at
-- =============================================================================
CREATE TABLE IF NOT EXISTS aggregate_snapshots (
    snapshot_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    aggregate_type  VARCHAR(100) NOT NULL,
    aggregate_id    UUID NOT NULL,
    stream_position INTEGER NOT NULL,
    snapshot_data   JSONB NOT NULL,
    schema_version  INTEGER NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (aggregate_type, aggregate_id, stream_position)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_latest
    ON aggregate_snapshots (aggregate_type, aggregate_id, stream_position DESC);

-- =============================================================================
-- dead_letter_events: Events that failed projection/saga processing after
-- all retries. Stored for operator inspection and manual replay.
--
-- source: 'projection' | 'saga'
-- processor_name: projection name or saga name
-- error_type / error_message: last failure details
-- resolved_at: set when operator marks the entry resolved
-- =============================================================================
CREATE TABLE IF NOT EXISTS dead_letter_events (
    dead_letter_id  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id        UUID NOT NULL,
    global_position BIGINT NOT NULL,
    aggregate_type  VARCHAR(100) NOT NULL,
    event_type      VARCHAR(200) NOT NULL,
    payload         JSONB NOT NULL,
    source          VARCHAR(50) NOT NULL CHECK (source IN ('projection', 'saga')),
    processor_name  VARCHAR(100) NOT NULL,
    retry_count     INTEGER NOT NULL DEFAULT 0,
    error_type      VARCHAR(200),
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ,
    resolved_by     VARCHAR(100)
);

CREATE INDEX IF NOT EXISTS idx_dead_letter_unresolved
    ON dead_letter_events (source, processor_name, created_at)
    WHERE resolved_at IS NULL;

-- =============================================================================
-- agent_tokens: Issued session tokens for MCP agent authentication.
-- token_hash: SHA-256 of the raw bearer token (never store plaintext).
-- agent_id: the verified identity this token belongs to.
-- roles: JSON array of role strings e.g. ["decision_agent", "read_only"]
-- expires_at: tokens are short-lived (default 24h)
-- revoked_at: set on explicit revocation
-- =============================================================================
CREATE TABLE IF NOT EXISTS agent_tokens (
    token_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    token_hash      VARCHAR(64) NOT NULL UNIQUE,  -- SHA-256 hex
    agent_id        VARCHAR(100) NOT NULL,
    roles           JSONB NOT NULL DEFAULT '[]',
    issued_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ NOT NULL,
    revoked_at      TIMESTAMPTZ,
    last_used_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_agent_tokens_hash
    ON agent_tokens (token_hash)
    WHERE revoked_at IS NULL;

-- =============================================================================
-- rate_limit_buckets: Token-bucket rate limiting per agent_id + action.
-- tokens: current token count (float stored as numeric)
-- last_refill: timestamp of last refill calculation
-- =============================================================================
CREATE TABLE IF NOT EXISTS rate_limit_buckets (
    bucket_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    agent_id        VARCHAR(100) NOT NULL,
    action          VARCHAR(100) NOT NULL,
    tokens          NUMERIC(10, 4) NOT NULL,
    capacity        NUMERIC(10, 4) NOT NULL,
    refill_rate     NUMERIC(10, 4) NOT NULL,  -- tokens per second
    last_refill     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (agent_id, action)
);

-- =============================================================================
-- Index to support correlation/causation chain queries.
-- Allows "show me all events caused by this correlation_id" in O(log n).
-- =============================================================================
CREATE INDEX IF NOT EXISTS idx_events_correlation_id
    ON events (correlation_id)
    WHERE correlation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_events_causation_id
    ON events (causation_id)
    WHERE causation_id IS NOT NULL;

-- =============================================================================
-- refresh_tokens: Allows agents to exchange an expiring token for a new one
-- without re-authenticating from scratch.
-- parent_token_id: the token that was exchanged to produce this one
-- =============================================================================
ALTER TABLE agent_tokens ADD COLUMN IF NOT EXISTS parent_token_id UUID REFERENCES agent_tokens(token_id);
