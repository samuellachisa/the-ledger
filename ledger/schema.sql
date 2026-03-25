-- =============================================================================
-- Apex Financial Services — Ledger Event Store Schema
-- Used by ledger/ package (string stream_id, event_version column)
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Applicant Registry schema (read-only, populated by datagen)
CREATE SCHEMA IF NOT EXISTS applicant_registry;

CREATE TABLE IF NOT EXISTS applicant_registry.companies (
    company_id      VARCHAR(50) PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    sector          VARCHAR(100),
    jurisdiction    VARCHAR(10),
    state           VARCHAR(10),
    legal_type      VARCHAR(100),
    founded_year    INTEGER,
    risk_profile    VARCHAR(50),
    trajectory      VARCHAR(50),
    annual_revenue  NUMERIC(15, 2),
    requested_amount_usd NUMERIC(15, 2)
);

CREATE TABLE IF NOT EXISTS applicant_registry.financial_history (
    id              SERIAL PRIMARY KEY,
    company_id      VARCHAR(50) NOT NULL REFERENCES applicant_registry.companies(company_id),
    year            INTEGER NOT NULL,
    revenue         NUMERIC(15, 2),
    net_income      NUMERIC(15, 2),
    ebitda          NUMERIC(15, 2),
    total_assets    NUMERIC(15, 2),
    total_liabilities NUMERIC(15, 2),
    UNIQUE (company_id, year)
);

CREATE TABLE IF NOT EXISTS applicant_registry.compliance_flags (
    id              SERIAL PRIMARY KEY,
    company_id      VARCHAR(50) NOT NULL REFERENCES applicant_registry.companies(company_id),
    flag_type       VARCHAR(100) NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    severity        VARCHAR(20) DEFAULT 'LOW',
    description     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS applicant_registry.loan_relationships (
    id              SERIAL PRIMARY KEY,
    company_id      VARCHAR(50) NOT NULL REFERENCES applicant_registry.companies(company_id),
    loan_id         VARCHAR(50),
    status          VARCHAR(50),
    default_occurred BOOLEAN DEFAULT FALSE,
    amount          NUMERIC(15, 2),
    year            INTEGER
);

-- Event store tables
CREATE TABLE IF NOT EXISTS event_streams (
    stream_id       VARCHAR(255) PRIMARY KEY,
    aggregate_type  VARCHAR(100) NOT NULL,
    current_version INTEGER NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    archived_at     TIMESTAMPTZ
);

CREATE SEQUENCE IF NOT EXISTS ledger_events_global_position_seq;

CREATE TABLE IF NOT EXISTS events (
    event_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    stream_id       VARCHAR(255) NOT NULL REFERENCES event_streams(stream_id),
    stream_position INTEGER NOT NULL,
    global_position BIGINT NOT NULL DEFAULT nextval('ledger_events_global_position_seq'),
    event_type      VARCHAR(200) NOT NULL,
    event_version   INTEGER NOT NULL DEFAULT 1,
    payload         JSONB NOT NULL,
    metadata        JSONB NOT NULL DEFAULT '{}',
    recorded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (stream_id, stream_position)
);

CREATE INDEX IF NOT EXISTS idx_ledger_events_stream ON events (stream_id, stream_position);
CREATE INDEX IF NOT EXISTS idx_ledger_events_global ON events (global_position);
CREATE INDEX IF NOT EXISTS idx_ledger_events_type ON events (event_type);

-- Append-only: forbid UPDATE/DELETE on immutable event log
CREATE OR REPLACE FUNCTION ledger_prevent_events_mutation()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'events table is append-only: UPDATE and DELETE are not allowed'
    USING ERRCODE = 'integrity_constraint_violation';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS ledger_events_append_only ON events;
CREATE TRIGGER ledger_events_append_only
  BEFORE UPDATE OR DELETE ON events
  FOR EACH ROW EXECUTE PROCEDURE ledger_prevent_events_mutation();

CREATE TABLE IF NOT EXISTS outbox (
    id              SERIAL PRIMARY KEY,
    event_id        UUID NOT NULL,
    destination     VARCHAR(255) NOT NULL,
    payload         JSONB NOT NULL,
    published_at    TIMESTAMPTZ,
    attempts        INTEGER DEFAULT 0
);
