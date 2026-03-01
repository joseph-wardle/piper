-- Piper warehouse — migration 001: initial schema.
--
-- silver_events   : one row per accepted telemetry event (flattened Envelope)
-- ingest_manifest : one row per processed source JSONL file
--
-- All tables use CREATE TABLE IF NOT EXISTS so this migration is idempotent.

-- ----------------------------------------------------------------------------
-- silver_events
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS silver_events (

    -- ── Identity ─────────────────────────────────────────────────────────────
    event_id        TEXT        PRIMARY KEY,
    schema_version  TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    occurred_at_utc TIMESTAMPTZ NOT NULL,
    status          TEXT        NOT NULL,
    ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,

    -- ── Pipeline context (flattened from envelope.pipeline) ──────────────────
    pipeline_name   TEXT        NOT NULL,
    pipeline_dcc    TEXT,                   -- NULL for background collectors

    -- ── Host context (flattened from envelope.host) ───────────────────────────
    host_hostname   TEXT        NOT NULL,
    host_user       TEXT        NOT NULL,
    host_os         TEXT,

    -- ── Session context (flattened from envelope.session) ─────────────────────
    session_id      TEXT        NOT NULL,
    action_id       TEXT,

    -- ── Scope (flattened from envelope.scope — all fields sparse) ──────────────
    scope_show       TEXT,
    scope_sequence   TEXT,
    scope_shot       TEXT,
    scope_asset      TEXT,
    scope_department TEXT,
    scope_task       TEXT,

    -- ── Error detail (NULL when status != 'error') ────────────────────────────
    error_code      TEXT,
    error_message   TEXT,

    -- ── Variable content (event-type-specific raw JSON) ─────────────────────
    payload         JSON,
    metrics         JSON,

    -- ── Source lineage ────────────────────────────────────────────────────────
    source_file     TEXT        NOT NULL,
    source_line     INTEGER     NOT NULL

);

-- ----------------------------------------------------------------------------
-- ingest_manifest
-- ----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ingest_manifest (
    file_path       TEXT        PRIMARY KEY,
    file_mtime      DOUBLE      NOT NULL,   -- Unix timestamp (fractional seconds)
    file_size       BIGINT      NOT NULL,   -- bytes
    ingested_at_utc TIMESTAMPTZ NOT NULL DEFAULT current_timestamp,
    event_count     INTEGER     NOT NULL,
    error_count     INTEGER     NOT NULL
);
