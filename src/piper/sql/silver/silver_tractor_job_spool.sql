-- Silver domain view: Tractor job spool events.
--
-- One row per farm job submission. Scope fields (show, sequence, shot) are
-- always present on spool events. spool_duration_ms is NULL on error rows.

CREATE OR REPLACE VIEW silver_tractor_job_spool AS
SELECT
    event_id,
    occurred_at_utc::DATE                                       AS event_date,
    status,
    host_hostname                                               AS hostname,
    host_user                                                   AS username,
    scope_show                                                  AS show,
    scope_sequence                                              AS sequence,
    scope_shot                                                  AS shot,
    payload ->> 'job_title'                                     AS job_title,
    TRY_CAST(payload ->> 'blade_count'     AS INTEGER)          AS blade_count,
    TRY_CAST(payload ->> 'priority'        AS INTEGER)          AS priority,
    TRY_CAST(metrics ->> 'spool_duration_ms' AS BIGINT)         AS spool_duration_ms,
    error_code,
    error_message
FROM silver_events
WHERE event_type = 'tractor.job.spool';
