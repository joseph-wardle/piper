-- Silver domain view: Tractor farm health snapshots.
--
-- One row per periodic farm health poll. Payload counters (active_jobs,
-- active_blades, errored_jobs) are present even on error rows (as zeros).
-- snapshot_duration_ms is NULL on error rows.

CREATE OR REPLACE VIEW silver_tractor_farm_snapshot AS
SELECT
    event_id,
    occurred_at_utc::DATE                                       AS event_date,
    status,
    host_hostname                                               AS hostname,
    host_user                                                   AS username,
    TRY_CAST(payload ->> 'active_jobs'    AS INTEGER)           AS active_jobs,
    TRY_CAST(payload ->> 'active_blades'  AS INTEGER)           AS active_blades,
    TRY_CAST(payload ->> 'errored_jobs'   AS INTEGER)           AS errored_jobs,
    TRY_CAST(metrics ->> 'snapshot_duration_ms' AS BIGINT)      AS snapshot_duration_ms,
    error_code,
    error_message
FROM silver_events
WHERE event_type = 'tractor.farm.snapshot';
