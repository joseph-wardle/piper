-- Silver domain view: Render job completion statistics.
--
-- One row per completed (or failed) farm render job. Unlike other event types,
-- error rows still carry metric values (as zeros rather than absent keys), so
-- avg_render_time_s, total_cpu_hours, and peak_memory_gb are never NULL.

CREATE OR REPLACE VIEW silver_render_stats_summary AS
SELECT
    event_id,
    occurred_at_utc::DATE                                       AS event_date,
    status,
    host_hostname                                               AS hostname,
    host_user                                                   AS username,
    scope_show                                                  AS show,
    scope_sequence                                              AS sequence,
    scope_shot                                                  AS shot,
    payload ->> 'job_id'                                        AS job_id,
    TRY_CAST(payload ->> 'frame_count'    AS INTEGER)           AS frame_count,
    TRY_CAST(payload ->> 'failed_frames'  AS INTEGER)           AS failed_frames,
    TRY_CAST(metrics ->> 'avg_render_time_s'  AS BIGINT)        AS avg_render_time_s,
    TRY_CAST(metrics ->> 'total_cpu_hours'    AS DOUBLE)        AS total_cpu_hours,
    TRY_CAST(metrics ->> 'peak_memory_gb'     AS DOUBLE)        AS peak_memory_gb,
    error_code,
    error_message
FROM silver_events
WHERE event_type = 'render.stats.summary';
