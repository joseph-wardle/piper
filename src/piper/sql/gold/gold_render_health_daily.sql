-- Gold model: daily render job health by show / sequence.
--
-- Unlike other domain models, render error rows carry zero metrics (not absent
-- keys), so avg_render_time_s and total_cpu_hours are always non-NULL.

CREATE OR REPLACE VIEW gold_render_health_daily AS
SELECT
    event_date,
    "show",
    sequence,
    COUNT(*)                                                                AS total_jobs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)                    AS success_count,
    SUM(CASE WHEN status = 'error'   THEN 1 ELSE 0 END)                    AS error_count,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                                                       AS success_rate_pct,
    SUM(frame_count)                                                        AS total_frames,
    SUM(failed_frames)                                                      AS total_failed_frames,
    ROUND(AVG(avg_render_time_s), 2)                                        AS avg_render_time_s,
    ROUND(SUM(total_cpu_hours), 4)                                          AS total_cpu_hours,
    MAX(peak_memory_gb)                                                     AS peak_memory_gb
FROM silver_render_stats_summary
GROUP BY event_date, "show", sequence
ORDER BY event_date, "show", sequence;
