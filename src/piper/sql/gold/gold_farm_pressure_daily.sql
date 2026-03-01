-- Gold model: daily Tractor farm utilisation pressure.
--
-- Aggregates successful farm health snapshots to show average and peak
-- blade/job occupancy per day.  Error snapshots are excluded because their
-- counters are all-zero placeholders, not real utilisation readings.

CREATE OR REPLACE VIEW gold_farm_pressure_daily AS
SELECT
    event_date,
    COUNT(*)                                AS snapshot_count,
    ROUND(AVG(active_jobs),    2)           AS avg_active_jobs,
    MAX(active_jobs)                        AS peak_active_jobs,
    ROUND(AVG(active_blades),  2)           AS avg_active_blades,
    MAX(active_blades)                      AS peak_active_blades,
    MAX(errored_jobs)                       AS peak_errored_jobs
FROM silver_tractor_farm_snapshot
WHERE status = 'success'
GROUP BY event_date
ORDER BY event_date;
