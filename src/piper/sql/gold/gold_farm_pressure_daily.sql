-- Gold model: daily Tractor farm utilisation pressure.
--
-- Aggregates farm health snapshots to show average and peak blade/job
-- occupancy per day.  tractor.farm.snapshot emits status='info' on success,
-- so we include both 'success' and 'info'.  Error snapshots are excluded
-- because their counters are all-zero placeholders.

CREATE OR REPLACE VIEW gold_farm_pressure_daily AS
SELECT
    event_date,
    COUNT(*)                                AS snapshot_count,
    ROUND(AVG(running_jobs),   2)           AS avg_active_jobs,
    MAX(running_jobs)                       AS peak_active_jobs,
    ROUND(AVG(waiting_jobs),   2)           AS avg_waiting_jobs,
    MAX(waiting_jobs)                       AS peak_waiting_jobs,
    ROUND(AVG(active_blades),  2)           AS avg_active_blades,
    MAX(active_blades)                      AS peak_active_blades
FROM silver_tractor_farm_snapshot
WHERE status IN ('success', 'info')
GROUP BY event_date
ORDER BY event_date;
