-- Gold model: daily pipeline data quality summary.
--
-- Provides a cross-domain view of event volume and error rate per day.
-- High error_rate_pct values or sudden volume drops warrant investigation.
-- active_users and active_hosts give a quick sense of pipeline breadth.

CREATE OR REPLACE VIEW gold_data_quality_daily AS
SELECT
    occurred_at_utc::DATE                                                   AS event_date,
    COUNT(*)                                                                AS total_events,
    SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END)                      AS error_events,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                                                       AS error_rate_pct,
    COUNT(DISTINCT host_user)                                               AS active_users,
    COUNT(DISTINCT host_hostname)                                           AS active_hosts,
    COUNT(DISTINCT event_type)                                              AS active_event_types
FROM silver_events
GROUP BY event_date
ORDER BY event_date;
