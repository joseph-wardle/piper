-- Gold model: daily DCC tool reliability by event_type.
--
-- success_rate_pct tracks how reliably each tool type completes without error.
-- avg_duration_ms covers success rows only (NULLs from error rows are
-- excluded by AVG).

CREATE OR REPLACE VIEW gold_tool_reliability_daily AS
SELECT
    event_date,
    event_type,
    COUNT(*)                                                                AS total_invocations,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)                    AS success_count,
    SUM(CASE WHEN status = 'error'   THEN 1 ELSE 0 END)                    AS error_count,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                                                       AS success_rate_pct,
    ROUND(AVG(CASE WHEN status = 'success' THEN duration_ms END), 2)       AS avg_duration_ms
FROM silver_tool_events
GROUP BY event_date, event_type
ORDER BY event_date, event_type;
