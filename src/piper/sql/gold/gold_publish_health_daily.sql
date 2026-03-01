-- Gold model: daily USD publish health by show / sequence / event_type.
--
-- success_rate_pct is the fraction of publishes that completed without error.
-- avg_duration_ms and total_output_bytes cover success rows only (NULLs excluded
-- by AVG/SUM aggregation on NULL values).

CREATE OR REPLACE VIEW gold_publish_health_daily AS
SELECT
    event_date,
    "show",
    sequence,
    event_type,
    COUNT(*)                                                                AS total_publishes,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END)                    AS success_count,
    SUM(CASE WHEN status = 'error'   THEN 1 ELSE 0 END)                    AS error_count,
    ROUND(
        100.0 * SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                                                       AS success_rate_pct,
    ROUND(AVG(CASE WHEN status = 'success' THEN duration_ms END), 2)       AS avg_duration_ms,
    SUM(output_size_bytes)                                                  AS total_output_bytes
FROM silver_publish_usd
GROUP BY event_date, "show", sequence, event_type
ORDER BY event_date, "show", sequence, event_type;
