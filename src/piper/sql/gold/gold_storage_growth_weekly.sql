-- Gold model: weekly storage growth per bucket.
--
-- Uses arg_max to take the latest observed size within each week so that
-- a bucket that shrinks mid-week is represented correctly.  Error scan rows
-- are excluded because their file_count and size are zero placeholders.

CREATE OR REPLACE VIEW gold_storage_growth_weekly AS
SELECT
    DATE_TRUNC('week', event_date)          AS week_start,
    bucket,
    COUNT(*)                                AS scan_count,
    ARG_MAX(total_size_bytes, event_date)   AS latest_size_bytes,
    ARG_MAX(file_count,       event_date)   AS latest_file_count,
    ARG_MAX(dir_count,        event_date)   AS latest_dir_count
FROM silver_storage_scan_bucket
WHERE status = 'success'
GROUP BY week_start, bucket
ORDER BY week_start, bucket;
