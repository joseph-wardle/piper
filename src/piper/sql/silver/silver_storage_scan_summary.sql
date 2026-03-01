-- Silver domain view: Storage scan roll-up summaries.
--
-- One row per full-storage scan run. bucket_count and total_file_count are
-- present on all rows (as zeros on error). total_size_bytes and
-- scan_duration_s are NULL on error rows.

CREATE OR REPLACE VIEW silver_storage_scan_summary AS
SELECT
    event_id,
    occurred_at_utc::DATE                                       AS event_date,
    status,
    host_hostname                                               AS hostname,
    host_user                                                   AS username,
    TRY_CAST(payload ->> 'bucket_count'      AS INTEGER)        AS bucket_count,
    TRY_CAST(payload ->> 'total_file_count'  AS BIGINT)         AS total_file_count,
    TRY_CAST(metrics ->> 'total_size_bytes'  AS BIGINT)         AS total_size_bytes,
    TRY_CAST(metrics ->> 'scan_duration_s'   AS DOUBLE)         AS scan_duration_s,
    error_code,
    error_message
FROM silver_events
WHERE event_type = 'storage.scan.summary';
