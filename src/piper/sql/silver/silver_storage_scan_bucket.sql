-- Silver domain view: Per-bucket storage scan results.
--
-- One row per bucket scanned. bucket, root_path, file_count, and dir_count
-- are present on all rows (as zeros on error). total_size_bytes and
-- scan_duration_s are NULL on error rows.

CREATE OR REPLACE VIEW silver_storage_scan_bucket AS
SELECT
    event_id,
    occurred_at_utc::DATE                                       AS event_date,
    status,
    host_hostname                                               AS hostname,
    host_user                                                   AS username,
    payload ->> 'bucket'                                        AS bucket,
    payload ->> 'root_path'                                     AS root_path,
    TRY_CAST(payload ->> 'file_count'      AS BIGINT)           AS file_count,
    TRY_CAST(payload ->> 'dir_count'       AS BIGINT)           AS dir_count,
    TRY_CAST(metrics ->> 'total_size_bytes' AS BIGINT)          AS total_size_bytes,
    TRY_CAST(metrics ->> 'scan_duration_s'  AS DOUBLE)          AS scan_duration_s,
    error_code,
    error_message
FROM silver_events
WHERE event_type = 'storage.scan.bucket';
