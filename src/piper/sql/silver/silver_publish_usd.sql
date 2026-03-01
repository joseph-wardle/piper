-- Silver domain view: USD publish events.
--
-- One row per publish attempt across all five publish event types.
-- Payload fields (output_path) and metrics fields (duration_ms,
-- output_size_bytes) are extracted from JSON; they are NULL for error rows
-- where the publish did not complete.

CREATE OR REPLACE VIEW silver_publish_usd AS
SELECT
    event_id,
    event_type,
    occurred_at_utc::DATE                                    AS event_date,
    status,
    pipeline_dcc                                             AS dcc,
    host_hostname                                            AS hostname,
    host_user                                                AS username,
    scope_show                                               AS show,
    scope_sequence                                           AS sequence,
    scope_shot                                               AS shot,
    scope_asset                                              AS asset,
    scope_department                                         AS department,
    payload ->> 'output_path'                                AS output_path,
    TRY_CAST(metrics ->> 'duration_ms'        AS BIGINT)    AS duration_ms,
    TRY_CAST(metrics ->> 'output_size_bytes'  AS BIGINT)    AS output_size_bytes,
    error_code,
    error_message
FROM silver_events
WHERE event_type IN (
    'publish.asset.usd',
    'publish.anim.usd',
    'publish.camera.usd',
    'publish.customanim.usd',
    'publish.previs_asset.usd'
);
