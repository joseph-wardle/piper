-- Silver domain view: interactive tool events.
--
-- One row per tool invocation across all eight tool event types.
-- duration_ms is COALESCED from both 'duration_ms' and 'launch_duration_ms'
-- so that dcc.launch events (which use the latter key) are handled uniformly.

CREATE OR REPLACE VIEW silver_tool_events AS
SELECT
    event_id,
    event_type,
    occurred_at_utc::DATE                                       AS event_date,
    status,
    pipeline_dcc                                                AS dcc,
    host_hostname                                               AS hostname,
    host_user                                                   AS username,
    scope_show                                                  AS show,
    scope_sequence                                              AS sequence,
    scope_shot                                                  AS shot,
    scope_asset                                                 AS asset,
    scope_department                                            AS department,
    COALESCE(
        TRY_CAST(metrics ->> 'duration_ms'        AS BIGINT),
        TRY_CAST(metrics ->> 'launch_duration_ms' AS BIGINT)
    )                                                           AS duration_ms,
    error_code,
    error_message
FROM silver_events
WHERE event_type IN (
    'dcc.launch',
    'file.open',
    'file.create',
    'shot.setup',
    'playblast.create',
    'build.houdini.component',
    'texture.export.substance',
    'texture.convert.tex'
);
