-- Create a table of all panel Mondays and their coverage windows
CREATE OR REPLACE TABLE samba_tv.modelling.panel_windows AS
SELECT DISTINCT
    metadata_date                           AS panel_monday,
    DATEADD(day, -28, metadata_date)        AS window_start,
    DATEADD(day, -1, metadata_date)         AS window_end,
    CURRENT_TIMESTAMP()                     AS created_at
FROM samba_tv.raw.geo_weights
WHERE DAYOFWEEK(metadata_date) = 1  -- Ensure it's actually Monday
ORDER BY panel_monday;

-- Add indexes for better performance
ALTER TABLE samba_tv.modelling.panel_windows ADD PRIMARY KEY (panel_monday);

-- Create a staging table for raw viewing events (can be filtered later)
CREATE OR REPLACE TABLE samba_tv.modelling.raw_viewing_events AS
SELECT
    'ACR' AS source_table,
    smba_id,
    content_type,
    application,
    title,
    content_id,
    episode_title,
    season,
    episode,
    network,
    duration,
    exposure_start_ts,
    exposure_end_ts,
    dma,
    zip,
    metadata_date,
    yyyy,
    mm,
    dd,
    CURRENT_TIMESTAMP() AS created_at
FROM samba_tv.raw.acr

UNION ALL

SELECT
    'STB' AS source_table,
    smba_id,
    content_type,
    NULL AS application,  -- STB doesn't have application
    title,
    content_id,
    episode_title,
    season,
    episode,
    network,
    duration,
    exposure_start_ts,
    exposure_end_ts,
    dma,
    zip,
    metadata_date,
    yyyy,
    mm,
    dd,
    CURRENT_TIMESTAMP() AS created_at
FROM samba_tv.raw.stb;

CREATE OR REPLACE TABLE samba_tv.modelling.weighted_events AS
WITH events_with_panel AS (
    SELECT
        e.source_table,
        e.smba_id,
        e.dma,
        e.content_type,
        e.application,
        e.title,
        e.content_id,
        e.episode_title,
        e.season,
        e.episode,
        e.network,
        e.duration,
        e.exposure_start_ts,
        e.exposure_end_ts,
        e.metadata_date,
        e.yyyy,
        e.mm,
        e.dd,
        (SELECT MAX(pw.panel_monday)
         FROM samba_tv.modelling.panel_windows pw
         WHERE e.metadata_date BETWEEN pw.window_start AND pw.window_end) AS panel_monday
    FROM samba_tv.modelling.raw_viewing_events e
)
SELECT
    e.source_table,
    e.smba_id,
    e.dma,
    e.content_type,
    e.application,
    e.title,
    e.content_id,
    e.episode_title,
    e.season,
    e.episode,
    e.network,
    e.duration,
    e.exposure_start_ts,
    e.exposure_end_ts,
    e.metadata_date,
    e.yyyy,
    e.mm,
    e.dd,
    e.panel_monday,
    g.hh_id,
    g.geo_weight AS panel_weight,
    CURRENT_TIMESTAMP() AS created_at
FROM events_with_panel e
LEFT JOIN samba_tv.raw.geo_weights g
    ON g.smba_id = e.smba_id
    AND g.metadata_date = e.panel_monday
WHERE e.panel_monday IS NOT NULL  -- Ensure we have a valid panel
  AND g.hh_id IS NOT NULL         -- Ensure we have a valid household
  AND g.geo_weight IS NOT NULL;   -- Ensure we have a valid weight

CREATE OR REPLACE TABLE samba_tv.modelling.viewing_sessions AS
WITH ordered_events AS (
    SELECT
        hh_id,
        dma,
        source_table,
        content_type,
        application,
        title,
        content_id,
        episode_title,
        season,
        episode,
        network,
        panel_weight,
        duration,
        exposure_start_ts,
        exposure_end_ts,
        metadata_date,
        panel_monday,
        -- Calculate gap from previous event
        exposure_start_ts - LAG(exposure_end_ts) OVER (
            PARTITION BY hh_id, title, content_id
            ORDER BY exposure_start_ts
        ) AS gap_seconds
    FROM samba_tv.modelling.weighted_events
),

session_markers AS (
    SELECT
        *,
        -- Mark session breaks (NULL or > 300 seconds gap)
        CASE WHEN gap_seconds IS NULL OR gap_seconds > 300 THEN 1 ELSE 0 END AS session_break
    FROM ordered_events
),

session_ids AS (
    SELECT
        *,
        -- Generate session IDs using cumulative sum of breaks
        SUM(session_break) OVER (
            PARTITION BY hh_id, title, content_id
            ORDER BY exposure_start_ts
        ) AS session_id
    FROM session_markers
)

-- Aggregate events within each session
SELECT
    hh_id,
    dma,
    source_table,
    content_type,
    application,
    title,
    content_id,
    episode_title,
    season,
    episode,
    network,
    panel_monday,
    session_id,
    MIN(exposure_start_ts) AS session_start_ts,
    MAX(exposure_end_ts) AS session_end_ts,
    SUM(duration) AS total_duration,
    panel_weight,
    CURRENT_TIMESTAMP() AS created_at
FROM session_ids
GROUP BY
    dma, hh_id, source_table, content_type, application, title, content_id, 
    episode_title, season, episode, network, panel_monday, session_id, panel_weight
HAVING total_duration >= 180;  -- Filter for sessions >= 3 minutes

-- Create a view that's ready for easy aggregation
CREATE OR REPLACE VIEW samba_tv.modelling.v_audience_metrics AS
SELECT
    title,
    content_id,
    episode_title,
    season,
    episode,
    network,
    content_type,
    application,
    source_table,
    panel_monday,
    dma,
    hh_id,
    panel_weight,
    -- A single session counts as one view with this weight
    panel_weight AS weighted_view,
    -- For reach, this HH will be counted once with this weight
    panel_weight AS weighted_hh,
    -- Duration weighted by panel weight
    total_duration * panel_weight AS weighted_duration,
    -- Session start/end for time-based analysis
    session_start_ts,
    session_end_ts,
    total_duration,
    TO_DATE(TO_TIMESTAMP(session_start_ts))   AS metadata_date
FROM samba_tv.modelling.viewing_sessions;

-- Snowflake Tasks Automation
CREATE OR REPLACE TABLE samba_tv.modelling.task_control (
    task_name VARCHAR,
    last_successful_run_date DATE
);

INSERT INTO samba_tv.modelling.task_control (task_name, last_successful_run_date)
VALUES
    ('raw_events_update', current_date()),
    ('panel_windows_update', current_date());