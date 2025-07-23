CREATE OR REPLACE TASK SAMBA_TV.MODELLING.TASK_ROOT_KICKOFF
    WAREHOUSE = 'TABLEAU_WH'
    SCHEDULE = 'USING CRON 0 5 * * * UTC'
AS
    SELECT 'Kicking off the Samba TV daily pipeline...';

-- Child Task 1: Update Panel Windows (using MERGE)
CREATE OR REPLACE TASK samba_tv.modelling.task_update_panel_windows
    WAREHOUSE = 'TABLEAU_WH'
    AFTER SAMBA_TV.MODELLING.TASK_ROOT_KICKOFF
AS
    MERGE INTO samba_tv.modelling.panel_windows t
    USING (
        -- This subquery finds the new panel mondays
        SELECT DISTINCT
            metadata_date AS panel_monday,
            DATEADD(day, -28, metadata_date) AS window_start,
            DATEADD(day, -1, metadata_date) AS window_end
        FROM samba_tv.raw.geo_weights
        WHERE
            DAYOFWEEK(metadata_date) = 1
            AND metadata_date > (SELECT last_successful_run_date FROM samba_tv.modelling.task_control WHERE task_name = 'panel_windows_update')
    ) s ON t.panel_monday = s.panel_monday
    WHEN NOT MATCHED THEN
        INSERT (panel_monday, window_start, window_end, created_at)
        VALUES (s.panel_monday, s.window_start, s.window_end, CURRENT_TIMESTAMP());

-- Child Task 2: Update Raw Events (using MERGE)
CREATE OR REPLACE TASK samba_tv.modelling.task_update_raw_events
    WAREHOUSE = 'TABLEAU_WH'
    AFTER SAMBA_TV.MODELLING.TASK_ROOT_KICKOFF
AS
    MERGE INTO samba_tv.modelling.raw_viewing_events t
    USING (
        -- Gather all new events from both sources
        SELECT 'ACR' AS source_table, smba_id, content_type, application, title, content_id, episode_title, season, episode, network, duration, exposure_start_ts, exposure_end_ts, dma, zip, metadata_date, yyyy, mm, dd
        FROM samba_tv.raw.acr
        WHERE metadata_date > (SELECT last_successful_run_date FROM samba_tv.modelling.task_control WHERE task_name = 'raw_events_update')
        UNION ALL
        SELECT 'STB' AS source_table, smba_id, content_type, NULL, title, content_id, episode_title, season, episode, network, duration, exposure_start_ts, exposure_end_ts, dma, zip, metadata_date, yyyy, mm, dd
        FROM samba_tv.raw.stb
        WHERE metadata_date > (SELECT last_successful_run_date FROM samba_tv.modelling.task_control WHERE task_name = 'raw_events_update')
    ) s ON t.smba_id = s.smba_id AND t.exposure_start_ts = s.exposure_start_ts -- Use smba_id and start timestamp as the unique key for an event
    WHEN NOT MATCHED THEN
        INSERT (source_table, smba_id, content_type, application, title, content_id, episode_title, season, episode, network, duration, exposure_start_ts, exposure_end_ts, dma, zip, metadata_date, yyyy, mm, dd, created_at)
        VALUES (s.source_table, s.smba_id, s.content_type, s.application, s.title, s.content_id, s.episode_title, s.season, s.episode, s.network, s.duration, s.exposure_start_ts, s.exposure_end_ts, s.dma, s.zip, s.metadata_date, s.yyyy, s.mm, s.dd, CURRENT_TIMESTAMP());

-- Child Task 3: Update Weighted Events (using MERGE)
CREATE OR REPLACE TASK samba_tv.modelling.task_update_weighted_events
    WAREHOUSE = 'TABLEAU_WH'
    AFTER samba_tv.modelling.task_update_panel_windows, samba_tv.modelling.task_update_raw_events
AS
    MERGE INTO samba_tv.modelling.weighted_events t
    USING (
        WITH new_raw_events AS (
            SELECT *
            FROM samba_tv.modelling.raw_viewing_events
            WHERE metadata_date > (SELECT last_successful_run_date FROM samba_tv.modelling.task_control WHERE task_name = 'raw_events_update')
        ),
        events_with_panel AS (
            SELECT
                e.*,
                (SELECT MAX(pw.panel_monday)
                 FROM samba_tv.modelling.panel_windows pw
                 WHERE e.metadata_date BETWEEN pw.window_start AND pw.window_end) AS panel_monday
            FROM new_raw_events e
        )
        SELECT
            e.source_table, e.smba_id, e.dma, e.content_type, e.application, e.title,
            e.content_id, e.episode_title, e.season, e.episode, e.network, e.duration,
            e.exposure_start_ts, e.exposure_end_ts, e.metadata_date, e.yyyy, e.mm, e.dd,
            e.panel_monday, g.hh_id, g.geo_weight AS panel_weight
        FROM events_with_panel e
        LEFT JOIN samba_tv.raw.geo_weights g
            ON g.smba_id = e.smba_id
            AND g.metadata_date = e.panel_monday
        WHERE e.panel_monday IS NOT NULL
          AND g.hh_id IS NOT NULL
          AND g.geo_weight IS NOT NULL
    ) s ON t.smba_id = s.smba_id AND t.exposure_start_ts = s.exposure_start_ts -- Use smba_id and start timestamp as the unique key
    WHEN NOT MATCHED THEN
        INSERT (source_table, smba_id, dma, content_type, application, title, content_id, episode_title, season, episode, network, duration, exposure_start_ts, exposure_end_ts, metadata_date, yyyy, mm, dd, panel_monday, hh_id, panel_weight, created_at)
        VALUES (s.source_table, s.smba_id, s.dma, s.content_type, s.application, s.title, s.content_id, s.episode_title, s.season, s.episode, s.network, s.duration, s.exposure_start_ts, s.exposure_end_ts, s.metadata_date, s.yyyy, s.mm, s.dd, s.panel_monday, s.hh_id, s.panel_weight, CURRENT_TIMESTAMP());

-- Child Task 4: Update Viewing Sessions (using DELETE/INSERT, which is correct for this logic)
CREATE OR REPLACE TASK samba_tv.modelling.task_update_viewing_sessions
    WAREHOUSE = 'TABLEAU_WH'
    AFTER samba_tv.modelling.task_update_weighted_events
AS
BEGIN
    CREATE OR REPLACE TEMP TABLE affected_sessions_keys AS
    SELECT DISTINCT hh_id, title, content_id
    FROM samba_tv.modelling.weighted_events
    WHERE metadata_date > (SELECT last_successful_run_date FROM samba_tv.modelling.task_control WHERE task_name = 'raw_events_update');

    DELETE FROM samba_tv.modelling.viewing_sessions
    WHERE (hh_id, title, content_id) IN (SELECT hh_id, title, content_id FROM affected_sessions_keys);

    INSERT INTO samba_tv.modelling.viewing_sessions (hh_id, dma, source_table, content_type, application, title, content_id, episode_title, season, episode, network, panel_monday, session_id, session_start_ts, session_end_ts, total_duration, panel_weight, created_at)
    WITH events_for_affected_sessions AS (
        SELECT w.*
        FROM samba_tv.modelling.weighted_events AS w
        INNER JOIN affected_sessions_keys AS k
            ON w.hh_id = k.hh_id AND w.title = k.title AND w.content_id = k.content_id
    ),
    ordered_events AS (
        SELECT *, exposure_start_ts - LAG(exposure_end_ts) OVER (PARTITION BY hh_id, title, content_id ORDER BY exposure_start_ts) AS gap_seconds
        FROM events_for_affected_sessions
    ),
    session_markers AS (
        SELECT *, CASE WHEN gap_seconds IS NULL OR gap_seconds > 300 THEN 1 ELSE 0 END AS session_break
        FROM ordered_events
    ),
    session_ids AS (
        SELECT *, SUM(session_break) OVER (PARTITION BY hh_id, title, content_id ORDER BY exposure_start_ts) AS session_id
        FROM session_markers
    )
    SELECT hh_id, dma, source_table, content_type, application, title, content_id, episode_title, season, episode, network, panel_monday, session_id, MIN(exposure_start_ts), MAX(exposure_end_ts), SUM(duration), panel_weight, CURRENT_TIMESTAMP()
    FROM session_ids
    GROUP BY dma, hh_id, source_table, content_type, application, title, content_id, episode_title, season, episode, network, panel_monday, session_id, panel_weight
    HAVING SUM(duration) >= 180;

    RETURN 'Session update complete.';
END;

-- Final Child Task 5: Update the control table watermarks
CREATE OR REPLACE TASK samba_tv.modelling.task_update_control_table
    WAREHOUSE = 'TABLEAU_WH'
    AFTER samba_tv.modelling.task_update_viewing_sessions
AS
BEGIN
    let acr_max_date DATE := (SELECT MAX(metadata_date) FROM samba_tv.raw.acr);
    let stb_max_date DATE := (SELECT MAX(metadata_date) FROM samba_tv.raw.stb);
    let geo_max_date DATE := (SELECT MAX(metadata_date) FROM samba_tv.raw.geo_weights);

    UPDATE samba_tv.modelling.task_control
    SET last_successful_run_date = LEAST(COALESCE(:acr_max_date, '1900-01-01'), COALESCE(:stb_max_date, '1900-01-01'))
    WHERE task_name = 'raw_events_update';

    UPDATE samba_tv.modelling.task_control
    SET last_successful_run_date = COALESCE(:geo_max_date, '1900-01-01')
    WHERE task_name = 'panel_windows_update';

    RETURN 'Control table watermarks updated successfully.';
END;


ALTER TASK SAMBA_TV.MODELLING.TASK_UPDATE_CONTROL_TABLE RESUME;
ALTER TASK SAMBA_TV.MODELLING.TASK_UPDATE_VIEWING_SESSIONS RESUME;
ALTER TASK SAMBA_TV.MODELLING.TASK_UPDATE_WEIGHTED_EVENTS RESUME;
ALTER TASK SAMBA_TV.MODELLING.TASK_UPDATE_RAW_EVENTS RESUME;
ALTER TASK SAMBA_TV.MODELLING.TASK_UPDATE_PANEL_WINDOWS RESUME;

ALTER TASK SAMBA_TV.MODELLING.TASK_ROOT_KICKOFF RESUME;

-- You can check the status of all tasks
SHOW TASKS IN SCHEMA samba_tv.modelling;