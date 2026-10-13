{{ config(
    materialized='table',
    unique_key=['user_id', 'session_id', 'item_in_session']
) }}

WITH int_fact_song_play__source AS (
    SELECT
        ts,
        sessionId,
        page,
        level,
        itemInSession,
        city,
        zip,
        state,
        lon,
        lat,
        userId,
        artist,
        song,
        duration
    FROM {{ ref('stg_page_view_events') }}
),

int_fact_song_play__timeline AS (
    SELECT
        *,
        LEAD(ts) OVER (
            PARTITION BY userId, sessionId
            ORDER BY ts, itemInSession
        ) AS next_event_at
    FROM int_fact_song_play__source
),

int_fact_song_play__plays AS (
    SELECT
        userId AS user_id,
        sessionId AS session_id,
        itemInSession AS item_in_session,
        artist AS song_artist_name,
        song AS song_name,
        duration AS song_duration_seconds,
        city AS location_city,
        zip AS location_zip,
        state AS location_state,
        lon AS location_lon,
        lat AS location_lat,
        CAST(ts AS DATE) AS play_date_key,
        ts AS play_started_at,
        next_event_at AS play_ended_at,
        DATE_DIFF('millisecond', ts, next_event_at) / 1000.0 AS listened_seconds,
        CAST(
            ROW_NUMBER() OVER (
                PARTITION BY userId, sessionId
                ORDER BY ts, itemInSession
            ) AS INTEGER
        ) AS song_position_in_session,
        level AS user_level_at_play
    FROM int_fact_song_play__timeline
    WHERE page = 'NextSong'
),

-- Song and time dimensions currently retain repeated source observations.
-- Collapse identical lookup rows so a play is not multiplied by the joins.
int_fact_song_play__song_lookup AS (
    SELECT DISTINCT
        song_key,
        song_artist_name,
        song_name,
        song_duration_seconds
    FROM {{ ref('int_dim_song') }}
),

int_fact_song_play__session_lookup AS (
    SELECT DISTINCT
        session_key,
        user_id,
        session_id,
        session_started_time
    FROM {{ ref('int_dim_session') }}
),

int_fact_song_play__time_lookup AS (
    SELECT DISTINCT time_key
    FROM {{ ref('int_dim_time') }}
),

int_fact_song_play__dimension_keys AS (
    SELECT
        plays.*,
        users.user_key,
        songs.song_key,
        sessions.session_key,
        locations.location_key,
        dates.date_key AS resolved_date_key,
        times.time_key AS resolved_time_key
    FROM int_fact_song_play__plays AS plays
    LEFT JOIN {{ ref('int_dim_user') }} AS users
        ON plays.user_id = users.user_id
       AND plays.play_started_at >= users.start_time
       AND plays.play_started_at < users.end_time
    LEFT JOIN int_fact_song_play__song_lookup AS songs
        ON plays.song_artist_name IS NOT DISTINCT FROM songs.song_artist_name
       AND plays.song_name IS NOT DISTINCT FROM songs.song_name
       AND plays.song_duration_seconds IS NOT DISTINCT FROM songs.song_duration_seconds
    LEFT JOIN int_fact_song_play__session_lookup AS sessions
        ON plays.user_id = sessions.user_id
       AND plays.session_id = sessions.session_id
       AND plays.play_started_at = sessions.session_started_time
    LEFT JOIN {{ ref('int_dim_location') }} AS locations
        ON plays.location_city IS NOT DISTINCT FROM locations.location_city
       AND plays.location_zip IS NOT DISTINCT FROM locations.location_zip
       AND plays.location_state IS NOT DISTINCT FROM locations.location_state
       AND plays.location_lon IS NOT DISTINCT FROM locations.location_lon
       AND plays.location_lat IS NOT DISTINCT FROM locations.location_lat
    LEFT JOIN {{ ref('int_dim_date') }} AS dates
        ON plays.play_date_key = dates.date_key
    LEFT JOIN int_fact_song_play__time_lookup AS times
        ON plays.play_started_at = times.time_key
),

int_fact_song_play__with_key AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'user_id', 'session_id', 'item_in_session'
        ]) }} AS play_key,
        fact.user_id,
        fact.session_id,
        fact.item_in_session,
        fact.user_key,
        fact.song_key,
        fact.session_key,
        fact.location_key,
        fact.resolved_date_key AS play_date_key,
        fact.resolved_time_key AS play_time_key,
        fact.play_started_at,
        fact.play_ended_at,
        fact.listened_seconds,
        fact.song_position_in_session,
        fact.user_level_at_play
    FROM int_fact_song_play__dimension_keys AS fact
)

SELECT * FROM int_fact_song_play__with_key
