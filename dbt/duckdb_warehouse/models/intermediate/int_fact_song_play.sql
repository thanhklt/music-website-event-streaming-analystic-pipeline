{{ config(
    materialized='table',
    unique_key=['user_id', 'session_id', 'item_in_session']
) }}

WITH int_fact_song_play__source AS (
    SELECT * FROM {{ ref('stg_listen_events') }}
),

int_fact_song_play__timeline AS (
    SELECT userId, sessionId, ts, itemInSession
    FROM int_fact_song_play__source

    UNION

    SELECT userId, sessionId, ts, itemInSession
    FROM {{ ref('stg_page_view_events') }}
),

int_fact_song_play__timeline_conflicts AS (
    SELECT userId, sessionId, itemInSession
    FROM int_fact_song_play__timeline
    GROUP BY userId, sessionId, itemInSession
    HAVING COUNT(*) > 1
),

int_fact_song_play__timeline_check AS (
    SELECT
        CASE
            WHEN COUNT(*) > 0 THEN error('Conflicting event timestamps in play timeline')
            ELSE TRUE
        END AS is_valid
    FROM int_fact_song_play__timeline_conflicts
),

int_fact_song_play__next_event AS (
    SELECT
        *,
        LEAD(ts) OVER (
            PARTITION BY userId, sessionId
            ORDER BY ts, itemInSession
        ) AS play_ended_at
    FROM int_fact_song_play__timeline
),

int_fact_song_play__columns AS (
    SELECT
        play.ts, play.city, play.zip, play.state, play.lon, play.lat,
        play.userId, play.artist, play.song, play.duration, play.sessionId, play.itemInSession,
        event.play_ended_at
    FROM int_fact_song_play__source AS play
    LEFT JOIN int_fact_song_play__next_event AS event
        ON play.userId = event.userId
        AND play.sessionId = event.sessionId
        AND play.ts = event.ts
        AND play.itemInSession = event.itemInSession
    CROSS JOIN int_fact_song_play__timeline_check AS timeline_check
    WHERE timeline_check.is_valid
),

int_dim_date__source AS (
    SELECT DISTINCT date_key FROM {{ ref('int_dim_date') }}
),

int_dim_time__source AS (
    SELECT DISTINCT time_key FROM {{ ref('int_dim_time') }}
),

int_dim_user__source AS (
    SELECT DISTINCT user_key, user_id, start_time, end_time
    FROM {{ ref('int_dim_user') }}
),

int_dim_song__source AS (
    SELECT DISTINCT song_key, song_artist_name, song_name, song_duration_seconds
    FROM {{ ref('int_dim_song') }}
),

int_dim_location__source AS (
    SELECT DISTINCT
        location_key, location_city, location_zip, location_state, location_lon, location_lat
    FROM {{ ref('int_dim_location') }}
),

int_dim_session__source AS (
    SELECT DISTINCT session_key, user_id, session_id, session_started_time, session_number
    FROM {{ ref('int_dim_session') }}
),

int_fact_song_play__retrieve AS (
    SELECT
        play.userId AS user_id,
        play.sessionId AS session_id,
        play.itemInSession AS item_in_session,
        usr.user_key,
        song.song_key,
        session.session_key,
        location.location_key,
        date.date_key AS play_date_key,
        time.time_key AS play_time_key,
        play.ts AS play_started_at,
        play.play_ended_at,
        EPOCH(play.play_ended_at - play.ts) AS listened_seconds
    FROM int_fact_song_play__columns AS play
    LEFT JOIN int_dim_date__source AS date
        ON CAST(play.ts AS DATE) = date.date_key
    LEFT JOIN int_dim_time__source AS time
        ON CAST(
            EXTRACT(HOUR FROM play.ts) * 3600
            + EXTRACT(MINUTE FROM play.ts) * 60
            + EXTRACT(SECOND FROM DATE_TRUNC('second', play.ts))
            AS INTEGER
        ) = time.time_key
    LEFT JOIN int_dim_user__source AS usr
        ON play.userId = usr.user_id
        AND play.ts >= usr.start_time
        AND play.ts < usr.end_time
    LEFT JOIN int_dim_song__source AS song
        ON play.artist IS NOT DISTINCT FROM song.song_artist_name
        AND play.song IS NOT DISTINCT FROM song.song_name
        AND play.duration IS NOT DISTINCT FROM song.song_duration_seconds
    LEFT JOIN int_dim_location__source AS location
        ON play.city IS NOT DISTINCT FROM location.location_city
        AND play.zip IS NOT DISTINCT FROM location.location_zip
        AND play.state IS NOT DISTINCT FROM location.location_state
        AND play.lon IS NOT DISTINCT FROM location.location_lon
        AND play.lat IS NOT DISTINCT FROM location.location_lat
    LEFT JOIN int_dim_session__source AS session
        ON play.userId = session.user_id
        AND play.sessionId = session.session_id
        AND play.ts = session.session_started_time
        AND play.itemInSession = session.session_number
),

int_fact_song_play__gen_key AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['user_id', 'session_id', 'item_in_session']) }}
         AS play_key,
        *
    FROM int_fact_song_play__retrieve
)

SELECT * FROM int_fact_song_play__gen_key
