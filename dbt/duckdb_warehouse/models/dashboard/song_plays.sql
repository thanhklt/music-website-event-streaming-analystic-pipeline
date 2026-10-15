{{ config(materialized='table', schema='dashboard') }}

WITH fact_song_play AS (
    SELECT * FROM {{ ref('fact_song_play') }}
),

dim_location AS (
    SELECT * FROM {{ ref('dim_location') }}
),

dim_user AS (
    SELECT * FROM {{ ref('dim_user') }}
),

dim_song AS (
    SELECT * FROM {{ ref('dim_song') }}
)

SELECT
    f.play_key,
    f.user_id,
    f.song_key,
    f.location_key,
    f.play_started_at,
    f.listened_seconds,
    COALESCE(NULLIF(TRIM(l.location_state), ''), 'Unknown') AS state,
    COALESCE(NULLIF(TRIM(u.user_gender), ''), 'Unknown') AS gender,
    COALESCE(NULLIF(TRIM(s.song_name), ''), 'Unknown') AS song,
    COALESCE(NULLIF(TRIM(s.song_artist_name), ''), 'Unknown') AS artist,
    CAST(f.play_started_at AS DATE) AS play_date,
    CAST(EXTRACT(ISODOW FROM f.play_started_at) AS INTEGER) AS weekday,
    CAST(EXTRACT(HOUR FROM f.play_started_at) AS INTEGER) AS hour,
    f.updated_time
FROM fact_song_play f
LEFT JOIN dim_location l ON f.location_key = l.location_key
LEFT JOIN dim_user u ON f.user_key = u.user_key
LEFT JOIN dim_song s ON f.song_key = s.song_key
