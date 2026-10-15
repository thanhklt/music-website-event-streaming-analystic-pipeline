{{ config(tags=['dashboard']) }}

WITH fact_rows AS (
    SELECT
        play_key,
        user_id,
        song_key,
        location_key,
        play_started_at,
        listened_seconds,
        updated_time
    FROM marts.fact_song_play
),

dash_rows AS (
    SELECT
        play_key,
        user_id,
        song_key,
        location_key,
        play_started_at,
        listened_seconds,
        updated_time
    FROM {{ ref('song_plays') }}
)

(SELECT * FROM fact_rows EXCEPT ALL SELECT * FROM dash_rows)
UNION ALL
(SELECT * FROM dash_rows EXCEPT ALL SELECT * FROM fact_rows)
