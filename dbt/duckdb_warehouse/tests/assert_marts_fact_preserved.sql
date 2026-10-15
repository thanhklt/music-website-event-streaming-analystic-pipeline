WITH source_rows AS (
    SELECT play_key, user_id, session_id, item_in_session, play_started_at, play_ended_at, listened_seconds FROM {{ ref('int_fact_song_play') }}
),
target_rows AS (
    SELECT play_key, user_id, session_id, item_in_session, play_started_at, play_ended_at, listened_seconds FROM {{ ref('fact_song_play') }}
)
(SELECT * FROM source_rows EXCEPT ALL SELECT * FROM target_rows)
UNION ALL
(SELECT * FROM target_rows EXCEPT ALL SELECT * FROM source_rows)
