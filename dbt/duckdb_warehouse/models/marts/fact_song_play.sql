{{ config(materialized='incremental', unique_key='play_key', incremental_strategy='delete+insert', on_schema_change='fail') }}

-- depends_on: {{ ref('dim_user') }}
-- depends_on: {{ ref('dim_song') }}
-- depends_on: {{ ref('dim_session') }}
-- depends_on: {{ ref('dim_location') }}
-- depends_on: {{ ref('dim_date') }}
-- depends_on: {{ ref('dim_time') }}

WITH fact_song_play__source AS (
    SELECT * FROM {{ ref('int_fact_song_play') }}
),

fact_song_play__source_check AS (
    SELECT CASE WHEN COUNT(*) = COUNT(DISTINCT play_key) THEN TRUE
        ELSE error('NULL or duplicate source play_key') END AS is_valid
    FROM fact_song_play__source
),

fact_song_play__coalesce_foreign_keys AS (
    SELECT
        play_key,
        user_id,
        session_id,
        item_in_session,
        COALESCE(user_key, '-1') AS user_key,
        COALESCE(song_key, '-1') AS song_key,
        COALESCE(session_key, '-1') AS session_key,
        COALESCE(location_key, '-1') AS location_key,
        COALESCE(play_date_key, DATE '1970-01-01') AS play_date_key,
        COALESCE(play_time_key, -1) AS play_time_key,
        play_started_at,
        play_ended_at,
        listened_seconds
    FROM fact_song_play__source
),

fact_song_play__changed_rows AS (
    SELECT source.*
    FROM fact_song_play__coalesce_foreign_keys AS source
    CROSS JOIN fact_song_play__source_check AS source_check
    {% if is_incremental() %}
    LEFT JOIN {{ this }} AS target ON source.play_key = target.play_key
    {% endif %}
    WHERE source_check.is_valid
    {% if is_incremental() %}
        AND (target.play_key IS NULL
            OR source.user_id IS DISTINCT FROM target.user_id
            OR source.session_id IS DISTINCT FROM target.session_id
            OR source.item_in_session IS DISTINCT FROM target.item_in_session
            OR source.user_key IS DISTINCT FROM target.user_key
            OR source.song_key IS DISTINCT FROM target.song_key
            OR source.session_key IS DISTINCT FROM target.session_key
            OR source.location_key IS DISTINCT FROM target.location_key
            OR source.play_date_key IS DISTINCT FROM target.play_date_key
            OR source.play_time_key IS DISTINCT FROM target.play_time_key
            OR source.play_started_at IS DISTINCT FROM target.play_started_at
            OR source.play_ended_at IS DISTINCT FROM target.play_ended_at
            OR source.listened_seconds IS DISTINCT FROM target.listened_seconds
        )
    {% endif %}
),

fact_song_play__audit AS (
    SELECT *, {{ marts_audit_columns() }}
    FROM fact_song_play__changed_rows
)

SELECT * FROM fact_song_play__audit
