{{ config(materialized='table', unique_key='song_key') }}

WITH dim_song__source AS (
    SELECT DISTINCT * FROM {{ ref('int_dim_song') }}
),

dim_song__coalesce AS (
    SELECT
        CASE WHEN song_key IS NULL OR song_key = '-1' THEN error('Invalid or reserved dimension key: dim_song') ELSE song_key END AS song_key,
        COALESCE(song_name, 'Unknown') AS song_name,
        COALESCE(song_artist_name, 'Unknown') AS song_artist_name,
        COALESCE(song_duration_seconds, CAST(-1 AS DECIMAL(18,3))) AS song_duration_seconds
    FROM dim_song__source
),

dim_song__unknown AS (
    SELECT * FROM dim_song__coalesce
    UNION ALL
    SELECT
        CAST('-1' AS VARCHAR) AS song_key,
        'Unknown' AS song_name,
        'Unknown' AS song_artist_name,
        CAST(-1 AS DECIMAL(18,3)) AS song_duration_seconds
),

dim_song__audit AS (
    SELECT *, {{ marts_audit_columns() }}
    FROM dim_song__unknown
)

SELECT * FROM dim_song__audit
