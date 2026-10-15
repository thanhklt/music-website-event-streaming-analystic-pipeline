{{ config(
    unique_key=['song_artist_name', 'song_name', 'song_duration_seconds']
) }}

WITH int_dim_song__source AS (
    SELECT
        *
    FROM 
        {{ ref('stg_listen_events') }}
),

int_dim_song__retrieve AS (
    SELECT DISTINCT
        artist,
        song,
        duration
    FROM 
        int_dim_song__source
),

int_dim_song__rename AS (
    SELECT
        song AS song_name,
        artist AS song_artist_name,
        duration AS song_duration_seconds
    FROM
        int_dim_song__retrieve
),

int_dim_song__gen_key AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'song_artist_name', 'song_name', 'song_duration_seconds'
        ]) }} AS song_key,
        song_name,
        song_artist_name,
        song_duration_seconds
    FROM 
        int_dim_song__rename
)

SELECT * FROM int_dim_song__gen_key
