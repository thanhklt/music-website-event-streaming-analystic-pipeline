{{ config(
    materialized='table',
    unique_key=['user_id', 'session_id', 'session_started_time']
) }}

WITH int_dim_session__source AS (
    SELECT * 
    FROM {{ ref('stg_page_view_events')}}
),

int_dim_session__retrieve AS (
    SELECT 
        userId, 
        sessionId, 
        ts,
        itemInSession,
        artist,
        song,
        duration
    FROM 
        int_dim_session__source
),

-- Lọc event nghe nhạc
int_dim_session__fitter AS (
    SELECT 
        userId, 
        sessionId, 
        ts,
        itemInSession,
        artist,
        song,
        duration
    FROM 
        int_dim_session__source
    WHERE
        artist IS NOT NULL
        OR song IS NOT NULL
        OR duration IS NOT NULL
),

int_dim_session__rename AS (
    SELECT
        sessionId AS session_id,
        userId AS user_id,
        ts AS session_started_time,
        itemInSession AS session_number,
    FROM
        int_dim_session__fitter
),

int_dim_session__gen_key AS (
    SELECT
        {{ 
            dbt_utils.generate_surrogate_key([
                'user_id',
                'session_id',
                'session_started_time'
            ])
        }} AS session_key,
        session_id,
        user_id,
        session_started_time,
        session_number
    FROM
        int_dim_session__rename        
)

SELECT * FROM int_dim_session__gen_key
