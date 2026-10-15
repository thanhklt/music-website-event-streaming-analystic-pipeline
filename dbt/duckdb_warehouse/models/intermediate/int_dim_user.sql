{{ config(
    materialized='table',
    unique_key=['user_id', 'start_time']
) }}

WITH int_dim_user__source AS (
    SELECT ts, userId, lastName, firstName, gender, registration, userAgent
    FROM {{ ref('stg_auth_events') }}
    WHERE userId IS NOT NULL

    UNION ALL

    SELECT ts, userId, lastName, firstName, gender, registration, userAgent
    FROM {{ ref('stg_listen_events') }}
    WHERE userId IS NOT NULL

    UNION ALL

    SELECT ts, userId, lastName, firstName, gender, registration, userAgent
    FROM {{ ref('stg_page_view_events') }}
    WHERE userId IS NOT NULL

    UNION ALL

    SELECT ts, userId, lastName, firstName, gender, registration, userAgent
    FROM {{ ref('stg_status_change_events') }}
    WHERE userId IS NOT NULL
),

int_dim_user__observations AS (
    SELECT DISTINCT
        ts,
        userId AS user_id,
        lastName AS user_last_name,
        firstName AS user_first_name,
        gender AS user_gender,
        userAgent AS user_agent,
        registration AS registered_at
    FROM int_dim_user__source
),

int_dim_user__previous_state AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY ts
        ) AS observation_number,
        LAG(user_last_name) OVER (
            PARTITION BY user_id
            ORDER BY ts
        ) AS previous_last_name,
        LAG(user_first_name) OVER (
            PARTITION BY user_id
            ORDER BY ts
        ) AS previous_first_name,
        LAG(user_gender) OVER (
            PARTITION BY user_id
            ORDER BY ts
        ) AS previous_gender,
        LAG(user_agent) OVER (
            PARTITION BY user_id
            ORDER BY ts
        ) AS previous_user_agent,
        LAG(registered_at) OVER (
            PARTITION BY user_id
            ORDER BY ts
        ) AS previous_registered_at
    FROM int_dim_user__observations
),

int_dim_user__change_flags AS (
    SELECT
        *,
        CASE
            WHEN observation_number = 1 THEN 1
            WHEN user_last_name IS DISTINCT FROM previous_last_name THEN 1
            WHEN user_first_name IS DISTINCT FROM previous_first_name THEN 1
            WHEN user_gender IS DISTINCT FROM previous_gender THEN 1
            WHEN user_agent IS DISTINCT FROM previous_user_agent THEN 1
            WHEN registered_at IS DISTINCT FROM previous_registered_at THEN 1
            ELSE 0
        END AS is_new_version
    FROM int_dim_user__previous_state
),

int_dim_user__version_groups AS (
    SELECT
        *,
        SUM(is_new_version) OVER (
            PARTITION BY user_id
            ORDER BY ts
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS version_number
    FROM int_dim_user__change_flags
),

int_dim_user__versions AS (
    SELECT
        user_id,
        user_last_name,
        user_first_name,
        user_gender,
        registered_at,
        user_agent,
        MIN(ts) AS start_time
    FROM int_dim_user__version_groups
    GROUP BY
        user_id,
        version_number,
        user_last_name,
        user_first_name,
        user_gender,
        registered_at,
        user_agent
),

int_dim_user__effective_periods AS (
    SELECT
        *,
        LEAD(start_time) OVER (
            PARTITION BY user_id
            ORDER BY start_time
        ) AS next_start_time
    FROM int_dim_user__versions
),

int_dim_user__scd2 AS (
    SELECT
        user_id,
        user_last_name,
        user_first_name,
        user_gender,
        registered_at,
        user_agent,
        start_time,
        CASE
            WHEN next_start_time IS NULL
                THEN TIMESTAMP '9999-12-01 00:00:00'
            ELSE next_start_time
        END AS end_time,
        next_start_time IS NULL AS is_active
    FROM 
        int_dim_user__effective_periods
),

int_dim_user__gen_key AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['user_id', 'start_time']) }} AS user_key,
        user_id,
        user_last_name,
        user_first_name,
        user_gender,
        registered_at,
        user_agent,
        start_time,
        end_time,
        is_active
    FROM 
        int_dim_user__scd2
)

SELECT * FROM int_dim_user__gen_key
