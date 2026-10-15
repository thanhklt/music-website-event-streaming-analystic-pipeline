{{ config(materialized='table', unique_key='user_key') }}

WITH dim_user__source AS (
    SELECT DISTINCT * FROM {{ ref('int_dim_user') }}
),

dim_user__coalesce AS (
    SELECT
        CASE WHEN user_key IS NULL OR user_key = '-1' THEN error('Invalid or reserved dimension key: dim_user') ELSE user_key END AS user_key,
        COALESCE(user_id, CAST(-1 AS INTEGER)) AS user_id,
        COALESCE(user_last_name, 'Unknown') AS user_last_name,
        COALESCE(user_first_name, 'Unknown') AS user_first_name,
        COALESCE(user_gender, 'Unknown') AS user_gender,
        COALESCE(registered_at, TIMESTAMP '1970-01-01 00:00:00') AS registered_at,
        COALESCE(user_agent, 'Unknown') AS user_agent,
        COALESCE(start_time, TIMESTAMP '1970-01-01 00:00:00') AS start_time,
        COALESCE(end_time, TIMESTAMP '1970-01-01 00:00:00') AS end_time,
        COALESCE(is_active, FALSE) AS is_active
    FROM dim_user__source
),

dim_user__unknown AS (
    SELECT * FROM dim_user__coalesce
    UNION ALL
    SELECT
        CAST('-1' AS VARCHAR) AS user_key,
        CAST(-1 AS INTEGER) AS user_id,
        'Unknown' AS user_last_name,
        'Unknown' AS user_first_name,
        'Unknown' AS user_gender,
        TIMESTAMP '1970-01-01 00:00:00' AS registered_at,
        'Unknown' AS user_agent,
        TIMESTAMP '1970-01-01 00:00:00' AS start_time,
        TIMESTAMP '1970-01-01 00:00:00' AS end_time,
        FALSE AS is_active
),

dim_user__audit AS (
    SELECT *, {{ marts_audit_columns() }}
    FROM dim_user__unknown
)

SELECT * FROM dim_user__audit
