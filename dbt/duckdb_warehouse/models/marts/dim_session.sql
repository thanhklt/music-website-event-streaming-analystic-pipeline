{{ config(materialized='table', unique_key='session_key') }}

WITH dim_session__source AS (
    SELECT DISTINCT * FROM {{ ref('int_dim_session') }}
),

dim_session__coalesce AS (
    SELECT
        CASE WHEN session_key IS NULL OR session_key = '-1' THEN error('Invalid or reserved dimension key: dim_session') ELSE session_key END AS session_key,
        COALESCE(session_id, CAST(-1 AS INTEGER)) AS session_id,
        COALESCE(user_id, CAST(-1 AS INTEGER)) AS user_id,
        COALESCE(session_started_time, TIMESTAMP '1970-01-01 00:00:00') AS session_started_time,
        COALESCE(session_number, CAST(-1 AS INTEGER)) AS session_number
    FROM dim_session__source
),

dim_session__unknown AS (
    SELECT * FROM dim_session__coalesce
    UNION ALL
    SELECT
        CAST('-1' AS VARCHAR) AS session_key,
        CAST(-1 AS INTEGER) AS session_id,
        CAST(-1 AS INTEGER) AS user_id,
        TIMESTAMP '1970-01-01 00:00:00' AS session_started_time,
        CAST(-1 AS INTEGER) AS session_number
),

dim_session__audit AS (
    SELECT *, {{ marts_audit_columns() }}
    FROM dim_session__unknown
)

SELECT * FROM dim_session__audit
