{{ config(materialized='table', unique_key='time_key') }}

WITH dim_time__source AS (
    SELECT DISTINCT * FROM {{ ref('int_dim_time') }}
),

dim_time__coalesce AS (
    SELECT
        CASE WHEN time_key IS NULL OR time_key = -1 THEN error('Invalid or reserved dimension key: dim_time') ELSE time_key END AS time_key,
        COALESCE(time_value, TIME '00:00:00') AS time_value,
        COALESCE(hour_number, CAST(-1 AS INTEGER)) AS hour_number,
        COALESCE(minute_number, CAST(-1 AS INTEGER)) AS minute_number,
        COALESCE(second_number, CAST(-1 AS INTEGER)) AS second_number,
        COALESCE(hour_12, CAST(-1 AS INTEGER)) AS hour_12,
        COALESCE(am_pm, 'Unknown') AS am_pm,
        COALESCE(minute_of_day, CAST(-1 AS INTEGER)) AS minute_of_day,
        COALESCE(second_of_day, CAST(-1 AS INTEGER)) AS second_of_day,
        COALESCE(time_period, 'Unknown') AS time_period
    FROM dim_time__source
),

dim_time__unknown AS (
    SELECT * FROM dim_time__coalesce
    UNION ALL
    SELECT
        CAST(-1 AS INTEGER) AS time_key,
        TIME '00:00:00' AS time_value,
        CAST(-1 AS INTEGER) AS hour_number,
        CAST(-1 AS INTEGER) AS minute_number,
        CAST(-1 AS INTEGER) AS second_number,
        CAST(-1 AS INTEGER) AS hour_12,
        'Unknown' AS am_pm,
        CAST(-1 AS INTEGER) AS minute_of_day,
        CAST(-1 AS INTEGER) AS second_of_day,
        'Unknown' AS time_period
),

dim_time__audit AS (
    SELECT *, {{ marts_audit_columns() }}
    FROM dim_time__unknown
)

SELECT * FROM dim_time__audit
