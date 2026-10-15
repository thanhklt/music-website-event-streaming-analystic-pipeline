{{ config(materialized='table', unique_key='location_key') }}

WITH dim_location__source AS (
    SELECT DISTINCT * FROM {{ ref('int_dim_location') }}
),

dim_location__coalesce AS (
    SELECT
        CASE WHEN location_key IS NULL OR location_key = '-1' THEN error('Invalid or reserved dimension key: dim_location') ELSE location_key END AS location_key,
        COALESCE(location_city, 'Unknown') AS location_city,
        COALESCE(location_zip, 'Unknown') AS location_zip,
        COALESCE(location_state, 'Unknown') AS location_state,
        COALESCE(location_lon, CAST(-1 AS DECIMAL(18,3))) AS location_lon,
        COALESCE(location_lat, CAST(-1 AS DECIMAL(18,3))) AS location_lat
    FROM dim_location__source
),

dim_location__unknown AS (
    SELECT * FROM dim_location__coalesce
    UNION ALL
    SELECT
        CAST('-1' AS VARCHAR) AS location_key,
        'Unknown' AS location_city,
        'Unknown' AS location_zip,
        'Unknown' AS location_state,
        CAST(-1 AS DECIMAL(18,3)) AS location_lon,
        CAST(-1 AS DECIMAL(18,3)) AS location_lat
),

dim_location__audit AS (
    SELECT *, {{ marts_audit_columns() }}
    FROM dim_location__unknown
)

SELECT * FROM dim_location__audit
