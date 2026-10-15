{{ config(materialized='table', unique_key='date_key') }}

WITH dim_date__source AS (
    SELECT DISTINCT * FROM {{ ref('int_dim_date') }}
),

dim_date__coalesce AS (
    SELECT
        CASE WHEN date_key IS NULL OR date_key = DATE '1970-01-01' THEN error('Invalid or reserved dimension key: dim_date') ELSE date_key END AS date_key,
        COALESCE(day_of_week, CAST(-1 AS INTEGER)) AS day_of_week,
        COALESCE(day_name, 'Unknown') AS day_name,
        COALESCE(day_of_month, CAST(-1 AS INTEGER)) AS day_of_month,
        COALESCE(day_of_year, CAST(-1 AS INTEGER)) AS day_of_year,
        COALESCE(week_of_year, CAST(-1 AS INTEGER)) AS week_of_year,
        COALESCE(iso_year_number, CAST(-1 AS INTEGER)) AS iso_year_number,
        COALESCE(month_number, CAST(-1 AS INTEGER)) AS month_number,
        COALESCE(month_name, 'Unknown') AS month_name,
        COALESCE(quarter_number, CAST(-1 AS INTEGER)) AS quarter_number,
        COALESCE(year_number, CAST(-1 AS INTEGER)) AS year_number,
        COALESCE(is_weekend, FALSE) AS is_weekend
    FROM dim_date__source
),

dim_date__unknown AS (
    SELECT * FROM dim_date__coalesce
    UNION ALL
    SELECT
        CAST(DATE '1970-01-01' AS DATE) AS date_key,
        CAST(-1 AS INTEGER) AS day_of_week,
        'Unknown' AS day_name,
        CAST(-1 AS INTEGER) AS day_of_month,
        CAST(-1 AS INTEGER) AS day_of_year,
        CAST(-1 AS INTEGER) AS week_of_year,
        CAST(-1 AS INTEGER) AS iso_year_number,
        CAST(-1 AS INTEGER) AS month_number,
        'Unknown' AS month_name,
        CAST(-1 AS INTEGER) AS quarter_number,
        CAST(-1 AS INTEGER) AS year_number,
        FALSE AS is_weekend
),

dim_date__audit AS (
    SELECT *, {{ marts_audit_columns() }}
    FROM dim_date__unknown
)

SELECT * FROM dim_date__audit
