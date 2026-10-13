{{ config(
    unique_key=['date_key'],
) }}
WITH int_dim_date__source AS (
    SELECT DISTINCT CAST(ts AS DATE) AS date_key
    FROM {{ ref('stg_listen_events') }}
),

int_dim_date__extract_date AS (
    SELECT
        date_key,
        CAST(EXTRACT(ISODOW FROM date_key) AS INTEGER) AS day_of_week,
        DAYNAME(date_key) AS day_name,
        CAST(DAY(date_key) AS INTEGER) AS day_of_month,
        CAST(DAYOFYEAR(date_key) AS INTEGER) AS day_of_year,
        CAST(WEEKOFYEAR(date_key) AS INTEGER) AS week_of_year,
        CAST(ISOYEAR(date_key) AS INTEGER) AS iso_year_number,
        CAST(MONTH(date_key) AS INTEGER) AS month_number,
        MONTHNAME(date_key) AS month_name,
        CAST(QUARTER(date_key) AS INTEGER) AS quarter_number,
        CAST(YEAR(date_key) AS INTEGER) AS year_number,
        EXTRACT(ISODOW FROM date_key) IN (6, 7) AS is_weekend
    FROM int_dim_date__source
)


SELECT * FROM int_dim_date__extract_date
