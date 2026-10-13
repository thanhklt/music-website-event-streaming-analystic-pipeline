{{ config(
    unique_key='time_key'
) }}

WITH int_dim_time__source AS (
    SELECT 
        * 
    FROM
        {{ ref('stg_listen_events') }}
),

int_dim_time__retrieve AS (
    SELECT
        ts
    FROM 
        int_dim_time__source
),

int_dim_time__get_hour_minute_second AS (
    SELECT
        ts AS time_key,
        CAST(EXTRACT(HOUR FROM ts) AS INTEGER) AS hour_number,
        CAST(EXTRACT(MINUTE FROM ts) AS INTEGER) AS minute_number,
        CAST(EXTRACT(SECOND FROM ts) AS INTEGER) AS second_number
    FROM 
        int_dim_time__retrieve
),

int_dim_time__get_other_attributes AS (
    SELECT
        time_key,
        hour_number,
        minute_number,
        second_number,
        CASE
            WHEN hour_number % 12 = 0 THEN 12
            ELSE hour_number % 12
        END AS hour_12,
        CASE WHEN hour_number < 12 THEN 'AM' ELSE 'PM' END AS am_pm,
        hour_number * 60 + minute_number AS minute_of_day,
        hour_number * 3600 + minute_number * 60 + second_number AS second_of_day,
        CASE
            WHEN hour_number BETWEEN 5 AND 11 THEN 'Morning'
            WHEN hour_number BETWEEN 12 AND 16 THEN 'Afternoon'
            WHEN hour_number BETWEEN 17 AND 20 THEN 'Evening'
            ELSE 'Night'
        END AS time_period
    FROM 
        int_dim_time__get_hour_minute_second
)

SELECT * FROM int_dim_time__get_other_attributes
