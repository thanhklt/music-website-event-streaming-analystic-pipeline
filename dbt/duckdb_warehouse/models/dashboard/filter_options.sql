{{ config(materialized='table', schema='dashboard') }}

WITH location_states AS (
    SELECT DISTINCT
        'states' AS filter_name,
        COALESCE(NULLIF(TRIM(location_state), ''), 'Unknown') AS filter_value
    FROM {{ ref('dim_location') }}
),

user_genders AS (
    SELECT DISTINCT
        'genders' AS filter_name,
        COALESCE(NULLIF(TRIM(user_gender), ''), 'Unknown') AS filter_value
    FROM {{ ref('dim_user') }}
),

all_options AS (
    SELECT filter_name, filter_value FROM location_states
    UNION ALL
    SELECT filter_name, filter_value FROM user_genders
)

SELECT filter_name, filter_value
FROM all_options
ORDER BY filter_name, filter_value
