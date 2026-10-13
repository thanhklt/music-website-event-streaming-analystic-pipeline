{{ config(
    materialized='table',
    unique_key=['location_city', 'location_zip', 'location_state', 'location_lon', 'location_lat']
) }}

WITH int_dim_location__source AS (
    SELECT 
        *
    FROM 
        {{ ref('stg_page_view_events') }}
),

int_dim_location__retrieve AS (
    SELECT
        city, 
        zip, 
        state, 
        lon, 
        lat
    FROM
        int_dim_location__source
),
int_dim_location__grain AS (
    SELECT DISTINCT
        city AS location_city,
        zip AS location_zip,
        state AS location_state,
        lon AS location_lon,
        lat AS location_lat
    FROM int_dim_location__retrieve
),

int_dim_location__generate_key AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'location_city',
            'location_zip',
            'location_state',
            'location_lon',
            'location_lat'
        ]) }} AS location_key,
        *  
    FROM int_dim_location__grain
)

SELECT * FROM int_dim_location__generate_key
