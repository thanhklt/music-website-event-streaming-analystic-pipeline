SELECT 'dim_user' AS model_name
FROM {{ ref('dim_user') }}
WHERE inserted_time IS NULL OR updated_time IS NULL
    OR inserted_time IS DISTINCT FROM updated_time
    OR inserted_by IS DISTINCT FROM 'dbt' OR updated_by IS DISTINCT FROM 'dbt'
UNION ALL
SELECT 'dim_song' AS model_name
FROM {{ ref('dim_song') }}
WHERE inserted_time IS NULL OR updated_time IS NULL
    OR inserted_time IS DISTINCT FROM updated_time
    OR inserted_by IS DISTINCT FROM 'dbt' OR updated_by IS DISTINCT FROM 'dbt'
UNION ALL
SELECT 'dim_session' AS model_name
FROM {{ ref('dim_session') }}
WHERE inserted_time IS NULL OR updated_time IS NULL
    OR inserted_time IS DISTINCT FROM updated_time
    OR inserted_by IS DISTINCT FROM 'dbt' OR updated_by IS DISTINCT FROM 'dbt'
UNION ALL
SELECT 'dim_location' AS model_name
FROM {{ ref('dim_location') }}
WHERE inserted_time IS NULL OR updated_time IS NULL
    OR inserted_time IS DISTINCT FROM updated_time
    OR inserted_by IS DISTINCT FROM 'dbt' OR updated_by IS DISTINCT FROM 'dbt'
UNION ALL
SELECT 'dim_date' AS model_name
FROM {{ ref('dim_date') }}
WHERE inserted_time IS NULL OR updated_time IS NULL
    OR inserted_time IS DISTINCT FROM updated_time
    OR inserted_by IS DISTINCT FROM 'dbt' OR updated_by IS DISTINCT FROM 'dbt'
UNION ALL
SELECT 'dim_time' AS model_name
FROM {{ ref('dim_time') }}
WHERE inserted_time IS NULL OR updated_time IS NULL
    OR inserted_time IS DISTINCT FROM updated_time
    OR inserted_by IS DISTINCT FROM 'dbt' OR updated_by IS DISTINCT FROM 'dbt'
UNION ALL
SELECT 'fact_song_play' AS model_name
FROM {{ ref('fact_song_play') }}
WHERE inserted_time IS NULL OR updated_time IS NULL
    OR inserted_time IS DISTINCT FROM updated_time
    OR inserted_by IS DISTINCT FROM 'dbt' OR updated_by IS DISTINCT FROM 'dbt'
