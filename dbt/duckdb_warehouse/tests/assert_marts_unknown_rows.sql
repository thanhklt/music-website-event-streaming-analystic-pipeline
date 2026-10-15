SELECT 'dim_user: missing/duplicate Unknown' AS problem
WHERE (SELECT COUNT(*) FROM {{ ref('dim_user') }} WHERE user_key = '-1') <> 1
UNION ALL
SELECT 'dim_user: reserved source key' AS problem
FROM {{ ref('int_dim_user') }} WHERE user_key = '-1'
UNION ALL
SELECT 'dim_user: wrong defaults' AS problem
FROM {{ ref('dim_user') }} WHERE user_key = '-1' AND (user_id IS DISTINCT FROM CAST(-1 AS INTEGER) OR user_last_name IS DISTINCT FROM 'Unknown' OR user_first_name IS DISTINCT FROM 'Unknown' OR user_gender IS DISTINCT FROM 'Unknown' OR registered_at IS DISTINCT FROM TIMESTAMP '1970-01-01 00:00:00' OR user_agent IS DISTINCT FROM 'Unknown' OR start_time IS DISTINCT FROM TIMESTAMP '1970-01-01 00:00:00' OR end_time IS DISTINCT FROM TIMESTAMP '1970-01-01 00:00:00' OR is_active IS DISTINCT FROM FALSE)
UNION ALL
SELECT 'dim_song: missing/duplicate Unknown' AS problem
WHERE (SELECT COUNT(*) FROM {{ ref('dim_song') }} WHERE song_key = '-1') <> 1
UNION ALL
SELECT 'dim_song: reserved source key' AS problem
FROM {{ ref('int_dim_song') }} WHERE song_key = '-1'
UNION ALL
SELECT 'dim_song: wrong defaults' AS problem
FROM {{ ref('dim_song') }} WHERE song_key = '-1' AND (song_name IS DISTINCT FROM 'Unknown' OR song_artist_name IS DISTINCT FROM 'Unknown' OR song_duration_seconds IS DISTINCT FROM CAST(-1 AS DECIMAL(18,3)))
UNION ALL
SELECT 'dim_session: missing/duplicate Unknown' AS problem
WHERE (SELECT COUNT(*) FROM {{ ref('dim_session') }} WHERE session_key = '-1') <> 1
UNION ALL
SELECT 'dim_session: reserved source key' AS problem
FROM {{ ref('int_dim_session') }} WHERE session_key = '-1'
UNION ALL
SELECT 'dim_session: wrong defaults' AS problem
FROM {{ ref('dim_session') }} WHERE session_key = '-1' AND (session_id IS DISTINCT FROM CAST(-1 AS INTEGER) OR user_id IS DISTINCT FROM CAST(-1 AS INTEGER) OR session_started_time IS DISTINCT FROM TIMESTAMP '1970-01-01 00:00:00' OR session_number IS DISTINCT FROM CAST(-1 AS INTEGER))
UNION ALL
SELECT 'dim_location: missing/duplicate Unknown' AS problem
WHERE (SELECT COUNT(*) FROM {{ ref('dim_location') }} WHERE location_key = '-1') <> 1
UNION ALL
SELECT 'dim_location: reserved source key' AS problem
FROM {{ ref('int_dim_location') }} WHERE location_key = '-1'
UNION ALL
SELECT 'dim_location: wrong defaults' AS problem
FROM {{ ref('dim_location') }} WHERE location_key = '-1' AND (location_city IS DISTINCT FROM 'Unknown' OR location_zip IS DISTINCT FROM 'Unknown' OR location_state IS DISTINCT FROM 'Unknown' OR location_lon IS DISTINCT FROM CAST(-1 AS DECIMAL(18,3)) OR location_lat IS DISTINCT FROM CAST(-1 AS DECIMAL(18,3)))
UNION ALL
SELECT 'dim_date: missing/duplicate Unknown' AS problem
WHERE (SELECT COUNT(*) FROM {{ ref('dim_date') }} WHERE date_key = DATE '1970-01-01') <> 1
UNION ALL
SELECT 'dim_date: reserved source key' AS problem
FROM {{ ref('int_dim_date') }} WHERE date_key = DATE '1970-01-01'
UNION ALL
SELECT 'dim_date: wrong defaults' AS problem
FROM {{ ref('dim_date') }} WHERE date_key = DATE '1970-01-01' AND (day_of_week IS DISTINCT FROM CAST(-1 AS INTEGER) OR day_name IS DISTINCT FROM 'Unknown' OR day_of_month IS DISTINCT FROM CAST(-1 AS INTEGER) OR day_of_year IS DISTINCT FROM CAST(-1 AS INTEGER) OR week_of_year IS DISTINCT FROM CAST(-1 AS INTEGER) OR iso_year_number IS DISTINCT FROM CAST(-1 AS INTEGER) OR month_number IS DISTINCT FROM CAST(-1 AS INTEGER) OR month_name IS DISTINCT FROM 'Unknown' OR quarter_number IS DISTINCT FROM CAST(-1 AS INTEGER) OR year_number IS DISTINCT FROM CAST(-1 AS INTEGER) OR is_weekend IS DISTINCT FROM FALSE)
UNION ALL
SELECT 'dim_time: missing/duplicate Unknown' AS problem
WHERE (SELECT COUNT(*) FROM {{ ref('dim_time') }} WHERE time_key = -1) <> 1
UNION ALL
SELECT 'dim_time: reserved source key' AS problem
FROM {{ ref('int_dim_time') }} WHERE time_key = -1
UNION ALL
SELECT 'dim_time: wrong defaults' AS problem
FROM {{ ref('dim_time') }} WHERE time_key = -1 AND (time_value IS DISTINCT FROM TIME '00:00:00' OR hour_number IS DISTINCT FROM CAST(-1 AS INTEGER) OR minute_number IS DISTINCT FROM CAST(-1 AS INTEGER) OR second_number IS DISTINCT FROM CAST(-1 AS INTEGER) OR hour_12 IS DISTINCT FROM CAST(-1 AS INTEGER) OR am_pm IS DISTINCT FROM 'Unknown' OR minute_of_day IS DISTINCT FROM CAST(-1 AS INTEGER) OR second_of_day IS DISTINCT FROM CAST(-1 AS INTEGER) OR time_period IS DISTINCT FROM 'Unknown')
