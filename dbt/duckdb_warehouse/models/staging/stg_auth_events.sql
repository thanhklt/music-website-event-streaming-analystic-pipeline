WITH stg_auth_events__source AS (
    SELECT *
    FROM {{source('landing', 'auth_events')}}
),

stg_auth_events__cast_type AS(
    SELECT
        CAST(ts AS TIMESTAMP) AS ts,
        CAST(sessionId AS INT) AS sessionId,
        CAST(level AS STRING) AS level,
        CAST(itemInSession AS INT) AS itemInSession,
        CAST(city AS STRING) AS city,
        CAST(zip AS STRING) AS zip,
        CAST(state AS STRING) AS state,
        CAST(userAgent AS STRING) AS userAgent,
        CAST(lon AS NUMERIC) AS lon,
        CAST(lat AS NUMERIC) AS lat,
        CAST(userId AS INT) AS userId,
        CAST(lastName AS STRING) AS lastName,
        CAST(firstName AS STRING) AS firstName,
        CAST(gender AS STRING) AS gender,
        CAST({{ unix_ms_to_timestamp('registration') }} AS TIMESTAMP) AS registration,
        CAST(success AS BOOLEAN) AS success
    FROM stg_auth_events__source
),

stg_auth_events__dedupe AS (
    SELECT DISTINCT
        ts,
        sessionId,
        level,
        itemInSession,
        city,
        zip,
        state,
        userAgent,
        lon,
        lat,
        userId,
        lastName,
        firstName,
        gender,
        registration,
        success
    FROM
        stg_auth_events__cast_type
)

SELECT * FROM stg_auth_events__dedupe