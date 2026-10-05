WITH stg_listen_events__source AS (
    SELECT *
    FROM {{ source('landing', 'listen_events') }}
),

stg_listen_events__cast_type AS (
    SELECT
        CAST(artist AS STRING) AS artist,
        CAST(song AS STRING) AS song,
        CAST(duration AS NUMERIC) AS duration,
        CAST(ts AS TIMESTAMP) AS ts,
        CAST(sessionId AS INT) AS sessionId,
        CAST(auth AS STRING) AS auth,
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
        CAST({{ unix_ms_to_timestamp('registration') }} AS TIMESTAMP) AS registration
    FROM stg_listen_events__source
),

stg_listen_events__dedupe AS (
    SELECT DISTINCT
        artist,
        song,
        duration,
        ts,
        sessionId,
        auth,
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
        registration
    FROM stg_listen_events__cast_type
)

SELECT * FROM stg_listen_events__dedupe
