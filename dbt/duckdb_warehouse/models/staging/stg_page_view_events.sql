WITH stg_page_view_events__source AS (
    SELECT *
    FROM {{ source('landing', 'page_view_events') }}
),

stg_page_view_events__cast_type AS (
    SELECT
        CAST(ts AS TIMESTAMP) AS ts,
        CAST(sessionId AS INT) AS sessionId,
        CAST(page AS STRING) AS page,
        CAST(auth AS STRING) AS auth,
        CAST(method AS STRING) AS method,
        CAST(status AS INT) AS status,
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
        CAST(artist AS STRING) AS artist,
        CAST(song AS STRING) AS song,
        CAST(duration AS NUMERIC) AS duration
    FROM stg_page_view_events__source
),

stg_page_view_events__dedupe AS (
    SELECT DISTINCT
        ts,
        sessionId,
        page,
        auth,
        method,
        status,
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
        artist,
        song,
        duration
    FROM stg_page_view_events__cast_type
)

SELECT * FROM stg_page_view_events__dedupe
