SELECT 1 AS failure
FROM {{ ref('stg_listen_events') }}
WHERE FALSE

UNION ALL

SELECT 1 AS failure
FROM {{ ref('stg_page_view_events') }}
WHERE FALSE

UNION ALL

SELECT 1 AS failure
FROM {{ ref('stg_status_change_events') }}
WHERE FALSE
