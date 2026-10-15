-- Return invalid durations; the final event in a user/session may have both values NULL.
SELECT play_key
FROM {{ ref('int_fact_song_play') }}
WHERE (play_ended_at IS NULL AND listened_seconds IS NOT NULL)
   OR (play_ended_at IS NOT NULL AND listened_seconds IS NULL)
   OR play_ended_at < play_started_at
   OR listened_seconds < 0
   OR listened_seconds IS DISTINCT FROM EPOCH(play_ended_at - play_started_at)
