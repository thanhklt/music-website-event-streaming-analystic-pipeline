"""Run the real dbt fact against dimension fixtures in a temporary warehouse.

Detects joins that multiply plays, select the wrong SCD2 version, or lose
unmatched plays. Requires installed dbt packages; never opens the user warehouse.
"""

from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import duckdb


PROJECT = Path(__file__).resolve().parents[1]


class FactSongPlayTests(unittest.TestCase):
    def test_dimension_lookup_preserves_grain_and_scd2_boundaries(self):
        with tempfile.TemporaryDirectory(prefix="fact-song-play-") as directory:
            work = Path(directory)
            database = work / "fixture.duckdb"
            (work / "profiles.yml").write_text(
                "duckdb_warehouse:\n"
                "  target: test\n"
                "  outputs:\n"
                "    test:\n"
                "      type: duckdb\n"
                f"      path: '{database.as_posix()}'\n"
                "      schema: main\n"
                "      threads: 1\n",
                encoding="utf-8",
            )
            with duckdb.connect(str(database)) as connection:
                connection.execute("CREATE SCHEMA staging")
                connection.execute("CREATE SCHEMA intermediate")
                connection.execute("""
                    CREATE TABLE staging.stg_page_view_events AS
                    SELECT * FROM (VALUES
                        (TIMESTAMP '2026-10-13 08:00:00.123', 7, 'NextSong', 'free', 0,
                         'City', NULL::VARCHAR, 'ST', 1.0, 2.0, 42, 'Artist', 'Song', 180.0),
                        (TIMESTAMP '2026-10-13 08:01:00.123', 7, 'Home', 'free', 1,
                         'City', NULL, 'ST', 1.0, 2.0, 42, NULL, NULL, NULL),
                        (TIMESTAMP '2026-10-13 08:02:00.123', 7, 'NextSong', 'paid', 2,
                         'City', NULL, 'ST', 1.0, 2.0, 42, 'Artist', 'Song', 180.0),
                        (TIMESTAMP '2026-10-13 08:04:00.123', 9, 'NextSong', 'free', 0,
                         'Missing', NULL, NULL, NULL, NULL, 99, 'Other', 'Missing', 100.0)
                    ) AS events(ts, sessionId, page, level, itemInSession,
                        city, zip, state, lon, lat, userId, artist, song, duration);

                    CREATE TABLE intermediate.int_dim_user AS
                    SELECT * FROM (VALUES
                        ('user-old', 42, TIMESTAMP '2026-10-13 00:00:00',
                         TIMESTAMP '2026-10-13 08:02:00.123'),
                        ('user-new', 42, TIMESTAMP '2026-10-13 08:02:00.123',
                         TIMESTAMP '9999-12-01 00:00:00')
                    ) AS users(user_key, user_id, start_time, end_time);

                    CREATE TABLE intermediate.int_dim_song AS
                    SELECT * FROM (VALUES
                        ('song-existing', 'Artist', 'Song', 180.0),
                        ('song-existing', 'Artist', 'Song', 180.0)
                    ) AS songs(song_key, song_artist_name, song_name, song_duration_seconds);

                    CREATE TABLE intermediate.int_dim_session AS
                    SELECT * FROM (VALUES
                        ('session-event-0', 42, 7, TIMESTAMP '2026-10-13 08:00:00.123', 0),
                        ('session-event-2', 42, 7, TIMESTAMP '2026-10-13 08:02:00.123', 2)
                    ) AS sessions(session_key, user_id, session_id,
                        session_started_time, session_number);

                    CREATE TABLE intermediate.int_dim_location AS
                    SELECT 'location-existing' AS location_key, 'City' AS location_city,
                        NULL::VARCHAR AS location_zip, 'ST' AS location_state,
                        1.0 AS location_lon, 2.0 AS location_lat;

                    CREATE TABLE intermediate.int_dim_date AS
                    SELECT DATE '2026-10-13' AS date_key;

                    CREATE TABLE intermediate.int_dim_time AS
                    SELECT * FROM (VALUES
                        (TIMESTAMP '2026-10-13 08:00:00.123'),
                        (TIMESTAMP '2026-10-13 08:00:00.123'),
                        (TIMESTAMP '2026-10-13 08:02:00.123')
                    ) AS times(time_key);
                """)

            command = [
                shutil.which("dbt") or "dbt", "run",
                "--project-dir", str(PROJECT), "--profiles-dir", str(work),
                "--target-path", str(work / "target"),
                "--log-path", str(work / "logs"),
                "--select", "int_fact_song_play", "--no-use-colors",
            ]
            result = subprocess.run(command, capture_output=True, text=True, timeout=120)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

            with duckdb.connect(str(database), read_only=True) as connection:
                rows = connection.execute("""
                    SELECT user_id, item_in_session, user_key, song_key, session_key,
                        location_key, CAST(play_date_key AS VARCHAR),
                        CAST(play_time_key AS VARCHAR), listened_seconds,
                        song_position_in_session
                    FROM intermediate.int_fact_song_play
                    ORDER BY user_id, item_in_session
                """).fetchall()
                self.assertEqual(rows, [
                    (42, 0, 'user-old', 'song-existing', 'session-event-0',
                     'location-existing', '2026-10-13', '2026-10-13 08:00:00.123', 60.0, 1),
                    (42, 2, 'user-new', 'song-existing', 'session-event-2',
                     'location-existing', '2026-10-13', '2026-10-13 08:02:00.123', None, 2),
                    (99, 0, None, None, None, None, '2026-10-13', None, None, 1),
                ])
                total, unique_keys = connection.execute("""
                    SELECT COUNT(*), COUNT(DISTINCT play_key)
                    FROM intermediate.int_fact_song_play
                """).fetchone()
                self.assertEqual((total, unique_keys), (3, 3))


if __name__ == "__main__":
    unittest.main()

