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
from jinja2 import Environment


PROJECT = Path(__file__).resolve().parents[1]


class FactSongPlayTests(unittest.TestCase):
    def test_duration_quality_test_rejects_invalid_values(self):
        # Exercise the actual singular test using deliberately corrupt rows.
        query = Environment().from_string(
            (PROJECT / 'tests' / 'assert_fact_song_play_duration.sql').read_text(encoding='utf-8')
        ).render(ref=lambda name: name)
        with duckdb.connect(':memory:') as connection:
            connection.execute('''
                CREATE TABLE int_fact_song_play AS
                SELECT play_key, TIMESTAMP '2026-10-14 08:00:00' AS play_started_at,
                       play_ended_at, listened_seconds
                FROM (VALUES
                    ('last', NULL::TIMESTAMP, NULL::DOUBLE),
                    ('fractional', TIMESTAMP '2026-10-14 08:00:01.250', 1.25),
                    ('zero', TIMESTAMP '2026-10-14 08:00:00', 0.0),
                    ('missing_end', NULL, 1.0),
                    ('missing_seconds', TIMESTAMP '2026-10-14 08:00:01', NULL),
                    ('negative', TIMESTAMP '2026-10-14 07:59:59', -1.0),
                    ('wrong_seconds', TIMESTAMP '2026-10-14 08:00:01.250', 1.0)
                ) AS fixtures(play_key, play_ended_at, listened_seconds)
            ''')
            self.assertEqual({row[0] for row in connection.execute(query).fetchall()}, {
                'missing_end', 'missing_seconds', 'negative', 'wrong_seconds'
            })

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
                         'Missing', NULL, NULL, NULL, NULL, 99, 'Other', 'Missing', 100.0),
                        (TIMESTAMP '2026-10-14 08:00:00.123', 7, 'NextSong', 'paid', 3,
                         NULL, NULL, NULL, NULL, NULL, 42, NULL, 'Nullable', NULL)
                    ) AS events(ts, sessionId, page, level, itemInSession,
                        city, zip, state, lon, lat, userId, artist, song, duration);

                    CREATE TABLE staging.stg_listen_events AS
                    SELECT * EXCLUDE (page, level)
                    FROM staging.stg_page_view_events WHERE page = 'NextSong';

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
                        ('song-existing', 'Artist', 'Song', 180.0),
                        ('song-null', NULL, 'Nullable', NULL)
                    ) AS songs(song_key, song_artist_name, song_name, song_duration_seconds);

                    CREATE TABLE intermediate.int_dim_session AS
                    SELECT * FROM (VALUES
                        ('session-event-0', 42, 7, TIMESTAMP '2026-10-13 08:00:00.123', 0),
                        ('session-event-2', 42, 7, TIMESTAMP '2026-10-13 08:02:00.123', 2),
                        ('session-event-2', 42, 7, TIMESTAMP '2026-10-13 08:02:00.123', 2),
                        ('wrong-position', 42, 7, TIMESTAMP '2026-10-14 08:00:00.123', 99),
                        ('wrong-time', 42, 7, TIMESTAMP '2026-10-14 08:00:01.123', 3),
                        ('wrong-user', 100, 7, TIMESTAMP '2026-10-14 08:00:00.123', 3),
                        ('wrong-session', 42, 100, TIMESTAMP '2026-10-14 08:00:00.123', 3)
                    ) AS sessions(session_key, user_id, session_id,
                        session_started_time, session_number);

                    CREATE TABLE intermediate.int_dim_location AS
                    SELECT 'location-existing' AS location_key, 'City' AS location_city,
                        NULL::VARCHAR AS location_zip, 'ST' AS location_state,
                        1.0 AS location_lon, 2.0 AS location_lat;

                    INSERT INTO intermediate.int_dim_location VALUES
                        ('location-null', NULL, NULL, NULL, NULL, NULL);

                    CREATE TABLE intermediate.int_dim_date AS
                    SELECT DATE '2026-10-13' AS date_key;

                    CREATE TABLE intermediate.int_dim_time AS
                    SELECT * FROM (VALUES
                        (28800),
                        (28800),
                        (28920)
                    ) AS times(time_key);

                    INSERT INTO intermediate.int_dim_user
                    SELECT * FROM intermediate.int_dim_user;
                    INSERT INTO intermediate.int_dim_location
                    SELECT * FROM intermediate.int_dim_location;
                """)

            command = [
                shutil.which("dbt") or "dbt", "build",
                "--project-dir", str(PROJECT), "--profiles-dir", str(work),
                "--target-path", str(work / "target"),
                "--log-path", str(work / "logs"),
                "--select", "int_fact_song_play", "--no-use-colors",
                # This fixture builds intermediate only, not its downstream marts snapshot.
                "--exclude", "assert_marts_fact_preserved",
            ]
            result = subprocess.run(command, capture_output=True, text=True, timeout=120)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

            with duckdb.connect(str(database), read_only=True) as connection:
                rows = connection.execute("""
                    SELECT user_id, item_in_session, user_key, song_key, session_key,
                        location_key, CAST(play_date_key AS VARCHAR),
                        CAST(play_time_key AS VARCHAR), CAST(play_started_at AS VARCHAR)
                    FROM intermediate.int_fact_song_play
                    ORDER BY user_id, item_in_session
                """).fetchall()
                self.assertEqual(rows, [
                    (42, 0, 'user-old', 'song-existing', 'session-event-0',
                     'location-existing', '2026-10-13', '28800',
                     '2026-10-13 08:00:00.123'),
                    (42, 2, 'user-new', 'song-existing', 'session-event-2',
                     'location-existing', '2026-10-13', '28920',
                     '2026-10-13 08:02:00.123'),
                    (42, 3, 'user-new', 'song-null', None, 'location-null', None, '28800',
                     '2026-10-14 08:00:00.123'),
                    (99, 0, None, None, None, None, '2026-10-13', None,
                     '2026-10-13 08:04:00.123'),
                ])
                total, unique_keys = connection.execute("""
                    SELECT COUNT(*), COUNT(DISTINCT play_key)
                    FROM intermediate.int_fact_song_play
                """).fetchone()
                self.assertEqual((total, unique_keys), (4, 4))
                durations = connection.execute('''
                    SELECT user_id, item_in_session, CAST(play_ended_at AS VARCHAR), listened_seconds
                    FROM intermediate.int_fact_song_play ORDER BY user_id, item_in_session
                ''').fetchall()
                self.assertEqual(durations, [
                    (42, 0, '2026-10-13 08:01:00.123', 60.0),
                    (42, 2, '2026-10-14 08:00:00.123', 86280.0),
                    (42, 3, None, None),
                    (99, 0, None, None),
                ])
                first_build = connection.execute(
                    'SELECT * FROM intermediate.int_fact_song_play ORDER BY play_key'
                ).fetchall()

            result = subprocess.run(command, capture_output=True, text=True, timeout=120)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            with duckdb.connect(str(database), read_only=True) as connection:
                self.assertEqual(first_build, connection.execute(
                    'SELECT * FROM intermediate.int_fact_song_play ORDER BY play_key'
                ).fetchall())
                missing = connection.execute('''
                    SELECT COUNT(*) FILTER (WHERE user_key IS NULL),
                           COUNT(*) FILTER (WHERE song_key IS NULL),
                           COUNT(*) FILTER (WHERE session_key IS NULL),
                           COUNT(*) FILTER (WHERE location_key IS NULL),
                           COUNT(*) FILTER (WHERE play_date_key IS NULL),
                           COUNT(*) FILTER (WHERE play_time_key IS NULL)
                    FROM intermediate.int_fact_song_play
                ''').fetchone()
                self.assertEqual(missing, (1, 1, 2, 1, 1, 1))

            # A late page event changes duration, while keys and play count stay stable.
            with duckdb.connect(str(database)) as connection:
                connection.execute('''
                    INSERT INTO staging.stg_page_view_events
                    SELECT * REPLACE (
                        TIMESTAMP '2026-10-13 08:00:01.373' AS ts, 100 AS itemInSession,
                        'Home' AS page
                    ) FROM staging.stg_page_view_events WHERE userId = 42 AND itemInSession = 0;

                    -- Same time as a listen, larger item: a valid zero-second interval.
                    INSERT INTO staging.stg_page_view_events
                    SELECT * REPLACE (4 AS itemInSession, 'Home' AS page)
                    FROM staging.stg_page_view_events WHERE userId = 42 AND itemInSession = 3;

                    -- Listen-only events must still participate in the timeline.
                    DELETE FROM staging.stg_page_view_events WHERE userId = 42 AND itemInSession = 2;

                    -- Neither another user nor another session may end user 99's last play.
                    INSERT INTO staging.stg_page_view_events
                    SELECT * REPLACE (
                        TIMESTAMP '2026-10-13 08:04:01.123' AS ts, 100 AS userId,
                        1 AS itemInSession, 'Home' AS page
                    ) FROM staging.stg_page_view_events WHERE userId = 99;
                    INSERT INTO staging.stg_page_view_events
                    SELECT * REPLACE (
                        TIMESTAMP '2026-10-13 08:04:02.123' AS ts, 10 AS sessionId,
                        1 AS itemInSession, 'Home' AS page
                    ) FROM staging.stg_page_view_events WHERE userId = 99;
                ''')
            result = subprocess.run(command, capture_output=True, text=True, timeout=120)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            with duckdb.connect(str(database), read_only=True) as connection:
                self.assertEqual(connection.execute('''
                    SELECT user_id, item_in_session, CAST(play_ended_at AS VARCHAR), listened_seconds
                    FROM intermediate.int_fact_song_play ORDER BY user_id, item_in_session
                ''').fetchall(), [
                    (42, 0, '2026-10-13 08:00:01.373', 1.25),
                    (42, 2, '2026-10-14 08:00:00.123', 86280.0),
                    (42, 3, '2026-10-14 08:00:00.123', 0.0),
                    (99, 0, None, None),
                ])
                self.assertEqual([r[0] for r in first_build], [r[0] for r in connection.execute(
                    'SELECT * FROM intermediate.int_fact_song_play ORDER BY play_key'
                ).fetchall()])

            # Same event identity at two timestamps is an invalid timeline.
            with duckdb.connect(str(database)) as connection:
                connection.execute('''
                    INSERT INTO staging.stg_page_view_events
                    SELECT * REPLACE (ts + INTERVAL '1 second' AS ts)
                    FROM staging.stg_page_view_events WHERE userId = 42 AND itemInSession = 0
                ''')
            result = subprocess.run(command, capture_output=True, text=True, timeout=120)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('Conflicting event timestamps in play timeline', result.stdout + result.stderr)
            with duckdb.connect(str(database)) as connection:
                connection.execute('''
                    DELETE FROM staging.stg_page_view_events
                    WHERE userId = 42 AND itemInSession = 0
                      AND ts = TIMESTAMP '2026-10-13 08:00:01.123'
                ''')

            # The existing dbt grain/unique tests reject ambiguous joins and source duplicates.
            with duckdb.connect(str(database)) as connection:
                connection.execute('''
                    INSERT INTO intermediate.int_dim_song
                    SELECT 'conflicting-song', song_artist_name, song_name, song_duration_seconds
                    FROM intermediate.int_dim_song LIMIT 1
                ''')
            result = subprocess.run(command, capture_output=True, text=True, timeout=120)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('FAIL 2 unique_int_fact_song_play_play_key', result.stdout + result.stderr)

            with duckdb.connect(str(database)) as connection:
                connection.execute("DELETE FROM intermediate.int_dim_song WHERE song_key = 'conflicting-song'")
                connection.execute('''
                    INSERT INTO intermediate.int_dim_user VALUES
                    ('overlap', 42, TIMESTAMP '2026-10-13 00:00:00', TIMESTAMP '9999-12-01')
                ''')
            result = subprocess.run(command, capture_output=True, text=True, timeout=120)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('unique_int_fact_song_play_play_key', result.stdout + result.stderr)

            with duckdb.connect(str(database)) as connection:
                connection.execute("DELETE FROM intermediate.int_dim_user WHERE user_key = 'overlap'")
                connection.execute('''
                    INSERT INTO staging.stg_listen_events
                    SELECT * REPLACE ('Changed song' AS song)
                    FROM staging.stg_listen_events WHERE userId = 42 AND itemInSession = 0
                ''')
            result = subprocess.run(command, capture_output=True, text=True, timeout=120)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('unique_int_fact_song_play_play_key', result.stdout + result.stderr)


if __name__ == "__main__":
    unittest.main()

