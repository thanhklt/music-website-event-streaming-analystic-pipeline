"""Test dashboard models in an isolated disposable DuckDB warehouse."""
from datetime import date
from pathlib import Path
import sys
import tempfile
import unittest

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[3]
STREAMLIT_TESTS = REPO_ROOT / 'streamlit' / 'tests'
sys.path.insert(0, str(STREAMLIT_TESTS))
from dashboard_fixture import build_dashboard_fixture, create_marts_fixture


class DashboardModelsTests(unittest.TestCase):
    def test_build_and_contract_verification(self):
        with tempfile.TemporaryDirectory(prefix='dash-model-test-') as directory:
            db_path = Path(directory) / 'fixture.duckdb'
            create_marts_fixture(db_path)

            # Build dashboard models
            build_dashboard_fixture(db_path, success=True)

            with duckdb.connect(str(db_path), read_only=True) as con:
                # 1. Fact preservation and counts
                rows = con.execute('SELECT COUNT(*), COUNT(DISTINCT play_key) FROM dashboard.song_plays').fetchone()
                self.assertEqual(rows, (6, 6))

                missing_ts = con.execute('SELECT COUNT(*) FROM dashboard.song_plays WHERE play_started_at IS NULL').fetchone()[0]
                self.assertEqual(missing_ts, 1)

                missing_dur = con.execute('SELECT COUNT(*) FROM dashboard.song_plays WHERE listened_seconds IS NULL').fetchone()[0]
                self.assertEqual(missing_dur, 3)

                # Filtered 12-13/10/2026: 5 plays, 2 listeners, 180 seconds
                kpi = con.execute("""
                    SELECT
                        COUNT(*) AS plays,
                        COUNT(DISTINCT user_id) FILTER (WHERE user_id IS NOT NULL AND user_id != -1) AS listeners,
                        COALESCE(SUM(listened_seconds), 0) AS total_seconds
                    FROM dashboard.song_plays
                    WHERE play_date >= DATE '2026-10-12' AND play_date <= DATE '2026-10-13'
                """).fetchone()
                self.assertEqual(kpi, (5, 2, 180.0))

                # User 1 has two versions (u1 and u3) but distinct user_id is 1
                u1_plays = con.execute("SELECT COUNT(*), COUNT(DISTINCT user_id) FROM dashboard.song_plays WHERE user_id = 1").fetchone()
                self.assertEqual(u1_plays, (2, 1))

                # Two songs with same name but different artist and song_key
                shared_songs = con.execute("""
                    SELECT song_key, song, artist FROM dashboard.song_plays
                    WHERE song = 'Shared title'
                    GROUP BY song_key, song, artist
                    ORDER BY song_key
                """).fetchall()
                self.assertEqual(len(shared_songs), 2)
                self.assertEqual(shared_songs[0][0], 's1')
                self.assertEqual(shared_songs[0][2], 'Artist A')
                self.assertEqual(shared_songs[1][0], 's2')
                self.assertEqual(shared_songs[1][2], 'Artist B')

                # 2. Options completeness, normalization and uniqueness
                options = con.execute("""
                    SELECT filter_name, filter_value
                    FROM dashboard.filter_options
                    ORDER BY filter_name, filter_value
                """).fetchall()

                states = [val for name, val in options if name == 'states']
                genders = [val for name, val in options if name == 'genders']

                # TX is included even without plays; whitespace normalized to Unknown
                self.assertEqual(states, ['CA', 'NY', 'TX', 'Unknown'])
                # NB is included even without plays; empty normalized to Unknown
                self.assertEqual(genders, ['F', 'M', 'NB', 'Unknown'])

                # Options must be strictly unique per filter_name
                duplicates = con.execute("""
                    SELECT filter_name, filter_value, COUNT(*)
                    FROM dashboard.filter_options
                    GROUP BY filter_name, filter_value
                    HAVING COUNT(*) > 1
                """).fetchall()
                self.assertEqual(duplicates, [])

    def test_rebuild_reflects_changes_and_idempotency(self):
        with tempfile.TemporaryDirectory(prefix='dash-rebuild-test-') as directory:
            db_path = Path(directory) / 'fixture.duckdb'
            create_marts_fixture(db_path)

            build_dashboard_fixture(db_path, success=True)

            # Change duration from NULL to 300, and change dimension label without altering fact audit
            with duckdb.connect(str(db_path)) as con:
                con.execute("UPDATE marts.fact_song_play SET listened_seconds = 300 WHERE play_key = 'p2'")
                con.execute("UPDATE marts.dim_location SET location_state = 'New York' WHERE location_key = 'l2'")

            # Rebuild
            build_dashboard_fixture(db_path, success=True)

            with duckdb.connect(str(db_path), read_only=True) as con:
                dur = con.execute("SELECT listened_seconds FROM dashboard.song_plays WHERE play_key = 'p2'").fetchone()[0]
                self.assertEqual(dur, 300.0)

                state = con.execute("SELECT state FROM dashboard.song_plays WHERE play_key = 'p3'").fetchone()[0]
                self.assertEqual(state, 'New York')

                # Rebuild again: still 6 rows
                count = con.execute("SELECT COUNT(*) FROM dashboard.song_plays").fetchone()[0]
                self.assertEqual(count, 6)

    def test_fanout_and_corrupt_keys_are_rejected(self):
        with tempfile.TemporaryDirectory(prefix='dash-fanout-test-') as directory:
            db_path = Path(directory) / 'fixture.duckdb'
            create_marts_fixture(db_path)

            # Duplicate key in dim_location produces fan-out and must fail test/build
            with duckdb.connect(str(db_path)) as con:
                con.execute("INSERT INTO marts.dim_location VALUES ('l1', 'Duplicate CA')")

            with self.assertRaises(Exception):
                build_dashboard_fixture(db_path, success=True)


if __name__ == '__main__':
    unittest.main()
