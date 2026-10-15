"""Build real marts in a disposable warehouse; never open the user's database."""
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import duckdb
from jinja2 import Environment


PROJECT = Path(__file__).resolve().parents[1]
DIMENSIONS = ['user', 'song', 'session', 'location', 'date', 'time']


class MartsTests(unittest.TestCase):
    def test_raw_conflicts_are_not_hidden_by_coalesce(self):
        macro = (PROJECT / 'macros/marts_audit_columns.sql').read_text(encoding='utf-8')
        model = (PROJECT / 'models/marts/dim_song.sql').read_text(encoding='utf-8')
        sql = Environment().from_string(macro + '\n' + model).render(
            config=lambda **kwargs: '', ref=lambda name: name,
            run_started_at=datetime.now(timezone.utc)
        )
        with duckdb.connect(':memory:') as con:
            con.execute('''
                CREATE TABLE int_dim_song AS SELECT * FROM (VALUES
                    ('song', NULL::VARCHAR, 'Title', 1.0),
                    ('song', NULL::VARCHAR, 'Title', 1.0),
                    ('song', 'Unknown', 'Title', 1.0)
                ) rows(song_key, song_artist_name, song_name, song_duration_seconds)
            ''')
            con.execute('CREATE TABLE result AS ' + sql)
            # Exact source duplicates collapse, but the two raw versions must survive
            # normalization so the dbt unique test can reject the conflicting key.
            self.assertEqual(con.execute("SELECT COUNT(*) FROM result WHERE song_key = 'song'").fetchone()[0], 2)

    def test_build_incremental_updates_unknowns_and_quality_failures(self):
        with tempfile.TemporaryDirectory(prefix='marts-fixture-') as directory:
            work = Path(directory)
            database = work / 'fixture.duckdb'
            (work / 'profiles.yml').write_text(
                'duckdb_warehouse:\n  target: test\n  outputs:\n    test:\n'
                f"      type: duckdb\n      path: '{database.as_posix()}'\n"
                '      schema: main\n      threads: 1\n', encoding='utf-8'
            )
            with duckdb.connect(str(database)) as con:
                con.execute('CREATE SCHEMA intermediate')
                con.execute('''
                    CREATE TABLE intermediate.int_dim_user (
                        user_key VARCHAR, user_id INTEGER, user_last_name VARCHAR,
                        user_first_name VARCHAR, user_gender VARCHAR, registered_at TIMESTAMP,
                        user_agent VARCHAR, start_time TIMESTAMP, end_time TIMESTAMP, is_active BOOLEAN
                    );
                    INSERT INTO intermediate.int_dim_user VALUES
                        ('user-old', 42, NULL, NULL, NULL, NULL, NULL,
                         TIMESTAMP '2026-01-01', TIMESTAMP '2026-10-01', FALSE),
                        ('user-new', 42, 'Nguyen', 'An', 'M', TIMESTAMP '2026-01-01', 'Agent',
                         TIMESTAMP '2026-10-01', TIMESTAMP '9999-12-01', TRUE);
                    CREATE TABLE intermediate.int_dim_song (
                        song_key VARCHAR, song_artist_name VARCHAR, song_name VARCHAR,
                        song_duration_seconds DECIMAL(18,3)
                    );
                    INSERT INTO intermediate.int_dim_song VALUES ('song', NULL, '', NULL), ('song', NULL, '', NULL);
                    CREATE TABLE intermediate.int_dim_session (
                        session_key VARCHAR, session_id INTEGER, user_id INTEGER,
                        session_started_time TIMESTAMP, session_number INTEGER
                    );
                    INSERT INTO intermediate.int_dim_session VALUES ('session', NULL, NULL, NULL, NULL);
                    CREATE TABLE intermediate.int_dim_location (
                        location_key VARCHAR, location_city VARCHAR, location_zip VARCHAR,
                        location_state VARCHAR, location_lon DECIMAL(18,3), location_lat DECIMAL(18,3)
                    );
                    INSERT INTO intermediate.int_dim_location VALUES ('location', NULL, NULL, NULL, NULL, NULL);
                    CREATE TABLE intermediate.int_dim_date (
                        date_key DATE, day_of_week INTEGER, day_name VARCHAR, day_of_month INTEGER,
                        day_of_year INTEGER, week_of_year INTEGER, iso_year_number INTEGER,
                        month_number INTEGER, month_name VARCHAR, quarter_number INTEGER,
                        year_number INTEGER, is_weekend BOOLEAN
                    );
                    INSERT INTO intermediate.int_dim_date VALUES
                        (DATE '2026-10-14', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
                    CREATE TABLE intermediate.int_dim_time (
                        time_key INTEGER, time_value TIME, hour_number INTEGER, minute_number INTEGER,
                        second_number INTEGER, hour_12 INTEGER, am_pm VARCHAR, minute_of_day INTEGER,
                        second_of_day INTEGER, time_period VARCHAR
                    );
                    INSERT INTO intermediate.int_dim_time VALUES
                        (0, TIME '00:00:00', 0, 0, 0, 12, 'AM', 0, 0, 'Night'),
                        (28801, TIME '08:00:01', 8, 0, 1, 8, 'AM', 480, 28801, 'Morning');
                    CREATE TABLE intermediate.int_fact_song_play (
                        play_key VARCHAR, user_id INTEGER, session_id INTEGER, item_in_session INTEGER,
                        user_key VARCHAR, song_key VARCHAR, session_key VARCHAR, location_key VARCHAR,
                        play_date_key DATE, play_time_key INTEGER, play_started_at TIMESTAMP,
                        play_ended_at TIMESTAMP, listened_seconds DOUBLE
                    );
                    INSERT INTO intermediate.int_fact_song_play VALUES
                        ('play-1', 42, 7, 0, 'user-old', 'song', 'session', 'location', DATE '2026-10-14',
                         28801, TIMESTAMP '2026-10-14 08:00:01.123', TIMESTAMP '2026-10-14 08:00:02.373', 1.25),
                        ('play-2', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                         TIMESTAMP '2026-10-14 00:00:00.001', NULL, NULL);
                ''')

            base = [shutil.which('dbt') or 'dbt', 'build', '--project-dir', str(PROJECT),
                    '--profiles-dir', str(work), '--target-path', str(work / 'target'),
                    '--log-path', str(work / 'logs'), '--no-use-colors']

            def build(selection='path:models/marts', success=True):
                result = subprocess.run(base + ['--select', selection], capture_output=True, text=True, timeout=180)
                if success:
                    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                else:
                    self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                return json.loads((work / 'target/run_results.json').read_text()), result.stdout + result.stderr

            def snapshot():
                with duckdb.connect(str(database), read_only=True) as con:
                    return con.execute('SELECT * FROM marts.fact_song_play ORDER BY play_key').fetchall()

            build()
            first = snapshot()
            with duckdb.connect(str(database), read_only=True) as con:
                self.assertEqual(con.execute('''
                    SELECT user_key, song_key, session_key, location_key, play_date_key, play_time_key,
                           user_id, session_id, item_in_session, play_ended_at, listened_seconds
                    FROM marts.fact_song_play WHERE play_key = 'play-2'
                ''').fetchone(), ('-1', '-1', '-1', '-1', datetime(1970, 1, 1).date(), -1,
                                  None, None, None, None, None))
                self.assertEqual(con.execute('''
                    SELECT song_artist_name, song_name, song_duration_seconds
                    FROM marts.dim_song WHERE song_key = 'song'
                ''').fetchone(), ('Unknown', '', -1))
                self.assertEqual(con.execute('SELECT COUNT(*) FROM marts.dim_user').fetchone()[0], 3)
                self.assertEqual(con.execute('SELECT COUNT(*) FROM marts.dim_song').fetchone()[0], 2)
                self.assertEqual(con.execute('''
                    SELECT session_id, user_id, session_started_time, session_number
                    FROM marts.dim_session WHERE session_key = 'session'
                ''').fetchone(), (-1, -1, datetime(1970, 1, 1), -1))
                self.assertEqual(con.execute('''
                    SELECT user_last_name, user_first_name, user_gender, registered_at, user_agent, is_active
                    FROM marts.dim_user WHERE user_key = 'user-old'
                ''').fetchone(), ('Unknown', 'Unknown', 'Unknown', datetime(1970, 1, 1), 'Unknown', False))
                self.assertEqual(con.execute('''
                    SELECT location_city, location_zip, location_state, location_lon, location_lat
                    FROM marts.dim_location WHERE location_key = 'location'
                ''').fetchone(), ('Unknown', 'Unknown', 'Unknown', -1, -1))
                self.assertEqual(con.execute("SELECT typeof(time_key), typeof(time_value) FROM marts.dim_time LIMIT 1").fetchone(), ('INTEGER', 'TIME'))
                dim_audit = con.execute('SELECT inserted_time FROM marts.dim_time LIMIT 1').fetchone()[0]
                self.assertEqual(first[0][-4], dim_audit)

            manifest = json.loads((work / 'target/manifest.json').read_text())
            config = manifest['nodes']['model.duckdb_warehouse.fact_song_play']['config']
            self.assertEqual((config['materialized'], config['unique_key'], config['incremental_strategy']),
                             ('incremental', 'play_key', 'delete+insert'))
            build()
            self.assertEqual(first, snapshot())
            with duckdb.connect(str(database), read_only=True) as con:
                self.assertNotEqual(dim_audit, con.execute('SELECT inserted_time FROM marts.dim_time LIMIT 1').fetchone()[0])
            self.assertIn('delete from', (work / 'logs/dbt.log').read_text().lower())

            # A new play is inserted, an old play's late duration and SCD key change, one play is unchanged.
            with duckdb.connect(str(database)) as con:
                con.execute('''
                    INSERT INTO intermediate.int_fact_song_play
                    SELECT * REPLACE ('play-3' AS play_key, 1 AS item_in_session)
                    FROM intermediate.int_fact_song_play WHERE play_key = 'play-1';
                    UPDATE intermediate.int_fact_song_play SET user_key = 'user-new',
                        play_ended_at = TIMESTAMP '2026-10-14 08:00:01.623', listened_seconds = 0.5
                    WHERE play_key = 'play-1';
                ''')
            build()
            changed = snapshot()
            self.assertEqual(len(changed), 3)
            self.assertEqual(changed[0][4], 'user-new')
            self.assertEqual(changed[0][12], 0.5)
            self.assertEqual(first[1], changed[1])
            self.assertNotEqual(first[0][-4], changed[0][-4])
            self.assertEqual(changed[0][-4], changed[0][-2])
            self.assertEqual(changed[0][-4], changed[2][-4])
            build()
            self.assertEqual(changed, snapshot())

            # A dimension arriving later changes an old NULL FK without a time watermark.
            with duckdb.connect(str(database)) as con:
                con.execute("UPDATE intermediate.int_fact_song_play SET song_key = 'song' WHERE play_key = 'play-2'")
            build()
            arrived = snapshot()
            self.assertEqual(arrived[0], changed[0])
            self.assertEqual(arrived[2], changed[2])
            self.assertEqual(arrived[1][5], 'song')
            self.assertNotEqual(arrived[1][-4], changed[1][-4])

            # Source deletes are detected, never silently propagated as data destruction.
            with duckdb.connect(str(database)) as con:
                con.execute("DELETE FROM intermediate.int_fact_song_play WHERE play_key = 'play-3'")
            results, output = build('assert_marts_fact_preserved', success=False)
            self.assertTrue(any(r['status'] == 'fail' for r in results['results']), output)

            # Real model SQL must reject NULL/reserved keys without coercing primary keys.
            env = Environment()
            macro = (PROJECT / 'macros/marts_audit_columns.sql').read_text(encoding='utf-8')
            with duckdb.connect(str(database)) as con:
                for dim, key, expression in [
                    ('song', 'song_key', "'-1'"), ('session', 'session_key', 'NULL'),
                    ('date', 'date_key', "DATE '1970-01-01'")
                ]:
                    with self.subTest(invalid_dimension=dim):
                        con.execute(f'CREATE TEMP TABLE backup AS SELECT * FROM intermediate.int_dim_{dim}')
                        con.execute(f'UPDATE intermediate.int_dim_{dim} SET {key} = {expression}')
                        sql = env.from_string(macro + '\n' + (PROJECT / f'models/marts/dim_{dim}.sql').read_text()).render(
                            config=lambda **kwargs: '', ref=lambda name: f'intermediate.{name}',
                            run_started_at=datetime.now(timezone.utc)
                        )
                        with self.assertRaisesRegex(duckdb.InvalidInputException, 'Invalid or reserved dimension key'):
                            con.execute(sql).fetchall()
                        con.execute(f'DELETE FROM intermediate.int_dim_{dim}')
                        con.execute(f'INSERT INTO intermediate.int_dim_{dim} SELECT * FROM backup')
                        con.execute('DROP TABLE backup')

                # Reject invalid source identity before incremental delete+insert can write duplicates.
                fact_sql = env.from_string(macro + '\n' + (PROJECT / 'models/marts/fact_song_play.sql').read_text()).render(
                    config=lambda **kwargs: '', ref=lambda name: f'intermediate.{name}',
                    is_incremental=lambda: False, run_started_at=datetime.now(timezone.utc)
                )
                for mutation in [
                    'INSERT INTO intermediate.int_fact_song_play SELECT * FROM intermediate.int_fact_song_play LIMIT 1',
                    "UPDATE intermediate.int_fact_song_play SET play_key = NULL WHERE play_key = 'play-1'",
                ]:
                    with self.subTest(invalid_fact_identity=mutation):
                        con.execute('CREATE TEMP TABLE backup_fact AS SELECT * FROM intermediate.int_fact_song_play')
                        con.execute(mutation)
                        with self.assertRaisesRegex(duckdb.InvalidInputException, 'NULL or duplicate source play_key'):
                            con.execute(fact_sql).fetchall()
                        con.execute('DELETE FROM intermediate.int_fact_song_play')
                        con.execute('INSERT INTO intermediate.int_fact_song_play SELECT * FROM backup_fact')
                        con.execute('DROP TABLE backup_fact')

            # Orphan FKs must be reported, not silently changed to the Unknown member.
            with duckdb.connect(str(database)) as con:
                con.execute("UPDATE intermediate.int_fact_song_play SET location_key = 'orphan' WHERE play_key = 'play-1'")
            results, output = build('fact_song_play', success=False)
            failures = [r['unique_id'] for r in results['results'] if r['status'] == 'fail']
            self.assertTrue(any('relationships_fact_song_play_location_key' in name for name in failures), output)

            # Conflicting versions of a dimension key must fail uniqueness.
            with duckdb.connect(str(database)) as con:
                con.execute('''
                    INSERT INTO intermediate.int_dim_song
                    SELECT * REPLACE ('Unknown' AS song_artist_name) FROM intermediate.int_dim_song LIMIT 1;
                ''')
            results, output = build('dim_song', success=False)
            failures = [r['unique_id'] for r in results['results'] if r['status'] == 'fail']
            self.assertTrue(any('unique_dim_song_song_key' in name for name in failures), output)


if __name__ == '__main__':
    unittest.main()
