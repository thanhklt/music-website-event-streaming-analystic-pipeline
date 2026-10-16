"""Hand-counted dashboard fixture; never writes to the real warehouse."""
from datetime import date
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import duckdb

APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP_DIR))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from dashboard_fixture import build_dashboard_fixture, create_marts_fixture


class ReportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.work = tempfile.TemporaryDirectory(prefix='music-dashboard-')
        cls.path = Path(cls.work.name) / 'fixture.duckdb'
        create_marts_fixture(cls.path)
        build_dashboard_fixture(cls.path)

    @classmethod
    def tearDownClass(cls):
        cls.work.cleanup()

    def get_module(self):
        self.assertTrue((APP_DIR / 'reports.py').is_file(), 'Dashboard report implementation is missing')
        import reports
        return reports

    def test_counts_duration_unknowns_and_scd_versions(self):
        module = self.get_module()
        meta = module.load_metadata(self.path)
        self.assertEqual(meta['total_rows'], 6)
        self.assertEqual(meta['missing_timestamp'], 1)
        report = module.load_report(self.path, date(2026, 10, 12), date(2026, 10, 13))
        kpi = report['summary'].iloc[0]
        self.assertEqual(kpi['plays'], 5)
        self.assertEqual(kpi['listeners'], 2)  # user 1 has two SCD versions
        self.assertEqual(kpi['known_seconds'], 180)
        self.assertEqual(kpi['duration_missing'], 2)
        self.assertEqual(kpi['unknown_location'], 1)
        self.assertEqual(report['states']['state'].tolist(), ['CA', 'NY'])
        self.assertEqual(report['states']['plays'].tolist(), [3, 1])
        self.assertEqual(report['songs']['plays'].tolist(), [3, 1])
        self.assertEqual(report['songs']['artist'].tolist(), ['Artist A', 'Artist B'])
        heat = report['hourly']
        self.assertEqual(len(heat), 168)
        self.assertEqual(heat['plays'].sum(), 5)
        self.assertEqual(heat['known_seconds'].sum(), 180)
        self.assertEqual(heat.query('weekday == 1 and hour == 8').iloc[0]['known_seconds'], 120)
        gender = report['gender_songs']
        self.assertEqual(gender.query("gender == 'F'")['plays'].tolist(), [2])
        # Male winner is tied: deterministic title/artist/key tie break gives Artist A.
        self.assertEqual(gender.query("gender == 'M'")['artist'].tolist(), ['Artist A'])
        # Connections must be closed so the next pipeline writer can acquire the database.
        with duckdb.connect(str(self.path)) as con:
            self.assertEqual(con.execute('SELECT COUNT(*) FROM dashboard.song_plays').fetchone()[0], 6)

    def test_date_state_and_gender_filters_and_empty_result(self):
        module = self.get_module()
        report = module.load_report(self.path, date(2026, 10, 13), date(2026, 10, 13), ('CA',), ('F',))
        self.assertEqual(report['summary'].iloc[0]['plays'], 1)
        self.assertEqual(report['summary'].iloc[0]['known_seconds'], 0)
        ny = module.load_report(self.path, date(2026, 10, 12), date(2026, 10, 13), ('NY',))
        self.assertEqual(ny['daily']['plays'].tolist(), [0, 1])
        empty = module.load_report(self.path, date(2026, 11, 1), date(2026, 11, 2))
        self.assertEqual(empty['summary'].iloc[0]['plays'], 0)
        self.assertEqual(empty['summary'].iloc[0]['known_seconds'], 0)
        self.assertTrue(empty['songs'].empty)

    def test_missing_database_does_not_create_file(self):
        module = self.get_module()
        missing = Path(self.work.name) / 'missing.duckdb'
        with self.assertRaises(FileNotFoundError):
            module.load_metadata(missing)
        self.assertFalse(missing.exists())

    def test_reader_works_when_marts_are_dropped_after_build(self):
        """Prove that reader does not scan or join marts tables at runtime."""
        module = self.get_module()
        with tempfile.TemporaryDirectory(prefix='nomarts-') as tmpdir:
            isolated_db = Path(tmpdir) / 'nomarts.duckdb'
            create_marts_fixture(isolated_db)
            build_dashboard_fixture(isolated_db)

            # Drop the marts schema completely
            with duckdb.connect(str(isolated_db)) as con:
                con.execute('DROP SCHEMA marts CASCADE')

            meta = module.load_metadata(isolated_db)
            self.assertEqual(meta['total_rows'], 6)
            self.assertEqual(meta['states'], ['CA', 'NY', 'TX', 'Unknown'])
            self.assertEqual(meta['genders'], ['F', 'M', 'NB', 'Unknown'])

            report = module.load_report(isolated_db, date(2026, 10, 12), date(2026, 10, 13))
            self.assertEqual(report['summary'].iloc[0]['plays'], 5)
            self.assertEqual(report['summary'].iloc[0]['listeners'], 2)

    def test_missing_dashboard_tables_and_corrupt_keys(self):
        """Reader requires dashboard schema and fails clearly when missing or corrupted."""
        module = self.get_module()
        with tempfile.TemporaryDirectory(prefix='corrupt-') as tmpdir:
            db_path = Path(tmpdir) / 'corrupt.duckdb'
            with duckdb.connect(str(db_path)) as con:
                con.execute('CREATE TABLE foo (id INT)')

            # Missing dashboard tables must raise ValueError with helpful build advice
            with self.assertRaises(ValueError) as ctx:
                module.load_metadata(db_path)
            self.assertIn('dashboard', str(ctx.exception).lower())

            # Corrupt duplicate play_key in dashboard.song_plays
            create_marts_fixture(db_path)
            build_dashboard_fixture(db_path)
            with duckdb.connect(str(db_path)) as con:
                con.execute("INSERT INTO dashboard.song_plays SELECT * FROM dashboard.song_plays WHERE play_key = 'p1'")

            with self.assertRaises(ValueError) as ctx:
                module.load_report(db_path, date(2026, 10, 12), date(2026, 10, 13))
            self.assertIn('play_key', str(ctx.exception))

    def test_filter_combinations_and_parameter_safety(self):
        """Test parameter binding, quotes in filter values, midnight timestamps and combinations."""
        module = self.get_module()
        # Event p5 is at 23:59:59 on 2026-10-13, must be included when end_date is 2026-10-13
        rep = module.load_report(self.path, date(2026, 10, 13), date(2026, 10, 13))
        self.assertEqual(rep['summary'].iloc[0]['plays'], 3)  # p3, p4, p5

        # Unknown filtering
        rep_unknown = module.load_report(self.path, date(2026, 10, 12), date(2026, 10, 13), states=('Unknown',))
        self.assertEqual(rep_unknown['summary'].iloc[0]['plays'], 1)  # p5

        # Filter string with single quotes via parameter binding should not break SQL
        rep_quote = module.load_report(self.path, date(2026, 10, 12), date(2026, 10, 13), states=("O'Fallon",))
        self.assertEqual(rep_quote['summary'].iloc[0]['plays'], 0)

        # Multi-day distinct users: user 1 played on 12th (p1) and 13th (p4), user 2 played on 12th (p2) and 13th (p3)
        self.assertEqual(rep['summary'].iloc[0]['listeners'], 2)

    def test_duration_missing_vs_zero_and_empty_ranges(self):
        """Duration NULL vs 0 produce total 0, but duration_missing and measured_plays differ."""
        module = self.get_module()
        # On 2026-10-13, p4 has listened_seconds = 0, p3 has 60, p5 has NULL
        # Filter only CA (p4) -> 1 play with duration 0
        ca = module.load_report(self.path, date(2026, 10, 13), date(2026, 10, 13), states=('CA',))
        self.assertEqual(ca['summary'].iloc[0]['known_seconds'], 0)
        self.assertEqual(ca['summary'].iloc[0]['duration_missing'], 0)

        # Filter only Unknown (p5) -> 1 play with duration NULL
        unk = module.load_report(self.path, date(2026, 10, 13), date(2026, 10, 13), states=('Unknown',))
        self.assertEqual(unk['summary'].iloc[0]['known_seconds'], 0)
        self.assertEqual(unk['summary'].iloc[0]['duration_missing'], 1)

        # Empty range produces 168 rows of zeros in hourly, empty ranking
        empty = module.load_report(self.path, date(2025, 1, 1), date(2025, 1, 1))
        self.assertEqual(len(empty['hourly']), 168)
        self.assertEqual(empty['hourly']['plays'].sum(), 0)
        self.assertEqual(empty['daily']['plays'].sum(), 0)
        self.assertTrue(empty['artists'].empty)


class DashboardTests(unittest.TestCase):
    def test_render_filters_empty_result_and_refresh(self):
        self.assertTrue((APP_DIR / 'app.py').is_file(), 'Streamlit dashboard is missing')
        from streamlit.testing.v1 import AppTest
        with tempfile.TemporaryDirectory(prefix='music-ui-') as directory:
            path = Path(directory) / 'fixture.duckdb'
            create_marts_fixture(path)
            build_dashboard_fixture(path)
            with patch.dict(os.environ, {'DASHBOARD_DUCKDB_PATH': str(path)}):
                app = AppTest.from_file(str(APP_DIR / 'app.py'), default_timeout=30).run()
                self.assertFalse(app.exception)
                self.assertEqual(app.metric[0].value, '5')
                app.multiselect(key='states').set_value(['NY']).run()
                self.assertFalse(app.exception)
                self.assertEqual(app.metric[0].value, '1')
                app.multiselect(key='genders').set_value(['F']).run()
                self.assertFalse(app.exception)
                self.assertTrue(any('Không có lượt nghe' in x.value for x in app.info))
                app.multiselect(key='states').set_value([])
                app.multiselect(key='genders').set_value([]).run()
                with duckdb.connect(str(path)) as con:
                    con.execute("UPDATE marts.fact_song_play SET listened_seconds = 3600 WHERE play_key = 'p1'")
                # Rebuild dashboard before refresh
                build_dashboard_fixture(path)
                app.button(key='refresh').click().run()
                self.assertFalse(app.exception)
                self.assertEqual(app.metric[3].value, '1,02')

    def test_missing_database_has_helpful_error(self):
        self.assertTrue((APP_DIR / 'app.py').is_file(), 'Streamlit dashboard is missing')
        from streamlit.testing.v1 import AppTest
        with patch.dict(os.environ, {'DASHBOARD_DUCKDB_PATH': str(APP_DIR / 'does-not-exist.duckdb')}):
            app = AppTest.from_file(str(APP_DIR / 'app.py'), default_timeout=30).run()
            self.assertFalse(app.exception)
            self.assertTrue(app.error)


if __name__ == '__main__':
    unittest.main()
