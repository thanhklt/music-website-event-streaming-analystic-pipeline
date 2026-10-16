"""Shared dbt dashboard fixture and helper for isolated tests."""
from datetime import datetime
import json
from pathlib import Path
import shutil
import subprocess
import tempfile

import duckdb

REPO_ROOT = Path(__file__).resolve().parents[2]
DBT_PROJECT = REPO_ROOT / 'dbt' / 'duckdb_warehouse'


def create_marts_fixture(path):
    """Populate marts fixture tables in an isolated DuckDB file."""
    with duckdb.connect(str(path)) as con:
        con.execute("""
            CREATE SCHEMA marts;
            CREATE TABLE marts.dim_user AS SELECT * FROM (VALUES
                ('u1', 'F'),
                ('u2', 'M'),
                ('u3', 'F'),
                ('u4', 'NB'),
                ('u5', ''),
                ('-1', 'Unknown')
            ) t(user_key, user_gender);
            CREATE TABLE marts.dim_song AS SELECT * FROM (VALUES
                ('s1', 'Shared title', 'Artist A'),
                ('s2', 'Shared title', 'Artist B'),
                ('-1', 'Unknown', 'Unknown')
            ) t(song_key, song_name, song_artist_name);
            CREATE TABLE marts.dim_location AS SELECT * FROM (VALUES
                ('l1', 'CA'),
                ('l2', 'NY'),
                ('l3', 'TX'),
                ('l4', '  '),
                ('-1', 'Unknown')
            ) t(location_key, location_state);
            CREATE TABLE marts.fact_song_play (
                play_key VARCHAR, user_id INTEGER, user_key VARCHAR,
                song_key VARCHAR, location_key VARCHAR, play_started_at TIMESTAMP,
                listened_seconds DOUBLE, updated_time TIMESTAMP
            );
            INSERT INTO marts.fact_song_play VALUES
                ('p1', 1, 'u1', 's1', 'l1', '2026-10-12 08:59:00', 120, '2026-10-14'),
                ('p2', 2, 'u2', 's1', 'l1', '2026-10-12 08:00:00', NULL, '2026-10-14'),
                ('p3', 2, 'u2', 's2', 'l2', '2026-10-13 10:00:00', 60, '2026-10-14'),
                ('p4', 1, 'u3', 's1', 'l1', '2026-10-13 10:30:00', 0, '2026-10-14'),
                ('p5', NULL, '-1', '-1', '-1', '2026-10-13 23:59:59', NULL, '2026-10-14'),
                ('p6', 3, 'u2', 's2', 'l2', NULL, NULL, '2026-10-14');
        """)


def build_dashboard_fixture(path, success=True, selection=None):
    """Build dashboard tables in the target database using an isolated temporary dbt project."""
    with tempfile.TemporaryDirectory(prefix='dbt-dashboard-') as temp_dir:
        work = Path(temp_dir)
        db_path = Path(path).resolve().as_posix()
        (work / 'profiles.yml').write_text(
            'duckdb_warehouse:\n'
            '  target: test\n'
            '  outputs:\n'
            '    test:\n'
            '      type: duckdb\n'
            f"      path: '{db_path}'\n"
            '      schema: main\n'
            '      threads: 1\n',
            encoding='utf-8'
        )
        (work / 'dbt_project.yml').write_text(
            "name: 'duckdb_warehouse'\n"
            "version: '1.0.0'\n"
            "profile: 'duckdb_warehouse'\n"
            'model-paths: ["models"]\n'
            'macro-paths: ["macros"]\n'
            'test-paths: ["tests"]\n'
            'vars:\n'
            "  'dbt_date:time_zone': 'UTC'\n"
            'models:\n'
            '  duckdb_warehouse:\n'
            '    dashboard:\n'
            '      +schema: dashboard\n'
            '      +materialized: table\n',
            encoding='utf-8'
        )

        macros_dir = work / 'macros'
        macros_dir.mkdir(parents=True, exist_ok=True)
        schema_macro = DBT_PROJECT / 'macros' / 'generate_schema_name.sql'
        if schema_macro.is_file():
            shutil.copy2(schema_macro, macros_dir / 'generate_schema_name.sql')

        # Create ephemeral stubs for upstream marts so dashboard models resolve refs cleanly
        marts_dir = work / 'models' / 'marts'
        marts_dir.mkdir(parents=True, exist_ok=True)
        (marts_dir / 'fact_song_play.sql').write_text(
            "{{ config(materialized='ephemeral') }}\nSELECT * FROM marts.fact_song_play\n",
            encoding='utf-8'
        )
        (marts_dir / 'dim_user.sql').write_text(
            "{{ config(materialized='ephemeral') }}\nSELECT * FROM marts.dim_user\n",
            encoding='utf-8'
        )
        (marts_dir / 'dim_song.sql').write_text(
            "{{ config(materialized='ephemeral') }}\nSELECT * FROM marts.dim_song\n",
            encoding='utf-8'
        )
        (marts_dir / 'dim_location.sql').write_text(
            "{{ config(materialized='ephemeral') }}\nSELECT * FROM marts.dim_location\n",
            encoding='utf-8'
        )

        dashboard_dir = work / 'models' / 'dashboard'
        dashboard_dir.mkdir(parents=True, exist_ok=True)
        src_dashboard = DBT_PROJECT / 'models' / 'dashboard'
        if src_dashboard.is_dir():
            for item in src_dashboard.glob('*'):
                if item.is_file():
                    shutil.copy2(item, dashboard_dir / item.name)

        tests_dir = work / 'tests'
        tests_dir.mkdir(parents=True, exist_ok=True)
        singular_test = DBT_PROJECT / 'tests' / 'assert_dashboard_song_plays_preserved.sql'
        if singular_test.is_file():
            shutil.copy2(singular_test, tests_dir / 'assert_dashboard_song_plays_preserved.sql')

        dbt_bin = shutil.which('dbt') or 'dbt'
        cmd = [
            dbt_bin, 'build',
            '--project-dir', str(work),
            '--profiles-dir', str(work),
            '--target-path', str(work / 'target'),
            '--log-path', str(work / 'logs'),
            '--no-use-colors'
        ]
        if selection:
            cmd.extend(['--select', selection])

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=180)
        output = result.stdout + result.stderr

        run_results = None
        run_results_path = work / 'target' / 'run_results.json'
        if run_results_path.is_file():
            try:
                run_results = json.loads(run_results_path.read_text(encoding='utf-8'))
            except Exception:
                pass

        if success and result.returncode != 0:
            raise RuntimeError(f'dbt build failed (exit code {result.returncode}):\n{output}')
        if not success and result.returncode == 0:
            raise AssertionError(f'dbt build unexpectedly succeeded:\n{output}')

        return run_results, output
