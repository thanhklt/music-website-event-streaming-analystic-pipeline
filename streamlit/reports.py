"""Read-only, SQL-aggregated reports over the dbt dashboard tables.

One dashboard row is one play. Duration comes unchanged from the fact; NULL
duration is counted separately. Hours attribute duration to the play start.
"""
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE = ROOT / 'infra/duckdb/data/warehouse.duckdb'


@contextmanager
def _reader(database):
    path = Path(database).expanduser()
    if not path.is_absolute():
        path = ROOT / path
    if not path.is_file():
        raise FileNotFoundError(f'Không tìm thấy DuckDB: {path}')
    con = duckdb.connect(str(path), read_only=True)
    try:
        con.execute('BEGIN TRANSACTION')
        # Check required dashboard tables exist
        tables = [r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'dashboard'"
        ).fetchall()]
        if 'song_plays' not in tables or 'filter_options' not in tables:
            raise ValueError('Không tìm thấy bảng dashboard. Hãy chạy dbt build tầng dashboard.')

        # Refuse fan-out or NULL keys instead of silently inflating the reported play counts.
        count, unique_count = con.execute(
            'SELECT COUNT(*), COUNT(DISTINCT play_key) FROM dashboard.song_plays'
        ).fetchone()
        if count != unique_count:
            raise ValueError('dashboard.song_plays.play_key có khóa NULL/trùng; hãy kiểm tra dbt tests.')
        yield con
        con.execute('COMMIT')
    finally:
        con.close()


def load_metadata(database):
    with _reader(database) as con:
        row = con.execute('''
            SELECT COUNT(*), COUNT(*) FILTER (WHERE play_started_at IS NULL),
                   MIN(play_date), MAX(play_date),
                   MAX(updated_time), MAX(play_started_at)
            FROM dashboard.song_plays
        ''').fetchone()
        return {
            'total_rows': row[0], 'missing_timestamp': row[1],
            'min_date': row[2], 'max_date': row[3], 'updated_at': row[4],
            'latest_event': row[5],
            'states': [r[0] for r in con.execute(
                "SELECT filter_value FROM dashboard.filter_options WHERE filter_name = 'states' ORDER BY 1"
            ).fetchall()],
            'genders': [r[0] for r in con.execute(
                "SELECT filter_value FROM dashboard.filter_options WHERE filter_name = 'genders' ORDER BY 1"
            ).fetchall()],
        }


def load_report(database, start_date, end_date, states=(), genders=()):
    """Empty selections mean all; inclusive UTC dates, bound SQL parameters."""
    if start_date > end_date:
        raise ValueError('Ngày bắt đầu phải trước hoặc bằng ngày kết thúc.')
    conditions = ['play_started_at >= ?', 'play_started_at < ?']
    params = [start_date, end_date + timedelta(days=1)]
    for column, values in [('state', states), ('gender', genders)]:
        if values:
            conditions.append(column + ' IN (' + ','.join('?' for _ in values) + ')')
            params.extend(values)
    base = '''WITH plays AS (
        SELECT play_key, user_id, song_key, location_key,
               play_started_at, listened_seconds,
               state, gender, song, artist,
               weekday, hour
        FROM dashboard.song_plays
        WHERE ''' + ' AND '.join(conditions) + ') '
    queries = {
        'summary': '''SELECT COUNT(*) AS plays,
            COUNT(DISTINCT user_id) FILTER (WHERE user_id IS NOT NULL AND user_id != -1) AS listeners,
            COUNT(DISTINCT song_key) FILTER (WHERE song_key != '-1') AS songs,
            COALESCE(SUM(listened_seconds), 0) AS known_seconds,
            COUNT(*) FILTER (WHERE listened_seconds IS NULL) AS duration_missing,
            COUNT(*) FILTER (WHERE state = 'Unknown') AS unknown_location,
            COUNT(*) FILTER (WHERE song_key = '-1') AS unknown_song,
            COUNT(*) FILTER (WHERE gender = 'Unknown') AS unknown_gender
            FROM plays''',
        'states': '''SELECT state, COUNT(*) AS plays FROM plays
            WHERE state != 'Unknown' GROUP BY state ORDER BY plays DESC, state LIMIT 5''',
        'songs': '''SELECT song_key, song, artist, COUNT(*) AS plays FROM plays
            WHERE song_key != '-1' GROUP BY song_key, song, artist
            ORDER BY plays DESC, song, artist, song_key LIMIT 5''',
        'gender_songs': '''SELECT gender, song_key, song, artist, plays FROM (
            SELECT gender, song_key, song, artist, COUNT(*) AS plays,
                ROW_NUMBER() OVER (PARTITION BY gender ORDER BY COUNT(*) DESC, song, artist, song_key) AS rank
            FROM plays WHERE song_key != '-1' GROUP BY gender, song_key, song, artist
            ) ranked WHERE rank = 1 ORDER BY gender''',
        'hourly': ''', hourly AS (
            SELECT weekday, hour,
                   COUNT(*) AS plays, COALESCE(SUM(listened_seconds), 0) AS known_seconds,
                   COUNT(listened_seconds) AS measured_plays
            FROM plays GROUP BY weekday, hour
            ) SELECT d.weekday, h.hour, COALESCE(a.plays, 0) AS plays,
                     COALESCE(a.known_seconds, 0) AS known_seconds,
                     COALESCE(a.measured_plays, 0) AS measured_plays
              FROM range(1, 8) d(weekday) CROSS JOIN range(0, 24) h(hour)
              LEFT JOIN hourly a ON d.weekday = a.weekday AND h.hour = a.hour
              ORDER BY d.weekday, h.hour''',
        'daily': ''', daily AS (SELECT CAST(play_started_at AS DATE) AS date, COUNT(*) AS plays,
            COALESCE(SUM(listened_seconds), 0) / 3600.0 AS hours
            FROM plays GROUP BY date)
            SELECT CAST(d.day AS DATE) AS date, COALESCE(a.plays, 0) AS plays,
                   COALESCE(a.hours, 0) AS hours
            FROM generate_series(CAST(? AS TIMESTAMP), CAST(? AS TIMESTAMP), INTERVAL '1 day') d(day)
            LEFT JOIN daily a ON a.date = CAST(d.day AS DATE) ORDER BY date''',
        'artists': '''SELECT artist, COUNT(*) AS plays FROM plays
            WHERE song_key != '-1' AND artist != 'Unknown'
            GROUP BY artist ORDER BY plays DESC, artist LIMIT 5''',
    }
    with _reader(database) as con:
        return {name: con.execute(
            base + sql, params + ([start_date, end_date] if name == 'daily' else [])
        ).fetchdf() for name, sql in queries.items()}
