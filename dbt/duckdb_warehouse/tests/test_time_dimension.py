"""Exercise the real time model in memory without opening the warehouse."""
from pathlib import Path
import unittest

import duckdb
from jinja2 import Environment


PROJECT = Path(__file__).resolve().parents[1]


class TimeDimensionTests(unittest.TestCase):
    def test_second_grain_preserves_midnight_and_truncates_fractions(self):
        sql = Environment().from_string(
            (PROJECT / 'models/intermediate/int_dim_time.sql').read_text(encoding='utf-8-sig')
        ).render(config=lambda **kwargs: '', ref=lambda name: name)
        with duckdb.connect(':memory:') as connection:
            connection.execute('''
                CREATE TABLE stg_listen_events AS
                SELECT * FROM (VALUES
                    (TIMESTAMP '2026-10-14 00:00:00'),
                    (TIMESTAMP '2026-10-14 08:00:01.001'),
                    (TIMESTAMP '2026-10-14 08:00:01.999'),
                    (TIMESTAMP '2026-10-15 08:00:01.001'),
                    (TIMESTAMP '2026-10-14 23:59:59.999')
                ) events(ts)
            ''')
            connection.execute('CREATE TABLE result AS ' + sql)
            rows = connection.execute('''
                SELECT time_key, CAST(time_value AS VARCHAR), second_of_day, second_number
                FROM result ORDER BY time_key
            ''').fetchall()
            self.assertEqual(rows, [
                (0, '00:00:00', 0, 0),
                (28801, '08:00:01', 28801, 1),
                (86399, '23:59:59', 86399, 59),
            ])
            self.assertEqual(connection.execute(
                'SELECT typeof(time_key), typeof(time_value) FROM result LIMIT 1'
            ).fetchone(), ('INTEGER', 'TIME'))


if __name__ == '__main__':
    unittest.main()
