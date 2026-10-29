"""Exact row reconciliation, offline regression tests, and a read-only live audit.

Offline: python -m unittest discover -s tests -p test_hdfs_duckdb_quality.py -v
Live:    python tests/test_hdfs_duckdb_quality.py --live --topic listen_events
"""

import argparse
import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.parse import unquote, urlsplit

import duckdb
import requests


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config.settings import read_config
from spark.batch.functions import _fetch_webhdfs_bytes


def quote_identifier(name):
    return '"' + name.replace('"', '""') + '"'


def canonical_path(uri):
    return "/" + unquote(urlsplit(uri).path).lstrip("/")


def list_metadata(webhdfs_url, topic_path):
    """List committed log names independently of the ingestion helper."""
    response = requests.get(
        f"{webhdfs_url}{topic_path}/_spark_metadata?op=LISTSTATUS", timeout=15
    )
    response.raise_for_status()
    logs = {}
    for entry in response.json()["FileStatuses"]["FileStatus"]:
        name = entry["pathSuffix"]
        if entry.get("type", "FILE") == "FILE" and name.removesuffix(".compact").isdigit():
            logs[name] = (entry.get("length"), entry.get("modificationTime"))
    if not logs:
        raise AssertionError(f"No committed metadata logs at {topic_path}")
    return logs


def committed_files(webhdfs_url, topic_path, logs):
    """Read the latest compact snapshot and later batches; fail on unreadable logs.

    Unlike read_from_hdfs, this audit never skips malformed metadata or a failed
    download. Otherwise the same missing file could disappear from both sides.
    """
    compact = [name for name in logs if name.endswith(".compact")]
    latest = max(compact, key=lambda name: int(name.removesuffix(".compact"))) if compact else None
    boundary = int(latest.removesuffix(".compact")) if latest else -1
    selected = ([latest] if latest else []) + sorted(
        (name for name in logs if name.isdigit() and int(name) > boundary), key=int
    )
    files = {}
    for name in selected:
        url = f"{webhdfs_url}{topic_path}/_spark_metadata/{name}?op=OPEN"
        lines = _fetch_webhdfs_bytes(url).decode("utf-8").splitlines()
        if not lines or lines[0] != "v1":
            raise AssertionError(f"Unsupported or empty metadata log: {name}")
        for line in lines[1:]:
            record = json.loads(line)
            path = canonical_path(record["path"])
            if not path.startswith(topic_path + "/"):
                raise AssertionError(f"Metadata path is outside topic: {path}")
            if record["action"] == "add":
                files[path] = record.get("size")
            elif record["action"] == "delete":
                files.pop(path, None)
            else:
                raise AssertionError(f"Unknown metadata action in {name}")
    if not files:
        raise AssertionError(f"No committed source files at {topic_path}")
    return files


def compare_rows(connection, parquet_paths, topic):
    """Compare full rows with multiplicity; NULLs and legitimate duplicates count.

    EXCEPT ALL is exact: no sampling, row hashing, or DISTINCT that could hide a
    lost duplicate. Only columns physically stored in Parquet are expected,
    matching ingestion with hive_partitioning=False.
    """
    source = connection.read_parquet(
        [str(path) for path in parquet_paths], hive_partitioning=False, union_by_name=True
    )
    connection.execute("CREATE OR REPLACE TEMP VIEW _quality_source AS " + source.sql_query())
    destination = "landing." + quote_identifier(topic)
    target = connection.sql(f"SELECT * FROM {destination}")
    source_schema = dict(zip(source.columns, map(str, source.types)))
    target_schema = dict(zip(target.columns, map(str, target.types)))
    schema_differences = {
        name: {"hdfs": source_schema.get(name), "duckdb": target_schema.get(name)}
        for name in sorted(source_schema.keys() | target_schema.keys())
        if source_schema.get(name) != target_schema.get(name)
    }
    source_rows = source.count("*").fetchone()[0]
    target_rows = target.count("*").fetchone()[0]
    def difference_counts(column_names):
        if not set(column_names) <= target_schema.keys():
            return None, None
        columns = ", ".join(quote_identifier(column) for column in column_names)
        counts = []
        for left, right in (("_quality_source", destination), (destination, "_quality_source")):
            counts.append(connection.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT {columns} FROM {left}
                    EXCEPT ALL
                    SELECT {columns} FROM {right}
                ) AS differences
            """).fetchone()[0])
        return tuple(counts)

    missing_rows, extra_rows = difference_counts(source.columns)
    return {
        "topic": topic,
        "hdfs_rows": source_rows,
        "duckdb_rows": target_rows,
        "missing_or_changed_rows": missing_rows,
        "extra_or_changed_rows": extra_rows,
        "schema_differences": schema_differences,
        "passed": source_rows == target_rows and missing_rows == 0
        and extra_rows == 0 and not schema_differences,
    }


def audit_topic(topic, database_path, webhdfs_url, hdfs_base_path):
    """Audit a stable committed source snapshot against a read-only DB transaction.

    Only a temporary directory and a temporary SQL view are created. The audit
    never runs ingestion, changes warehouse records, or stops Docker services.
    """
    database_path = Path(database_path)
    if not database_path.is_file():
        raise AssertionError(f"Warehouse does not exist: {database_path}")
    webhdfs_url = webhdfs_url.rstrip("/")
    topic_path = hdfs_base_path.rstrip("/") + "/" + topic
    with duckdb.connect(str(database_path), read_only=True) as connection:
        connection.execute("BEGIN TRANSACTION")
        ledger_paths = {
            canonical_path(row[0]) for row in connection.execute(
                "SELECT file_path FROM landing.ingestion_ledger WHERE topic = ? AND status = 'SUCCESS'",
                [topic],
            ).fetchall()
        }
        logs = list_metadata(webhdfs_url, topic_path)
        inventory = committed_files(webhdfs_url, topic_path, logs)
        with tempfile.TemporaryDirectory(prefix="hdfs-duckdb-quality-") as temporary:
            root = Path(temporary).resolve()
            local_files = []
            for source_path, expected_size in inventory.items():
                # Keep source-relative paths; read_parquet does not infer Hive columns.
                relative = source_path[len(topic_path) + 1:]
                local_path = (root / relative).resolve()
                if not local_path.is_relative_to(root):
                    raise AssertionError(f"Source path escapes temporary directory: {source_path}")
                data = _fetch_webhdfs_bytes(f"{webhdfs_url}{source_path}?op=OPEN")
                if expected_size is not None and len(data) != expected_size:
                    raise AssertionError(f"Downloaded size differs from metadata: {source_path}")
                local_path.parent.mkdir(parents=True, exist_ok=True)
                local_path.write_bytes(data)
                local_files.append(local_path)
            report = compare_rows(connection, local_files, topic)
        report.update({
            "hdfs_files": len(inventory),
            "success_ledger_files": len(ledger_paths),
            "files_missing_from_ledger": sorted(inventory.keys() - ledger_paths),
            "ledger_files_absent_from_source": sorted(ledger_paths - inventory.keys()),
        })
        # A changing source is inconclusive, never reported as a clean pass.
        if list_metadata(webhdfs_url, topic_path) != logs:
            raise AssertionError("HDFS metadata changed during audit; rerun after ingestion catches up")
        report["passed"] = report["passed"] and not (
            report["files_missing_from_ledger"] or report["ledger_files_absent_from_source"]
        )
        return report


class HDFSDuckDBQualityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = self.root / "warehouse.duckdb"
        self.source_file = self.root / "source.parquet"
        self.connection = duckdb.connect(str(self.database))
        self.addCleanup(self.connection.close)
        self.connection.execute((PROJECT_ROOT / "infra/duckdb/init.sql").read_text(encoding="utf-8"))
        self.connection.execute("""
            CREATE TABLE expected_rows AS
            SELECT * FROM (VALUES
                (1, 'duplicate', 1.25, TIMESTAMP '2026-10-29 10:00:00'),
                (1, 'duplicate', 1.25, TIMESTAMP '2026-10-29 10:00:00'),
                (2, NULL, NULL, TIMESTAMP '2026-10-29 10:01:00'),
                (3, '', 0.0, NULL),
                (4, 'last row', 9.50, TIMESTAMP '2026-10-29 10:02:00')
            ) AS rows(id, payload, duration, event_time);
            CREATE TABLE landing.listen_events AS SELECT * FROM expected_rows;
        """)
        self.connection.execute("COPY expected_rows TO ? (FORMAT PARQUET)", [str(self.source_file)])

    def compare(self, paths=None):
        return compare_rows(self.connection, paths or [self.source_file], "listen_events")

    def test_all_rows_including_duplicates_nulls_and_timestamps_match(self):
        report = self.compare()
        self.assertTrue(report["passed"], report)
        self.assertEqual(report["hdfs_rows"], 5)
        self.assertEqual(report["missing_or_changed_rows"], 0)
        self.assertEqual(report["extra_or_changed_rows"], 0)

    def test_loss_of_one_duplicate_is_detected(self):
        self.connection.execute("DELETE FROM landing.listen_events WHERE rowid = 0")
        report = self.compare()
        self.assertFalse(report["passed"])
        self.assertEqual(report["missing_or_changed_rows"], 1)

    def test_equal_row_counts_cannot_hide_missing_and_duplicated_records(self):
        self.connection.execute("""
            DELETE FROM landing.listen_events WHERE rowid = 0;
            INSERT INTO landing.listen_events SELECT * FROM expected_rows WHERE id = 3;
        """)
        report = self.compare()
        self.assertEqual(report["hdfs_rows"], report["duckdb_rows"])
        self.assertFalse(report["passed"])
        self.assertEqual(report["missing_or_changed_rows"], 1)
        self.assertEqual(report["extra_or_changed_rows"], 1)

    def test_changed_values_are_detected(self):
        self.connection.execute("UPDATE landing.listen_events SET payload = 'changed' WHERE id = 2")
        report = self.compare()
        self.assertFalse(report["passed"])
        self.assertEqual(report["missing_or_changed_rows"], 1)
        self.assertEqual(report["extra_or_changed_rows"], 1)

    def test_type_changes_are_detected_even_if_values_match(self):
        self.connection.execute("ALTER TABLE landing.listen_events ALTER id TYPE BIGINT")
        report = self.compare()
        self.assertFalse(report["passed"])
        self.assertIn("id", report["schema_differences"])

    def test_hive_directories_do_not_add_columns_to_expected_schema(self):
        path = self.root / "year=2026" / "month=10" / "day=29" / "part.parquet"
        path.parent.mkdir(parents=True)
        path.write_bytes(self.source_file.read_bytes())
        report = self.compare([path])
        self.assertTrue(report["passed"], report)
        self.assertEqual(report["schema_differences"], {})
        self.assertEqual(report["missing_or_changed_rows"], 0)
        self.assertEqual(report["extra_or_changed_rows"], 0)

    def test_date_columns_physically_stored_in_parquet_are_still_compared(self):
        path = self.root / "year=2030" / "month=1" / "day=1" / "part.parquet"
        path.parent.mkdir(parents=True)
        self.connection.execute("""
            COPY (
                SELECT *, 2026::INTEGER AS year, 10::INTEGER AS month, 29::INTEGER AS day
                FROM expected_rows
            ) TO ? (FORMAT PARQUET)
        """, [str(path)])
        self.connection.execute("""
            CREATE OR REPLACE TABLE landing.listen_events AS
            SELECT * FROM read_parquet(?, hive_partitioning=false)
        """, [str(path)])
        self.assertTrue(self.compare([path])["passed"])
        self.connection.execute("UPDATE landing.listen_events SET year = 2025")
        report = self.compare([path])
        self.assertFalse(report["passed"])
        self.assertEqual(report["missing_or_changed_rows"], 5)
        self.assertEqual(report["extra_or_changed_rows"], 5)

    def test_empty_destination_is_detected(self):
        self.connection.execute("DELETE FROM landing.listen_events")
        report = self.compare()
        self.assertFalse(report["passed"])
        self.assertEqual(report["missing_or_changed_rows"], 5)

    def test_audit_detects_entire_source_file_missing_from_data_and_ledger(self):
        parquet = self.source_file.read_bytes()
        paths = ["/data/events/listen_events/part-a.parquet", "/data/events/listen_events/part-b.parquet"]
        self.connection.execute(
            "INSERT INTO landing.ingestion_ledger (file_path, topic) VALUES (?, 'listen_events')",
            [paths[0]],
        )
        self.connection.close()
        metadata = ("v1\n" + "\n".join(json.dumps({
            "action": "add", "path": path, "size": len(parquet)
        }) for path in paths)).encode()

        def response_for(url, **kwargs):
            response = Mock(status_code=200)
            if url.endswith("op=LISTSTATUS"):
                response.json.return_value = {"FileStatuses": {"FileStatus": [
                    {"pathSuffix": "0", "type": "FILE"}
                ]}}
            else:
                response.content = parquet if url.endswith(".parquet?op=OPEN") else metadata
            return response

        with patch("requests.get", side_effect=response_for):
            report = audit_topic(
                "listen_events", self.database, "http://host/webhdfs/v1", "/data/events"
            )
        self.assertFalse(report["passed"])
        self.assertEqual(report["hdfs_rows"], 10)
        self.assertEqual(report["duckdb_rows"], 5)
        self.assertEqual(report["missing_or_changed_rows"], 5)
        self.assertEqual(report["files_missing_from_ledger"], [paths[1]])

    def test_unreadable_source_file_cannot_produce_a_passing_audit(self):
        self.connection.close()
        metadata = ("v1\n" + json.dumps({
            "action": "add", "path": "/data/events/listen_events/missing.parquet"
        })).encode()
        listing = Mock()
        listing.json.return_value = {"FileStatuses": {"FileStatus": [
            {"pathSuffix": "0", "type": "FILE"}
        ]}}
        with (
            patch("requests.get", return_value=listing),
            patch(__name__ + "._fetch_webhdfs_bytes", side_effect=[
                metadata, requests.HTTPError("source file unavailable")
            ]),
            self.assertRaises(requests.HTTPError),
        ):
            audit_topic("listen_events", self.database, "http://host/webhdfs/v1", "/data/events")

    def test_ingestion_and_rerun_preserve_all_rows_and_ledger(self):
        from spark.batch.hdfs_to_duckdb import run_ingestion

        parquet = self.source_file.read_bytes()
        self.connection.execute("DROP TABLE landing.listen_events")
        self.connection.close()
        path = "/data/events/listen_events/part.parquet"
        metadata = ("v1\n" + json.dumps({"action": "add", "path": path, "size": len(parquet)})).encode()

        def response_for(url, **kwargs):
            response = Mock(status_code=200)
            if url.endswith("op=LISTSTATUS"):
                response.json.return_value = {"FileStatuses": {"FileStatus": [
                    {"pathSuffix": "0", "type": "FILE"},
                    {"pathSuffix": "9.compact", "type": "FILE"},
                ]}}
            elif url.endswith(".parquet?op=OPEN"):
                response.content = parquet
            else:
                response.content = metadata
            return response

        with patch("requests.get", side_effect=response_for), redirect_stdout(io.StringIO()):
            arguments = dict(
                duckdb_path=str(self.database), topics="listen_events",
                hdfs_base_path="/data/events", webhdfs_url="http://host/webhdfs/v1",
                staging_dir=str(self.root / "staging"),
            )
            self.assertEqual(run_ingestion(**arguments), {"listen_events": 5})
            self.assertEqual(run_ingestion(**arguments), {})
            report = audit_topic(
                "listen_events", self.database, "http://host/webhdfs/v1", "/data/events"
            )
        self.assertTrue(report["passed"], report)
        self.assertEqual(report["hdfs_files"], 1)
        self.assertEqual(report["hdfs_rows"], 5)
        self.assertEqual(report["files_missing_from_ledger"], [])


def live_main():
    parser = argparse.ArgumentParser(description="Read-only HDFS to DuckDB data quality audit")
    parser.add_argument("--live", action="store_true", required=True)
    parser.add_argument("--topic", required=True, help="One topic, for example listen_events")
    args = parser.parse_args()
    settings = read_config()
    try:
        report = audit_topic(
            args.topic, (PROJECT_ROOT / settings.DUCKDB_PATH).resolve(),
            settings.HDFS_NAMENODE_HTTP.rstrip("/") + "/webhdfs/v1", settings.HDFS_BASE_PATH,
        )
    except Exception as exc:
        print(json.dumps({"topic": args.topic, "passed": False, "error": str(exc)}, indent=2))
        return 1
    print(json.dumps(report, indent=2))
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    if "--live" in sys.argv:
        raise SystemExit(live_main())
    unittest.main()
