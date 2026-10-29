"""Regression checks for the batch helper/orchestration boundary."""

import importlib.util
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from dataclasses import replace
from pathlib import Path
from unittest.mock import Mock, call, patch

import duckdb
import requests
import yaml

import config.settings as config_module
from spark.batch import functions


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def load_module(name, relative_path):
    spec = importlib.util.spec_from_file_location(name, PROJECT_ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ConfigurationTests(unittest.TestCase):
    def test_helpers_import_without_configuration_or_io(self):
        with (
            patch.object(config_module, "read_config") as read_config,
            patch.object(functions.duckdb, "connect") as connect,
            patch.object(functions.requests, "get") as get,
        ):
            load_module("isolated_helpers", "spark/batch/functions.py")
        read_config.assert_not_called()
        connect.assert_not_called()
        get.assert_not_called()

    def test_configurations_match_settings_schema(self):
        for filename in ("config.yml", "config.example.yml"):
            with self.subTest(filename=filename):
                values = yaml.safe_load(
                    (PROJECT_ROOT / "config" / filename).read_text(encoding="utf-8")
                )
                config_module.parse_config(values)
                self.assertEqual(values["STAGING_DIR"], "staging")
                self.assertEqual(values["DUCKDB_PATH"], "infra/duckdb/data/warehouse.duckdb")

    def test_configuration_is_read_once_for_multiple_runs(self):
        settings = config_module.read_config()
        for address in ("http://localhost:9870", "http://localhost:9870/"):
            with self.subTest(address=address), patch.object(
                config_module, "read_config", return_value=replace(
                    settings, HDFS_NAMENODE_HTTP=address
                )
            ) as read_config:
                pipeline = load_module("isolated_pipeline", "spark/batch/hdfs_to_duckdb.py")
                with (
                    patch.object(pipeline, "read_ingestion_ledger", return_value={"old"}) as ledger,
                    patch.object(pipeline, "read_from_hdfs", return_value=[]) as read,
                    patch.object(pipeline, "write_to_duckdb", return_value={}) as write,
                    redirect_stdout(io.StringIO()),
                ):
                    pipeline.run_ingestion(topics="listen_events")
                    pipeline.run_ingestion()

                read_config.assert_called_once_with()
                self.assertEqual(ledger.call_count, 2)
                first = read.call_args_list[0]
                self.assertEqual(first.kwargs["topic"], "listen_events")
                self.assertEqual(
                    [item.kwargs["topic"] for item in read.call_args_list[1:]],
                    pipeline.ALL_TOPICS,
                )
                self.assertEqual(first.kwargs["existing_files"], {"old"})
                self.assertEqual(first.kwargs["webhdfs_url"], "http://localhost:9870/webhdfs/v1")
                self.assertEqual(first.kwargs["hdfs_base_path"], settings.HDFS_BASE_PATH)
                self.assertNotIn("\\", first.kwargs["webhdfs_url"])
                self.assertEqual(write.call_args.kwargs["staging_dir"], pipeline.DEFAULT_STAGING_DIR)
                self.assertEqual(
                    pipeline.DEFAULT_DUCKDB_PATH,
                    str((PROJECT_ROOT / settings.DUCKDB_PATH).resolve()),
                )

    def test_absolute_local_paths_are_preserved(self):
        settings = config_module.read_config()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            with patch.object(config_module, "read_config", return_value=replace(
                settings, DUCKDB_PATH=str(root / "warehouse.duckdb"), STAGING_DIR=str(root)
            )):
                pipeline = load_module("absolute_paths_pipeline", "spark/batch/hdfs_to_duckdb.py")
            self.assertEqual(pipeline.DEFAULT_DUCKDB_PATH, str(root / "warehouse.duckdb"))
            self.assertEqual(pipeline.DEFAULT_STAGING_DIR, str(root))


class WebHDFSTests(unittest.TestCase):
    def test_redirect_uses_host_datanode_address(self):
        redirect = Mock(status_code=307, headers={
            "Location": "http://datanode:9864/webhdfs/v1/data/file?op=OPEN"
        })
        response = Mock(content=b"parquet")
        with patch.object(functions.requests, "get", side_effect=[redirect, response]) as get:
            result = functions._fetch_webhdfs_bytes("http://localhost:9870/file", timeout=5)
        self.assertEqual(result, b"parquet")
        self.assertEqual(get.call_args_list, [
            call("http://localhost:9870/file", allow_redirects=False, timeout=5),
            call("http://localhost:9864/webhdfs/v1/data/file?op=OPEN", timeout=10),
        ])
        response.raise_for_status.assert_called_once()

    def test_success_returns_bytes_without_redirect(self):
        with patch.object(functions.requests, "get", return_value=Mock(
            status_code=200, content=b"metadata"
        )):
            self.assertEqual(functions._fetch_webhdfs_bytes("http://host/file"), b"metadata")

    def test_redirect_without_location_raises(self):
        with patch.object(functions.requests, "get", return_value=Mock(status_code=307, headers={})):
            with self.assertRaises(ConnectionError):
                functions._fetch_webhdfs_bytes("http://host/file")

    def test_http_error_is_propagated(self):
        response = Mock(status_code=404)
        response.raise_for_status.side_effect = requests.HTTPError("missing")
        with patch.object(functions.requests, "get", return_value=response):
            with self.assertRaises(requests.HTTPError):
                functions._fetch_webhdfs_bytes("http://host/file")

    def test_metadata_duplicates_and_existing_files_are_skipped(self):
        hdfs_path = "/data/eventsim/listen_events/year=2026/month=10/day=29/part.parquet"
        record = {"action": "add", "path": "hdfs://namenode:9000" + hdfs_path}
        metadata = "v1\n" + "\n".join(json.dumps(item) for item in (
            record, record, {"action": "delete", "path": "/ignored"}
        ))
        listing = Mock(status_code=200)
        listing.json.return_value = {"FileStatuses": {"FileStatus": [{"pathSuffix": "0"}]}}
        with tempfile.TemporaryDirectory() as staging, patch.object(
            functions.requests, "get", return_value=listing
        ) as get, patch.object(functions, "_fetch_webhdfs_bytes") as fetch:
            fetch.side_effect = [metadata.encode(), b"parquet"]
            existing = set()
            arguments = dict(
                topic="listen_events", hdfs_base_path="/data/eventsim",
                webhdfs_url="http://localhost:9870/webhdfs/v1", staging_dir=staging,
                existing_files=existing,
            )
            downloaded = functions.read_from_hdfs(**arguments)
            self.assertEqual(len(downloaded), 1)
            self.assertEqual(downloaded[0]["batch_name"], "0")
            self.assertEqual(downloaded[0]["hdfs_path"], hdfs_path)
            self.assertEqual(Path(downloaded[0]["local_path"]).read_bytes(), b"parquet")
            self.assertEqual(existing, {hdfs_path})
            self.assertEqual(fetch.call_count, 2)
            self.assertEqual(get.call_args.args[0],
                "http://localhost:9870/webhdfs/v1/data/eventsim/listen_events/_spark_metadata?op=LISTSTATUS")
            fetch.reset_mock(side_effect=True)
            fetch.return_value = metadata.encode()
            self.assertEqual(functions.read_from_hdfs(**arguments), [])
            fetch.assert_called_once()

    def test_batch_names_and_listing_order_are_preserved(self):
        names = ["10", "0002", "9.compact"]
        listing = Mock(status_code=200)
        listing.json.return_value = {"FileStatuses": {"FileStatus": [
            {"pathSuffix": name} for name in names
        ]}}
        responses = []
        for index in range(len(names)):
            record = {"action": "add", "path":
                f"/data/eventsim/listen_events/batch/part-{index}.parquet"}
            responses.extend([("v1\n" + json.dumps(record)).encode(), b"parquet"])
        with tempfile.TemporaryDirectory() as staging, patch.object(
            functions.requests, "get", return_value=listing
        ), patch.object(functions, "_fetch_webhdfs_bytes", side_effect=responses) as fetch:
            downloaded = functions.read_from_hdfs(
                topic="listen_events", hdfs_base_path="/data/eventsim",
                webhdfs_url="http://localhost:9870/webhdfs/v1", staging_dir=staging,
            )
            self.assertEqual([file["batch_name"] for file in downloaded], names)
            self.assertTrue(all(Path(file["local_path"]).exists() for file in downloaded))
            self.assertEqual(
                [item.args[0].rsplit("/", 1)[1] for item in fetch.call_args_list[::2]],
                [f"{name}?op=OPEN" for name in names],
            )

    def test_compact_log_does_not_download_previously_seen_file(self):
        path = "/data/eventsim/listen_events/batch/part.parquet"
        metadata = ("v1\n" + json.dumps({"action": "add", "path": path})).encode()
        listing = Mock(status_code=200)
        listing.json.return_value = {"FileStatuses": {"FileStatus": [
            {"pathSuffix": "0"}, {"pathSuffix": "9.compact"}
        ]}}
        with tempfile.TemporaryDirectory() as staging, patch.object(
            functions.requests, "get", return_value=listing
        ), patch.object(functions, "_fetch_webhdfs_bytes", side_effect=[
            metadata, b"parquet", metadata
        ]) as fetch:
            downloaded = functions.read_from_hdfs(
                "listen_events", "/data/eventsim", "http://localhost:9870/webhdfs/v1", staging
            )
            self.assertEqual(len(downloaded), 1)
            self.assertEqual(downloaded[0]["batch_name"], "0")
            self.assertEqual(fetch.call_count, 3)

    def test_malformed_listing_returns_empty(self):
        for response in ({}, {"FileStatuses": None}):
            with self.subTest(response=response), tempfile.TemporaryDirectory() as staging:
                listing = Mock(status_code=200)
                listing.json.return_value = response
                with (
                    patch.object(functions.requests, "get", return_value=listing),
                    patch.object(functions, "_fetch_webhdfs_bytes") as fetch,
                    redirect_stdout(io.StringIO()),
                ):
                    self.assertEqual(functions.read_from_hdfs(
                        "listen_events", "/data/eventsim", "http://host/webhdfs/v1", staging
                    ), [])
                fetch.assert_not_called()

    def test_failed_download_is_not_marked_as_seen(self):
        path = "/data/eventsim/listen_events/batch/part.parquet"
        metadata = ("v1\n" + json.dumps({"action": "add", "path": path})).encode()
        listing = Mock(status_code=200)
        listing.json.return_value = {"FileStatuses": {"FileStatus": [{"pathSuffix": "0"}]}}
        with tempfile.TemporaryDirectory() as staging, patch.object(
            functions.requests, "get", return_value=listing
        ), patch.object(functions, "_fetch_webhdfs_bytes", side_effect=[
            metadata, requests.ConnectionError("download failed")
        ]), redirect_stdout(io.StringIO()):
            existing = set()
            self.assertEqual(functions.read_from_hdfs(
                "listen_events", "/data/eventsim", "http://host/webhdfs/v1", staging, existing
            ), [])
            self.assertEqual(existing, set())


class DuckDBTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.database = str(self.root / "warehouse.duckdb")
        self.staging = self.root / "custom-staging"
        self.staging.mkdir()
        self.output = redirect_stdout(io.StringIO())
        self.output.__enter__()
        self.addCleanup(self.output.__exit__, None, None, None)

    def initialize_database(self):
        with duckdb.connect(self.database) as con:
            con.execute((PROJECT_ROOT / "infra/duckdb/init.sql").read_text(encoding="utf-8"))

    def make_file(self, directory=None):
        path = (directory if directory is not None else self.staging / "listen_events" / "batch") / "part.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        with duckdb.connect() as con:
            con.execute("COPY (SELECT 42 AS value) TO ? (FORMAT PARQUET)", [str(path)])
        return {"topic": "listen_events", "hdfs_path": "/events/listen_events/part.parquet",
                "local_path": str(path), "batch_name": "0"}

    def write(self, file, cleanup=True):
        return functions.write_to_duckdb(
            duckdb_path=self.database, files_to_load=[file],
            staging_dir=str(self.staging), cleanup_staging=cleanup,
        )

    def test_missing_database_does_not_create_one(self):
        self.assertEqual(functions.read_ingestion_ledger(self.database), set())
        self.assertFalse(Path(self.database).exists())

    def test_missing_ledger_returns_empty_and_releases_connection(self):
        with duckdb.connect(self.database):
            pass
        self.assertEqual(functions.read_ingestion_ledger(self.database), set())
        with duckdb.connect(self.database) as con:
            con.execute("CREATE TABLE connection_released (value INTEGER)")

    def test_ledger_only_returns_successful_paths(self):
        self.initialize_database()
        with duckdb.connect(self.database) as con:
            con.execute("INSERT INTO landing.ingestion_ledger (file_path, status) VALUES ('/ok', 'SUCCESS'), ('/bad', 'FAILED')")
        self.assertEqual(functions.read_ingestion_ledger(self.database), {"/ok"})

    def test_ledger_query_failure_closes_connection(self):
        Path(self.database).touch()
        connection = Mock()
        connection.execute.side_effect = RuntimeError("query failed")
        with patch.object(functions.duckdb, "connect", return_value=connection):
            self.assertEqual(functions.read_ingestion_ledger(self.database), set())
        connection.close.assert_called_once()

    def test_success_commits_rows_and_ledger_and_cleans_custom_staging(self):
        self.initialize_database()
        file = self.make_file()
        self.assertEqual(self.write(file), {"listen_events": 1})
        with duckdb.connect(self.database) as con:
            self.assertEqual(con.execute("SELECT value FROM landing.listen_events").fetchall(), [(42,)])
        self.assertEqual(functions.read_ingestion_ledger(self.database), {file["hdfs_path"]})
        self.assertEqual(list(self.staging.iterdir()), [])
        self.assertTrue(self.staging.is_dir())

    def test_cleanup_preserves_unrelated_files(self):
        self.initialize_database()
        file = self.make_file()
        unrelated = Path(file["local_path"]).parent / "keep.txt"
        unrelated.write_text("keep", encoding="utf-8")
        self.write(file)
        self.assertFalse(Path(file["local_path"]).exists())
        self.assertEqual(unrelated.read_text(encoding="utf-8"), "keep")

    def test_cleanup_can_be_disabled(self):
        self.initialize_database()
        file = self.make_file()
        self.write(file, cleanup=False)
        self.assertTrue(Path(file["local_path"]).exists())

    def test_compact_batch_name_is_compatible_with_numeric_ledger(self):
        self.initialize_database()
        file = self.make_file()
        file["batch_name"] = "9.compact"
        self.write(file)
        self.assertEqual(file["batch_name"], "9.compact")
        with duckdb.connect(self.database) as con:
            self.assertEqual(con.execute(
                "SELECT batch_id FROM landing.ingestion_ledger WHERE file_path = ?",
                [file["hdfs_path"]],
            ).fetchone(), (9,))

    def test_two_pipeline_runs_ingest_once_using_real_ledger(self):
        self.initialize_database()
        fixture = self.make_file(self.root / "fixture")
        parquet = Path(fixture["local_path"]).read_bytes()
        hdfs_path = "/data/eventsim/listen_events/batch/part.parquet"
        metadata = ("v1\n" + json.dumps({"action": "add", "path": hdfs_path})).encode()
        listing = Mock(status_code=200)
        listing.json.return_value = {"FileStatuses": {"FileStatus": [{"pathSuffix": "0"}]}}
        pipeline = load_module("integration_pipeline", "spark/batch/hdfs_to_duckdb.py")
        with (
            patch.object(functions.requests, "get", return_value=listing),
            patch.object(functions, "_fetch_webhdfs_bytes", side_effect=[metadata, parquet, metadata]) as fetch,
        ):
            arguments = dict(
                duckdb_path=self.database, topics="listen_events",
                hdfs_base_path="/data/eventsim", webhdfs_url="http://localhost:9870/webhdfs/v1",
                staging_dir=str(self.staging),
            )
            self.assertEqual(pipeline.run_ingestion(**arguments), {"listen_events": 1})
            self.assertEqual(pipeline.run_ingestion(**arguments), {})
        self.assertEqual(fetch.call_count, 3)
        with duckdb.connect(self.database) as con:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM landing.listen_events").fetchone()[0], 1)
        self.assertEqual(functions.read_ingestion_ledger(self.database), {hdfs_path})
        self.assertEqual(list(self.staging.iterdir()), [])

    def test_cleanup_does_not_delete_file_outside_staging(self):
        self.initialize_database()
        file = self.make_file(self.root / "outside")
        self.write(file)
        self.assertTrue(Path(file["local_path"]).exists())

    def test_ledger_insert_failure_rolls_back_data_and_keeps_staging(self):
        with duckdb.connect(self.database) as con:
            con.execute("CREATE SCHEMA landing; CREATE TABLE landing.listen_events (value INTEGER)")
        file = self.make_file()
        with self.assertRaises(duckdb.Error):
            self.write(file)
        with duckdb.connect(self.database) as con:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM landing.listen_events").fetchone()[0], 0)
        self.assertTrue(Path(file["local_path"]).exists())

    def test_invalid_parquet_keeps_staging_and_ledger_unchanged(self):
        self.initialize_database()
        file = self.make_file()
        Path(file["local_path"]).write_bytes(b"invalid parquet")
        with self.assertRaises(duckdb.Error):
            self.write(file)
        self.assertEqual(functions.read_ingestion_ledger(self.database), set())
        self.assertTrue(Path(file["local_path"]).exists())

    def test_empty_batch_does_not_open_database(self):
        with patch.object(functions.duckdb, "connect") as connect:
            self.assertEqual(functions.write_to_duckdb(self.database, [], str(self.staging)), {})
        connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()
