"""Điều phối batch ingestion từ HDFS vào DuckDB landing layer."""

import sys
from pathlib import Path

from config.settings import Settings, read_config
from spark.batch.functions import (
    read_from_hdfs,
    read_ingestion_ledger,
    write_to_duckdb,
)

settings: Settings = read_config()

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DEFAULT_WEBHDFS_URL = settings.HDFS_NAMENODE_HTTP.rstrip("/") + "/webhdfs/v1"
DEFAULT_HDFS_BASE_PATH = settings.HDFS_BASE_PATH
DEFAULT_DUCKDB_PATH = str((PROJECT_ROOT / settings.DUCKDB_PATH).resolve())
DEFAULT_STAGING_DIR = str((PROJECT_ROOT / settings.STAGING_DIR).resolve())

ALL_TOPICS = [
    "auth_events",
    "listen_events",
    "page_view_events",
    "status_change_events",
]


def run_ingestion(
    duckdb_path: str = DEFAULT_DUCKDB_PATH,
    topics: list[str] | str | None = None,
    hdfs_base_path: str = DEFAULT_HDFS_BASE_PATH,
    webhdfs_url: str = DEFAULT_WEBHDFS_URL,
    staging_dir: str = DEFAULT_STAGING_DIR,
) -> dict[str, int]:
    """
    Entrypoint điều phối toàn bộ chu trình Ingestion từ HDFS sang DuckDB.
    Được thiết kế để chạy trực tiếp từ Terminal hoặc import vào Airflow PythonOperator.

    Parameters
    ----------
    duckdb_path    : Đường dẫn tới file database DuckDB
    topics         : Danh sách topic cần nạp (None = toàn bộ 4 topic)
    hdfs_base_path : Thư mục gốc trên HDFS (/data/events)
    webhdfs_url    : Endpoint WebHDFS REST API
    staging_dir    : Thư mục staging tạm trên host

    Returns
    -------
    dict[str, int]: Thống kê số lượng bản ghi đã nạp theo từng topic.
    """
    print("\n" + "=" * 70)
    print("  BẮT ĐẦU CHU TRÌNH INGESTION: HDFS -> DUCKDB LANDING")
    print(f"  Target DuckDB : {duckdb_path}")
    print(f"  WebHDFS URL   : {webhdfs_url}")
    print("=" * 70)

    if topics is None:
        target_topics = list(ALL_TOPICS)
    elif isinstance(topics, str):
        target_topics = [topics]
    else:
        target_topics = list(topics)

    # 1. Đọc các file đã nạp thành công từ DuckDB ledger.
    existing_files = read_ingestion_ledger(duckdb_path)

    # 2. Đọc và tải các file mới từ HDFS qua WebHDFS REST API
    new_files = []
    for topic in target_topics:
        new_files.extend(read_from_hdfs(
            topic=topic,
            hdfs_base_path=hdfs_base_path,
            webhdfs_url=webhdfs_url,
            staging_dir=staging_dir,
            existing_files=existing_files,
        ))
    print(f"[*] Tìm thấy {len(new_files)} file mới cần nạp từ HDFS.")

    # 3. Nạp dữ liệu vào DuckDB và ghi log ledger
    summary = write_to_duckdb(
        duckdb_path=duckdb_path,
        files_to_load=new_files,
        staging_dir=staging_dir,
        cleanup_staging=True,
    )

    print("=" * 70)
    print(f"  HOÀN THÀNH INGESTION. Tổng kết nạp:")
    for t, count in summary.items():
        print(f"   - {t}: {count} bản ghi")
    if not summary:
        print("   - Không có bản ghi mới nào được nạp.")
    print("=" * 70 + "\n")

    return summary


if __name__ == "__main__":
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    run_ingestion()
