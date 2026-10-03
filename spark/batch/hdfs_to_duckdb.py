"""
spark/batch/hdfs_to_duckdb.py
Batch Ingestion: Đọc dữ liệu Parquet từ HDFS và nạp vào DuckDB landing layer.

Tự động nội suy schema từ Parquet và các cột phân vùng Hive (year, month, day).
Lưu vết metadata vào landing.ingestion_ledger để đảm bảo tính Idempotency.
"""

import io
import json
import os
import shutil
import sys
import urllib.parse
from collections import defaultdict
from pathlib import Path

# Đảm bảo UTF-8 output trên môi trường Windows terminal
sys.stdout.reconfigure(encoding="utf-8")

import duckdb
import requests
from dotenv import load_dotenv

# Nạp biến môi trường từ .env nếu có
load_dotenv()

DEFAULT_WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://localhost:9870/webhdfs/v1")
DEFAULT_HDFS_BASE_PATH = os.getenv("HDFS_BASE_PATH", "/data/events")
DEFAULT_DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH",
    str(Path(__file__).resolve().parent.parent.parent / "infra" / "duckdb" / "data" / "warehouse.duckdb")
)
DEFAULT_STAGING_DIR = str(Path(__file__).resolve().parent.parent.parent / "data" / "staging")

ALL_TOPICS = [
    "auth_events",
    "listen_events",
    "page_view_events",
    "status_change_events",
]


# ─────────────────────────────────────────────────────────────────────────────
# 1. Helper tải file qua WebHDFS REST API
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_webhdfs_bytes(url: str, timeout: int = 15) -> bytes:
    """
    Tải nội dung byte của file từ WebHDFS REST API.
    Xử lý tự động HTTP 307 Redirect từ NameNode sang DataNode.
    Trên máy host Windows, thay thế hostname nội bộ 'datanode:9864' thành 'localhost:9864'.
    """
    resp = requests.get(url, allow_redirects=False, timeout=timeout)
    if resp.status_code == 307:
        redirect_url = resp.headers.get("Location")
        if redirect_url:
            # Map hostname Docker nội bộ về localhost của máy host
            redirect_url = redirect_url.replace("datanode:9864", "localhost:9864")
            data_resp = requests.get(redirect_url, timeout=timeout * 2)
            data_resp.raise_for_status()
            return data_resp.content
        raise ConnectionError(f"WebHDFS trả về 307 nhưng không có Location header: {url}")
    elif resp.status_code == 200:
        return resp.content
    else:
        resp.raise_for_status()
        return b""


# ─────────────────────────────────────────────────────────────────────────────
# 2. Hàm đọc dữ liệu từ HDFS: read_from_hdfs()
# ─────────────────────────────────────────────────────────────────────────────

def read_from_hdfs(
    topics: list[str] | str | None = None,
    hdfs_base_path: str = DEFAULT_HDFS_BASE_PATH,
    webhdfs_url: str = DEFAULT_WEBHDFS_URL,
    staging_dir: str = DEFAULT_STAGING_DIR,
    existing_files: set[str] | None = None,
) -> list[dict]:
    """
    Đọc commit logs trong _spark_metadata từ HDFS qua WebHDFS REST API.
    Chỉ lấy các bản ghi có action là 'add'.
    Đối chiếu với existing_files (từ landing.ingestion_ledger) để lọc ra file mới.
    Tải các file mới về thư mục staging cục bộ (giữ nguyên cấu trúc phân vùng Hive).

    Parameters
    ----------
    topics         : Danh sách các topic cần đọc (mặc định đọc toàn bộ 4 topic)
    hdfs_base_path : Đường dẫn gốc chứa events trên HDFS (/data/events)
    webhdfs_url    : Endpoint WebHDFS REST API (http://localhost:9870/webhdfs/v1)
    staging_dir    : Thư mục staging tạm trên máy host để chứa Parquet trước khi nạp
    existing_files : Tập hợp các file_path đã nạp thành công vào DuckDB ledger

    Returns
    -------
    list[dict]: Danh sách metadata các file mới đã tải về staging:
        [
            {
                "topic": "listen_events",
                "hdfs_path": "/data/events/listen_events/year=2026/month=10/day=03/part-0000.parquet",
                "local_path": "data/staging/listen_events/year=2026/month=10/day=03/part-0000.parquet",
                "batch_id": 0
            },
            ...
        ]
    """
    if topics is None:
        target_topics = ALL_TOPICS
    elif isinstance(topics, str):
        target_topics = [topics]
    else:
        target_topics = list(topics)

    if existing_files is None:
        existing_files = set()

    webhdfs_url = webhdfs_url.rstrip("/")
    staging_base = Path(staging_dir).resolve()
    new_files_to_load = []

    for topic in target_topics:
        meta_dir_url = f"{webhdfs_url}{hdfs_base_path}/{topic}/_spark_metadata?op=LISTSTATUS"
        try:
            resp = requests.get(meta_dir_url, timeout=10)
        except Exception as exc:
            print(f"[WARN] Không thể kết nối tới WebHDFS cho topic '{topic}': {exc}")
            continue

        if resp.status_code != 200:
            print(f"[INFO] Topic '{topic}' chưa có _spark_metadata (status: {resp.status_code}). Bỏ qua.")
            continue

        # Lấy danh sách các commit files
        file_statuses = resp.json().get("FileStatuses", {}).get("FileStatus", [])
        commit_files = []
        for f in file_statuses:
            name = f.get("pathSuffix", "")
            # Commit file là số nguyên (ví dụ: '0', '1', '2') hoặc file compact (ví dụ: '9.compact')
            is_digit = name.isdigit()
            is_compact = name.endswith(".compact") and name.split(".")[0].isdigit()
            if is_digit or is_compact:
                batch_id = int(name.split(".")[0])
                commit_files.append((batch_id, is_compact, name))

        if not commit_files:
            continue

        # Sắp xếp commit file theo thứ tự batch tăng dần
        commit_files.sort(key=lambda x: (x[0], 1 if x[1] else 0))

        # Đọc từng commit file để trích xuất các file có action == 'add'
        for batch_id, _, commit_filename in commit_files:
            file_url = f"{webhdfs_url}{hdfs_base_path}/{topic}/_spark_metadata/{commit_filename}?op=OPEN"
            try:
                content_bytes = _fetch_webhdfs_bytes(file_url)
                lines = content_bytes.decode("utf-8", errors="replace").splitlines()
            except Exception as e:
                print(f"[WARN] Không đọc được commit file {commit_filename} của topic {topic}: {e}")
                continue

            if not lines:
                continue

            # Bỏ qua header phiên bản (ví dụ: 'v1'), duyệt qua từng record JSON
            for line in lines[1:]:
                line = line.strip()
                if not line:
                    continue

                try:
                    entry = json.loads(line)
                except Exception:
                    continue

                # Chỉ lấy các bản ghi trên _spark_metadata có action là 'add'
                if entry.get("action") != "add":
                    continue

                raw_path = entry.get("path")
                if not raw_path:
                    continue

                # Chuẩn hóa đường dẫn HDFS
                if raw_path.startswith("hdfs://"):
                    parsed_url = urllib.parse.urlparse(raw_path)
                    hdfs_file_path = parsed_url.path
                else:
                    hdfs_file_path = raw_path

                # Đảm bảo đường dẫn bắt đầu bằng '/'
                if not hdfs_file_path.startswith("/"):
                    hdfs_file_path = "/" + hdfs_file_path

                # Kiểm tra nếu file đã từng nạp thành công vào DuckDB ledger thì bỏ qua
                if hdfs_file_path in existing_files:
                    continue

                # Trích xuất đường dẫn tương đối sau topic để bảo toàn cấu trúc Hive Partition
                # Ví dụ: /data/events/listen_events/year=2026/month=10/day=03/part-00000.parquet
                # -> year=2026/month=10/day=03/part-00000.parquet
                topic_prefix = f"/{topic}/"
                if topic_prefix in hdfs_file_path:
                    rel_subpath = hdfs_file_path.split(topic_prefix, 1)[1]
                else:
                    rel_subpath = Path(hdfs_file_path).name

                local_dest = staging_base / topic / rel_subpath
                local_dest.parent.mkdir(parents=True, exist_ok=True)

                # Tải file Parquet về thư mục staging cục bộ
                parquet_download_url = f"{webhdfs_url}{hdfs_file_path}?op=OPEN"
                try:
                    parquet_bytes = _fetch_webhdfs_bytes(parquet_download_url)
                    with open(local_dest, "wb") as pf:
                        pf.write(parquet_bytes)

                    # Ghi nhận file mới đã tải
                    new_files_to_load.append({
                        "topic": topic,
                        "hdfs_path": hdfs_file_path,
                        "local_path": str(local_dest.resolve().as_posix()),
                        "batch_id": batch_id,
                    })
                    # Đánh dấu đã thấy để tránh tải trùng trong cùng một lần quét
                    existing_files.add(hdfs_file_path)

                except Exception as dl_err:
                    print(f"[ERROR] Lỗi khi tải file {hdfs_file_path} từ HDFS: {dl_err}")

    return new_files_to_load


# ─────────────────────────────────────────────────────────────────────────────
# 3. Hàm ghi dữ liệu vào DuckDB: write_to_duckdb()
# ─────────────────────────────────────────────────────────────────────────────

def write_to_duckdb(
    duckdb_path: str = DEFAULT_DUCKDB_PATH,
    files_to_load: list[dict] | None = None,
    cleanup_staging: bool = True,
) -> dict[str, int]:
    """
    Nạp dữ liệu Parquet từ danh sách files_to_load vào DuckDB schema landing.
    - Tự động nội suy schema từ Parquet và trích xuất các cột phân vùng Hive (year, month, day).
    - Tự động tạo bảng landing.<topic> nếu chưa tồn tại.
    - Nạp nguyên bản bản ghi, không biến đổi.
    - Cập nhật landing.ingestion_ledger trong cùng một transaction (ACID).
    - Tự động dọn dẹp các file staging tạm thời sau khi nạp thành công.

    Parameters
    ----------
    duckdb_path     : Đường dẫn tới file database DuckDB
    files_to_load   : Danh sách metadata các file Parquet đã tải từ read_from_hdfs()
    cleanup_staging : Xóa các file Parquet tạm trong staging sau khi hoàn tất

    Returns
    -------
    dict[str, int]: Thống kê số lượng bản ghi đã nạp thành công theo từng topic.
    """
    if not files_to_load:
        print("[INFO] Không có file mới cần nạp vào DuckDB.")
        return {}

    # Gom nhóm danh sách file theo topic
    files_by_topic = defaultdict(list)
    for f in files_to_load:
        files_by_topic[f["topic"]].append(f)

    results = {}
    con = duckdb.connect(duckdb_path)

    try:
        for topic, topic_files in files_by_topic.items():
            table_name = f"landing.{topic}"
            local_paths = [f["local_path"] for f in topic_files]

            print(f"[*] Đang nạp {len(local_paths)} file vào bảng '{table_name}'...")

            # Mở transaction để đảm bảo nạp dữ liệu và ghi ledger nguyên tử (Atomic)
            con.begin()
            try:
                # 1. Tự động tạo bảng nếu chưa có (Dùng nội suy schema từ Parquet với LIMIT 0)
                con.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} AS 
                    SELECT * 
                    FROM read_parquet(?::VARCHAR[], hive_partitioning=true, union_by_name=true) 
                    LIMIT 0;
                """, [local_paths])

                # 2. Đếm số dòng trước khi nạp
                count_before = con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]

                # 3. Nạp dữ liệu nguyên bản từ Parquet vào bảng landing
                con.execute(f"""
                    INSERT INTO {table_name} BY NAME 
                    SELECT * 
                    FROM read_parquet(?::VARCHAR[], hive_partitioning=true, union_by_name=true);
                """, [local_paths])

                # 4. Đếm số dòng sau khi nạp để tính chênh lệch
                count_after = con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
                inserted_rows = count_after - count_before
                results[topic] = inserted_rows

                # 5. Ghi log các file đã nạp vào landing.ingestion_ledger
                ledger_records = [
                    (f["hdfs_path"], f["topic"], f["batch_id"], "SUCCESS")
                    for f in topic_files
                ]
                con.executemany("""
                    INSERT INTO landing.ingestion_ledger (file_path, topic, batch_id, status, ingested_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT (file_path) DO NOTHING;
                """, ledger_records)

                con.commit()
                print(f" [PASS] Đã nạp thành công {inserted_rows} bản ghi vào '{table_name}' và cập nhật ledger.")

            except Exception as tx_err:
                con.rollback()
                print(f" [FAIL] Lỗi giao dịch khi nạp vào '{table_name}': {tx_err}")
                raise tx_err

    finally:
        con.close()

    # Dọn dẹp an toàn các file staging cục bộ sau khi đã nạp và commit vào DuckDB
    if cleanup_staging:
        for f in files_to_load:
            local_path = Path(f["local_path"])
            try:
                if local_path.exists():
                    local_path.unlink()
            except Exception as del_err:
                print(f"[WARN] Không thể xóa file staging {local_path}: {del_err}")

        # Dọn các thư mục con rỗng trong staging_dir
        staging_root = Path(DEFAULT_STAGING_DIR)
        if staging_root.exists():
            for root, dirs, files in os.walk(staging_root, topdown=False):
                for d in dirs:
                    dir_path = Path(root) / d
                    try:
                        if not any(dir_path.iterdir()):
                            dir_path.rmdir()
                    except Exception:
                        pass

    return results


# ─────────────────────────────────────────────────────────────────────────────
# 4. Hàm điều phối trung tâm: run_ingestion()
# ─────────────────────────────────────────────────────────────────────────────

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

    # 1. Truy vấn các file đã nạp thành công từ ingestion_ledger trong DuckDB
    existing_files = set()
    if os.path.exists(duckdb_path):
        try:
            con = duckdb.connect(duckdb_path, read_only=True)
            # Kiểm tra xem bảng ingestion_ledger đã tồn tại chưa
            table_check = con.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'landing' AND table_name = 'ingestion_ledger';
            """).fetchone()[0]

            if table_check > 0:
                rows = con.execute("SELECT file_path FROM landing.ingestion_ledger WHERE status = 'SUCCESS';").fetchall()
                existing_files = {r[0] for r in rows}
                print(f"[*] Đã tải {len(existing_files)} file đã nạp từ 'landing.ingestion_ledger'.")
            con.close()
        except Exception as e:
            print(f"[WARN] Không thể đọc landing.ingestion_ledger: {e}. Sẽ tiến hành quét mới.")

    # 2. Đọc và tải các file mới từ HDFS qua WebHDFS REST API
    new_files = read_from_hdfs(
        topics=topics,
        hdfs_base_path=hdfs_base_path,
        webhdfs_url=webhdfs_url,
        staging_dir=staging_dir,
        existing_files=existing_files,
    )
    print(f"[*] Tìm thấy {len(new_files)} file mới cần nạp từ HDFS.")

    # 3. Nạp dữ liệu vào DuckDB và ghi log ledger
    summary = write_to_duckdb(
        duckdb_path=duckdb_path,
        files_to_load=new_files,
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
    run_ingestion()