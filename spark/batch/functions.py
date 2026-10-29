"""Helper WebHDFS và DuckDB; cấu hình được truyền từ file điều phối."""

import json
import urllib.parse
from collections import defaultdict
from pathlib import Path

import duckdb
import requests


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


def read_ingestion_ledger(duckdb_path: str) -> set[str]:
    """Đọc các file SUCCESS; trả tập rỗng nếu ledger chưa có hoặc không đọc được."""
    if not Path(duckdb_path).exists():
        return set()

    con = None
    try:
        con = duckdb.connect(duckdb_path, read_only=True)
        table_check = con.execute("""
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'landing' AND table_name = 'ingestion_ledger';
        """).fetchone()[0]
        if not table_check:
            return set()
        rows = con.execute(
            "SELECT file_path FROM landing.ingestion_ledger WHERE status = 'SUCCESS';"
        ).fetchall()
        existing_files = {row[0] for row in rows}
        print(f"[*] Đã tải {len(existing_files)} file đã nạp từ 'landing.ingestion_ledger'.")
        return existing_files
    except Exception as exc:
        print(f"[WARN] Không thể đọc landing.ingestion_ledger: {exc}. Sẽ tiến hành quét mới.")
        return set()
    finally:
        if con is not None:
            con.close()


def read_from_hdfs(
    topic: str,
    hdfs_base_path: str,
    webhdfs_url: str,
    staging_dir: str,
    existing_files: set[str] | None = None,
) -> list[dict]:
    """
    Đọc commit logs của một topic trong _spark_metadata qua WebHDFS REST API.
    Chỉ lấy các bản ghi có action là 'add'.
    Đối chiếu với existing_files (từ landing.ingestion_ledger) để lọc ra file mới.
    Tải các file mới về thư mục staging cục bộ (giữ nguyên cấu trúc phân vùng Hive).

    Parameters
    ----------
    topic          : Tên topic cần đọc
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
                "local_path": "staging/listen_events/year=2026/month=10/day=03/part-0000.parquet",
                "batch_name": "0"
            },
            ...
        ]
    """
    if existing_files is None:
        existing_files = set()

    webhdfs_url = webhdfs_url.rstrip("/")
    staging_base = Path(staging_dir).resolve()
    success_files = []

    meta_dir_url = f"{webhdfs_url}{hdfs_base_path}/{topic}/_spark_metadata?op=LISTSTATUS"
    try:
        resp = requests.get(meta_dir_url, timeout=10)
    except Exception as exc:
        print(f"[WARN] Không thể kết nối tới WebHDFS cho topic '{topic}': {exc}")
        return success_files

    if resp.status_code != 200:
        print(f"[INFO] Topic '{topic}' chưa có _spark_metadata (status: {resp.status_code}). Bỏ qua.")
        return success_files

    # Kiểm tra cấu trúc response, giữ thứ tự và tên log do WebHDFS trả về.
    try:
        resp_json = resp.json()
        file_statuses: list = resp_json["FileStatuses"]["FileStatus"]
    except (ValueError, KeyError, TypeError):
        print("[WARN] Response trả về không đúng chuẩn")
        return success_files

    commit_files = []
    for file_status in file_statuses:
        batch_name = file_status["pathSuffix"]
        commit_files.append(batch_name)

    # Đọc từng commit file để trích xuất các file có action == 'add'.
    for batch_name in commit_files:
        file_url = f"{webhdfs_url}{hdfs_base_path}/{topic}/_spark_metadata/{batch_name}?op=OPEN"
        try:
            content_bytes = _fetch_webhdfs_bytes(file_url)
            lines = content_bytes.decode("utf-8", errors="replace").splitlines()
        except Exception as exc:
            print(f"[WARN] Không đọc được commit file {batch_name} của topic {topic}: {exc}")
            continue

        if not lines:
            continue

        # Bỏ qua header phiên bản (ví dụ: 'v1'), duyệt qua từng record JSON.
        for line in lines[1:]:
            line = line.strip()
            if not line:
                continue

            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Chỉ lấy các bản ghi trên _spark_metadata có action là 'add'.
            if entry.get("action") == "add":
                raw_path = entry.get("path")
                if not raw_path:
                    continue

                # Lấy path của URI và bảo đảm chỉ có một dấu '/' ở đầu.
                hdfs_file_path = "/" + urllib.parse.urlparse(raw_path).path.lstrip("/")

                if hdfs_file_path in existing_files:
                    continue

                # Giữ cấu trúc Hive Partition trong staging.
                topic_prefix = f"/{topic}/"
                rel_subpath = hdfs_file_path.split(topic_prefix, 1)[1]
                local_dest = staging_base / topic / rel_subpath
                local_dest.parent.mkdir(parents=True, exist_ok=True)

                parquet_download_url = f"{webhdfs_url}{hdfs_file_path}?op=OPEN"
                try:
                    parquet_bytes = _fetch_webhdfs_bytes(parquet_download_url)
                    with open(local_dest, "wb") as parquet_file:
                        parquet_file.write(parquet_bytes)

                    success_files.append({
                        "topic": topic,
                        "hdfs_path": hdfs_file_path,
                        "local_path": local_dest.resolve().as_posix(),
                        "batch_name": batch_name,
                    })
                    # Không tải trùng file khi gặp lại ở log khác hoặc .compact.
                    existing_files.add(hdfs_file_path)
                except Exception as exc:
                    print(f"[ERROR] Lỗi khi tải file {hdfs_file_path} từ HDFS: {exc}")

    return success_files


def write_to_duckdb(
    duckdb_path: str,
    files_to_load: list[dict] | None,
    staging_dir: str,
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
    staging_dir     : Thư mục staging cục bộ được phép dọn dẹp
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
                    FROM read_parquet(?::VARCHAR[], hive_partitioning=false, union_by_name=true)
                    LIMIT 0;
                """, [local_paths])

                # 2. Đếm số dòng trước khi nạp
                count_before = con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]

                # 3. Nạp dữ liệu nguyên bản từ Parquet vào bảng landing
                con.execute(f"""
                    INSERT INTO {table_name} BY NAME
                    SELECT *
                    FROM read_parquet(?::VARCHAR[], hive_partitioning=false, union_by_name=true);
                """, [local_paths])

                # 4. Đếm số dòng sau khi nạp để tính chênh lệch
                count_after = con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
                inserted_rows = count_after - count_before
                results[topic] = inserted_rows

                # 5. Ghi log các file đã nạp vào landing.ingestion_ledger
                # Ledger hiện lưu batch_id BIGINT; tên log đầy đủ nằm trong metadata đầu vào.
                ledger_records = [
                    (f["hdfs_path"], f["topic"], int(f["batch_name"].removesuffix(".compact")), "SUCCESS")
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

    # Chỉ dọn file đã commit, trong đúng thư mục staging được truyền vào.
    if cleanup_staging:
        staging_root = Path(staging_dir).resolve()
        for f in files_to_load:
            local_path = Path(f["local_path"])
            if not local_path.resolve().is_relative_to(staging_root):
                print(f"[WARN] Bỏ qua file ngoài staging: {local_path}")
                continue
            try:
                if local_path.exists():
                    local_path.unlink()
            except Exception as del_err:
                print(f"[WARN] Không thể xóa file staging {local_path}: {del_err}")

        if staging_root.exists():
            subdirs = sorted(
                (path for path in staging_root.rglob("*")
                 if path.is_dir() and not path.is_symlink()),
                key=lambda path: len(path.parts),
                reverse=True,
            )
            for directory in subdirs:
                try:
                    directory.rmdir()
                except OSError:
                    pass

    return results
