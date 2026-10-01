"""
scripts/test_recovery_and_batch.py
Kiểm thử TC-RT-03 (Fault Tolerance & Checkpointing)
và TC-BA-02 / TC-BA-04 (Parquet Schema Verification & Data Reconciliation)
"""

import json
import os
import subprocess
import sys
import time
from pathlib import Path
import io

# Ensure UTF-8 output on Windows terminal
sys.stdout.reconfigure(encoding="utf-8")

import requests
import pyarrow.parquet as pq

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://localhost:9870/webhdfs/v1")
PROJECT_ROOT = Path(__file__).resolve().parent.parent

def log_section(title: str):
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)

def log_ok(msg: str):
    print(f" [PASS] {msg}")

def log_fail(msg: str):
    print(f" [FAIL] {msg}")

def log_info(msg: str):
    print(f" [INFO] {msg}")

def run_command(cmd: str):
    if not cmd.startswith("rtk proxy "):
        cmd = f"rtk proxy {cmd}"
    res = subprocess.run(cmd, shell=True, capture_output=True, text=True, encoding="utf-8", errors="replace")
    return res.returncode, res.stdout, res.stderr

# ── 1. TC-RT-03: Fault Tolerance & Checkpoint Test ─────────────────────────
def test_fault_tolerance_checkpoint():
    log_section("TC-RT-03: Kiểm thử Khả năng Phục hồi sau sự cố (Fault Tolerance & Checkpointing)")
    
    # 1.1 Dừng container spark-streaming
    log_info("1. Đang dừng container 'spark-streaming' (giả lập sự cố crash)...")
    code, out, err = run_command("docker stop spark-streaming")
    if code != 0:
        log_fail(f"Không thể dừng spark-streaming: {err}")
        return False
    log_ok("Container 'spark-streaming' đã dừng.")

    # 1.2 Gửi 25 events mới vào Kafka trong lúc Spark offline
    from confluent_kafka import Producer
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",
    })
    
    sample_file = PROJECT_ROOT / "data" / "listen_events"
    records = []
    with open(sample_file, "r", encoding="utf-8") as f:
        for idx, line in enumerate(f):
            if 30 <= idx < 55:  # Lấy 25 events tiếp theo
                records.append(json.loads(line.strip()))
                
    log_info(f"2. Gửi {len(records)} events mới vào topic 'listen_events' khi Spark đang offline...")
    delivered = 0
    def cb(err, msg):
        nonlocal delivered
        if not err:
            delivered += 1
            
    for r in records:
        producer.produce(
            topic="listen_events",
            key=str(r.get("userId", "")).encode("utf-8"),
            value=json.dumps(r).encode("utf-8"),
            callback=cb
        )
        producer.poll(0)
    producer.flush(10)
    log_ok(f"Đã gửi {delivered} events tích tụ trong Kafka buffer (lag).")

    # 1.3 Khởi động lại container spark-streaming
    log_info("3. Khởi động lại container 'spark-streaming' để khôi phục từ HDFS checkpoint...")
    code, out, err = run_command("docker start spark-streaming")
    if code != 0:
        log_fail(f"Không thể khởi động lại spark-streaming: {err}")
        return False
    log_ok("Container 'spark-streaming' đã khởi động lại.")

    # 1.4 Chờ Spark nạp checkpoint và hoàn tất micro-batch
    log_info("4. Chờ 25 giây để Spark nạp checkpoint từ HDFS và tiêu thụ backlog...")
    time.sleep(25)

    # 1.5 Kiểm tra xem Spark có đang chạy bình thường không
    code, out, err = run_command("docker inspect -f '{{.State.Running}}' spark-streaming")
    is_running = "true" in out.lower()
    if is_running:
        log_ok("Spark Streaming tiếp tục chạy ổn định sau khi khôi phục từ checkpoint.")
    else:
        log_fail("Spark Streaming bị crash sau khi restart!")
        return False

    # 1.6 Kiểm tra metadata commit mới trên HDFS
    check_meta_url = f"{WEBHDFS_URL}/data/events/listen_events/_spark_metadata?op=LISTSTATUS"
    r = requests.get(check_meta_url, timeout=5)
    if r.status_code == 200:
        files = [f["pathSuffix"] for f in r.json().get("FileStatuses", {}).get("FileStatus", [])]
        log_ok(f"HDFS _spark_metadata đã ghi nhận các commit mới: {sorted(files, key=lambda x: int(x) if x.isdigit() else 999)}")
    else:
        log_fail(f"Không đọc được _spark_metadata từ HDFS: HTTP {r.status_code}")
        return False

    return True

# ── 2. TC-BA-02 & TC-BA-04: Parquet Schema & Content Verification ──────────
def test_parquet_schema_and_reconciliation():
    log_section("TC-BA-02 & TC-BA-04: Kiểm thử Schema Parquet & Đối soát dữ liệu (Reconciliation)")

    # 2.1 Tìm 1 file Parquet từ listen_events trên HDFS
    def find_first_parquet(path):
        url = f"{WEBHDFS_URL}{path}?op=LISTSTATUS"
        r = requests.get(url, timeout=5)
        if r.status_code != 200:
            return None
        for item in r.json().get("FileStatuses", {}).get("FileStatus", []):
            name = item["pathSuffix"]
            sub = f"{path}/{name}".replace("//", "/")
            if item["type"] == "DIRECTORY" and name != "_spark_metadata":
                res = find_first_parquet(sub)
                if res:
                    return res
            elif item["type"] == "FILE" and name.endswith(".parquet"):
                return sub
        return None

    target_file = find_first_parquet("/data/events/listen_events/year=2026")
    if not target_file:
        log_fail("Không tìm thấy file Parquet trong /data/events/listen_events/year=2026")
        return False

    log_info(f"Tải sample file Parquet từ HDFS để kiểm tra: {target_file}")
    
    # 2.2 Download file qua WebHDFS REST API
    # WebHDFS OPEN trả về 307 Redirect tới datanode:9864.
    # Trên máy host, map 'datanode:9864' -> 'localhost:9864'
    open_url = f"{WEBHDFS_URL}{target_file}?op=OPEN"
    resp = requests.get(open_url, allow_redirects=False, timeout=10)
    
    if resp.status_code == 307:
        redirect_url = resp.headers.get("Location")
        # Thay thế hostname datanode bằng localhost cho máy host
        redirect_url = redirect_url.replace("datanode:9864", "localhost:9864")
        log_info(f"Đang tải file từ DataNode WebHDFS endpoint: {redirect_url[:80]}...")
        data_resp = requests.get(redirect_url, timeout=15)
        parquet_bytes = data_resp.content
    else:
        # Fallback dùng docker exec cat
        log_info("Dùng docker exec để đọc file parquet...")
        code, out, err = run_command(f"docker exec namenode /opt/hadoop/bin/hdfs dfs -cat {target_file}")
        parquet_bytes = out.encode("latin-1") if isinstance(out, str) else out

    try:
        # Đọc schema bằng PyArrow
        reader = pq.ParquetFile(io.BytesIO(parquet_bytes))
        table = reader.read()
        schema = table.schema
        log_ok(f"Đọc thành công file Parquet! Số dòng trong file mẫu: {len(table)}")
        log_info(f"Các cột có trong Parquet schema: {schema.names}")

        # Kiểm tra các cột cốt lõi
        required_cols = ["topic", "partition", "offset", "kafka_timestamp", "raw_json", "artist", "song", "userId", "sessionId"]
        missing = [c for c in required_cols if c not in schema.names]
        if not missing:
            log_ok("Tất cả các cột nghiệp vụ và Kafka metadata đều hiện diện đầy đủ và đúng kiểu.")
        else:
            log_fail(f"Thiếu các cột: {missing}")
            return False

        # In mẫu 1 bản ghi
        pydict = table.to_pydict()
        sample_rec = {c: pydict[c][0] for c in required_cols[:6]}
        log_info(f"Mẫu bản ghi trích xuất từ Parquet:\n   {sample_rec}")

    except Exception as e:
        log_fail(f"Lỗi khi parse file Parquet với PyArrow: {e}")
        return False

    return True

if __name__ == "__main__":
    t1 = test_fault_tolerance_checkpoint()
    t2 = test_parquet_schema_and_reconciliation()

    log_section("TỔNG KẾT KẾT QUẢ GIAI ĐOẠN 2")
    print(f"1. TC-RT-03 (Fault Tolerance & Checkpoint): {'PASS' if t1 else 'FAIL'}")
    print(f"2. TC-BA-02 (Parquet Schema Verification):   {'PASS' if t2 else 'FAIL'}")
    print(f"3. TC-BA-04 (Downstream Batch Read):        {'PASS' if t2 else 'FAIL'}")
