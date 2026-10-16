"""
kafka/test_producer.py
Tao mot Producer va load du lieu tu thu muc test_data/ vao topic 'test_events'

Du lieu dau vao: cac file *.jsonl trong kafka/test_data/
Library: confluent-kafka (da co trong pyproject.toml)
"""

import json
import os
import time
import sys
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'spark'))
from kafka_cluster import bootstrap_servers, ensure_kafka_topics
load_dotenv(Path(__file__).resolve().parents[1] / '.env')

# ── Cau hinh ────────────────────────────────────────────────
BOOTSTRAP_SERVERS = bootstrap_servers()
TOPIC_NAME        = "test_events"
NUM_PARTITIONS    = 2
REPLICATION_FACTOR = 2

# Thu muc chua du lieu test (kafka/test_data/)
SCRIPT_DIR = Path(__file__).parent
TEST_DATA_DIR = SCRIPT_DIR / "test_data"

# ── Helper ───────────────────────────────────────────────────
def create_topic_if_not_exists(bootstrap_servers: str, topic: str) -> None:
    """Tao topic neu chua ton tai."""
    ensure_kafka_topics(bootstrap_servers, [topic])


def delivery_report(err, msg) -> None:
    """Callback duoc goi khi message duoc giao thanh cong / that bai."""
    if err:
        print(f"[ERROR] Giao that bai | partition={msg.partition()} | {err}")
    else:
        print(
            f"[OK]    Da gui | topic={msg.topic()} "
            f"| partition={msg.partition()} | offset={msg.offset()}"
        )


def load_and_produce(producer: Producer, topic: str, data_dir: Path) -> int:
    """Doc tung file .jsonl trong data_dir va gui len Kafka. Tra ve tong so message da gui."""
    jsonl_files = sorted(data_dir.glob("*.jsonl"))
    if not jsonl_files:
        print(f"[WARN]  Khong tim thay file .jsonl nao trong: {data_dir}")
        return 0

    total = 0
    delivered = 0
    failures = []
    def on_delivery(err, msg):
        nonlocal delivered
        delivery_report(err, msg)
        if err is None:
            delivered += 1
        else:
            failures.append(str(err))
    for file_path in jsonl_files:
        print(f"\n[INFO]  Dang doc file: {file_path.name}")
        with open(file_path, "r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                except json.JSONDecodeError as exc:
                    print(f"[WARN]  Dong {line_no} khong phai JSON hop le: {exc}")
                    continue

                # Dung userId lam key de dam bao cung user vao cung partition
                key = str(record.get("userId", "")).encode("utf-8")
                value = json.dumps(record, ensure_ascii=False).encode("utf-8")

                producer.produce(
                    topic=topic,
                    key=key,
                    value=value,
                    callback=on_delivery,
                )

                # Poll dinh ky de xu ly callback va tranh day buffer
                producer.poll(0)
                total += 1

        print(f"[INFO]  Xong file {file_path.name} — da queue {total} messages den hien tai")

    pending = producer.flush(35)
    if pending or failures or delivered != total:
        raise RuntimeError(f'Delivery failed: queued={total}, acknowledged={delivered}, '
                           f'pending={pending}, errors={len(failures)}')
    return delivered


# ── Main ─────────────────────────────────────────────────────
def main() -> None:
    print("=" * 60)
    print("  Kafka Test Producer")
    print(f"  Bootstrap : {BOOTSTRAP_SERVERS}")
    print(f"  Topic     : {TOPIC_NAME}")
    print(f"  Data dir  : {TEST_DATA_DIR}")
    print("=" * 60)

    # 1. Tao topic neu chua co
    create_topic_if_not_exists(BOOTSTRAP_SERVERS, TOPIC_NAME)

    # 2. Tao Producer
    producer_config = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "acks": "all",                  # dam bao do bao tin
        "retries": 3,
        "delivery.timeout.ms": 30_000,
        "linger.ms": 5,                 # gom nhom message de tang throughput
        "batch.size": 16384,
    }
    producer = Producer(producer_config)
    print(f"\n[OK]    Da tao Producer ket noi toi {BOOTSTRAP_SERVERS}")

    # 3. Gui du lieu
    start_time = time.time()
    total_sent = load_and_produce(producer, TOPIC_NAME, TEST_DATA_DIR)

    # 4. Flush — doi tat ca message trong queue duoc gui het
    # load_and_produce already flushes and rejects delivery errors.

    elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print(f"[OK]    Hoan thanh! Da gui {total_sent} messages trong {elapsed:.2f}s")
    print("=" * 60)


if __name__ == "__main__":
    main()
