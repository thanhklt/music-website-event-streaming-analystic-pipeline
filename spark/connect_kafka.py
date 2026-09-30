import sys
sys.stdout.reconfigure(encoding="utf-8")

import os
import time
from dotenv import load_dotenv

import schema
from streaming_functions import (
    create_or_get_spark_session,
    create_kafka_read_stream,
    parse_event,
    create_hdfs_write_stream,
)

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
load_dotenv()

KAFKA_ADDRESS        = os.getenv("KAFKA_ADDRESS", "kafka")
KAFKA_PORT           = os.getenv("KAFKA_PORT", "9092")
HDFS_URL             = os.getenv("HDFS_URL", "hdfs://namenode:9000")
HDFS_BASE_PATH       = os.getenv("HDFS_BASE_PATH", "/data/events")
HDFS_CHECKPOINT_PATH = os.getenv("HDFS_CHECKPOINT_PATH", "/checkpoint")
BOOTSTRAP_SERVERS    = f"{KAFKA_ADDRESS}:{KAFKA_PORT}"

print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}")
print(f"Target HDFS endpoint: {HDFS_URL} (Base: {HDFS_BASE_PATH})")

def ensure_kafka_topics(bootstrap: str, topics: list) -> None:
    """Đảm bảo các Kafka topic tồn tại trước khi khởi tạo readStream."""
    try:
        from confluent_kafka.admin import AdminClient, NewTopic
        admin = AdminClient({"bootstrap.servers": bootstrap})
        metadata = admin.list_topics(timeout=10)
        missing = [t for t in topics if t not in metadata.topics]
        if missing:
            print(f"Khởi tạo các topic còn thiếu: {missing}")
            new_topics = [NewTopic(t, num_partitions=1, replication_factor=1) for t in missing]
            futures = admin.create_topics(new_topics)
            for t, f in futures.items():
                f.result()
                print(f"[OK] Đã tạo topic '{t}'")
    except Exception as exc:
        print(f"[WARN] Không thể khởi tạo topic tự động: {exc}")

# Tự động tạo topic nếu chưa có
ensure_kafka_topics(BOOTSTRAP_SERVERS, list(schema.EVENTS.keys()))

# ──────────────────────────────────────────────
# SparkSession
# ──────────────────────────────────────────────
spark = create_or_get_spark_session("MusicStreaming")
spark.streams.resetTerminated()
print("Khởi tạo SparkSession thành công")

# ──────────────────────────────────────────────
# Streaming pipeline: mỗi Kafka topic → HDFS
# ──────────────────────────────────────────────
for topic, event_schema in schema.EVENTS.items():
    print(f"INFO: Khởi động stream cho topic '{topic}'...")

    # 1. Đọc từ Kafka
    kafka_df = create_kafka_read_stream(
        spark,
        kafka_address=KAFKA_ADDRESS,
        kafka_port=KAFKA_PORT,
        topic=topic,
    )

    # 2. Parse JSON + thêm partition columns
    parsed_df = parse_event(kafka_df, event_schema)

    # 3. Ghi ra HDFS (parquet, phân vùng year/month/day/hour)
    create_hdfs_write_stream(
        df=parsed_df,
        topic=topic,
        hdfs_url=HDFS_URL,
        base_path=HDFS_BASE_PATH,
        checkpoint_base=HDFS_CHECKPOINT_PATH,
    )

print("Tất cả streams đã khởi động. Đang lắng nghe...")

# ──────────────────────────────────────────────
# Chờ cho đến khi có stream nào kết thúc hoặc lỗi
# ──────────────────────────────────────────────
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nĐang dừng tất cả streaming queries...")
    for q in spark.streams.active:
        q.stop()
finally:
    spark.stop()
    print("Đã dừng SparkSession.")
