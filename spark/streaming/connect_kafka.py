import sys
sys.stdout.reconfigure(encoding="utf-8")

import os
import time
from dotenv import load_dotenv
from kafka_cluster import bootstrap_servers, ensure_kafka_topics, INTERNAL_BOOTSTRAP

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

HDFS_URL             = os.getenv("HDFS_URL", "hdfs://namenode:9000")
HDFS_BASE_PATH       = os.getenv("HDFS_BASE_PATH", "/data/events-kafka-v2")
HDFS_CHECKPOINT_PATH = os.getenv("HDFS_CHECKPOINT_PATH", "/checkpoint/kafka-v2")
BOOTSTRAP_SERVERS    = bootstrap_servers(default=INTERNAL_BOOTSTRAP)
TOPIC_PREFIX         = os.getenv("KAFKA_TOPIC_PREFIX", "")  # isolate live smoke topics

print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}")
print(f"Target HDFS endpoint: {HDFS_URL} (Base: {HDFS_BASE_PATH})")

# Tự động tạo topic nếu chưa có
ensure_kafka_topics(BOOTSTRAP_SERVERS, [TOPIC_PREFIX + topic for topic in schema.EVENTS])

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
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=TOPIC_PREFIX + topic,
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
