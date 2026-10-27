import sys

sys.stdout.reconfigure(encoding="utf-8")


from spark.streaming import schema
from spark.streaming.functions import (
    create_or_get_spark_session,
    create_kafka_read_stream,
    parse_event,
    create_hdfs_write_stream,
)

from config.settings import read_config, Settings

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
settings: Settings = read_config()

HDFS_URL             = settings.HDFS_URL
HDFS_BASE_PATH       = settings.HDFS_BASE_PATH
HDFS_CHECKPOINT_PATH = settings.HDFS_CHECKPOINT_PATH
BOOTSTRAP_SERVERS    = settings.KAFKA_BOOTSTRAP_SERVERS

print(f"Connecting to Kafka at {BOOTSTRAP_SERVERS}")
print(f"Target HDFS endpoint: {HDFS_URL} (Base: {HDFS_BASE_PATH})")

# ──────────────────────────────────────────────
# SparkSession
# ──────────────────────────────────────────────
spark = create_or_get_spark_session("MusicStreaming")
spark.streams.resetTerminated()
print("Khởi tạo SparkSession thành công")

# ──────────────────────────────────────────────
# Streaming pipeline: mỗi Kafka topic → HDFS
# ──────────────────────────────────────────────
for topic, event_schema in schema.events.items():
    print(f"INFO: Khởi động stream cho topic '{topic}'...")

    # 1. Đọc từ Kafka
    kafka_df = create_kafka_read_stream(
        spark,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topic=topic
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
        format=settings.HDFS_OUTPUT_FORMAT,
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
