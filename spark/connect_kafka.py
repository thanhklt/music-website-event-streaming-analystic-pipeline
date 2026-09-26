import sys
sys.stdout.reconfigure(encoding="utf-8")

import os
from dotenv import load_dotenv

import schema
from streaming_functions import (
    create_or_get_spark_session,
    create_kafka_read_stream,
    parse_event,
    create_gcs_write_stream,
)

# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
load_dotenv()

KAFKA_ADDRESS = os.getenv("KAFKA_ADDRESS", "localhost")
KAFKA_PORT    = os.getenv("KAFKA_PORT", "9093")
GCP_BUCKET    = os.environ.get("GCP_GCS_BUCKET")

# ──────────────────────────────────────────────
# SparkSession
# ──────────────────────────────────────────────
spark = create_or_get_spark_session("MusicStreaming")
spark.streams.resetTerminated()
print("Khởi tạo SparkSession thành công")

# ──────────────────────────────────────────────
# Streaming pipeline: mỗi Kafka topic → GCS
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

    # 3. Ghi ra GCS (parquet, phân vùng year/month/day/hour)
    create_gcs_write_stream(
        df=parsed_df,
        topic=topic,
        bucket=GCP_BUCKET,
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
