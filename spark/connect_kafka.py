import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import schema

spark = (
    SparkSession.builder
    .appName("MusicStreaming")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
    .getOrCreate()
)
print("Khởi tạo SparkSession thành công")

# Khởi tạo schema

# Khai báo nguồn streaming Kafka
kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9093")
    .option("subscribe", "test_events")
    .option("startingOffsets", "earliest") # Đọc từ đầu
    .option("maxOffsetsPerTrigger", 10) # Số lượng events spark được phép đọc trong 1 lần xử lý.
    .load()
)

# Parse json theo auth_events và giữ lại metadata để kiểm tra / truy vết
events_df = (
    kafka_df
    .select(
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("value").cast("string").alias("raw_json"),
    )
    .withColumn("event", F.from_json(F.col("raw_json"), schema.auth_events)) # Parse Json
    .select(
        "topic",
        "partition",
        "offset",
        "kafka_timestamp",
        "raw_json",
        "event.*",
    )
)

# Xuất ra micro-batch
print("Bắt đầu lắng nghe dữ liệu từ Kafka topic 'test_events'...")
query = (
    events_df.writeStream
    .format("json")
    .outputMode("append")
    .option("path", "./data/batch/")
    .option("checkpointLocation", "./checkpoints/test_events/") # File checkpoint được tạo ra như thế nào ?
    .trigger(processingTime="5 seconds") # Nhịp chạy (có thể chờ)
    .start()
)


# Tại sao triển khai như vậy
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print("\nĐang dừng streaming query...")
    query.stop()
finally:
    spark.stop()
    print("Đã dừng SparkSession.")

