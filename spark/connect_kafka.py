import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import schema

spark = (
    SparkSession.builder
    .appName("MusicStreaming")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
print("Khởi tạo SparkSession thành công")


for event_str in schema.event_dict:
    print(f"INFO: Đang xử lý event: {event_str}")

# Khai báo nguồn streaming Kafka
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9093")
        .option("subscribe", event_str)
        .option("startingOffsets", "earliest") # Đọc từ đầu
        .option("maxOffsetsPerTrigger", 10000) # Số lượng events spark được phép đọc trong 1 lần xử lý.
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
        .withColumn("event", F.from_json(F.col("raw_json"), schema.event_dict[event_str])) # Parse Json
        .select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "raw_json",
            "event.*",
        )
        .withColumn("ts", (F.col("ts") / 1000).cast("timestamp"))
        .withColumn("year", F.year(F.col("ts")))
        .withColumn("month", F.month(F.col("ts")))
        .withColumn("day", F.dayofmonth(F.col("ts")))
        .withColumn("hour", F.hour(F.col("ts")))
        .withWatermark("ts", "30 minutes")
    )



# Xuất ra micro-batch
    print(f"Bắt đầu lắng nghe dữ liệu từ Kafka topic '{event_str}'...")
    query = (
        events_df.writeStream
        .format("json")
        .partitionBy("year", "month", "day", "hour")
        .outputMode("append")
        .option("path", f"./data/batch/{event_str}")
        .option("checkpointLocation", f"./checkpoints/{event_str}/") # File checkpoint được tạo ra như thế nào ?
        .trigger(processingTime="10 seconds") # Nhịp chạy (có thể chờ)
        .start()
    )


try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nĐang dừng tất cả streaming queries...")
    for query in spark.streams.active:
        query.stop()
finally:
    spark.stop()
    print("Đã dừng SparkSession.")

