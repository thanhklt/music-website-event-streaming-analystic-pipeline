import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ──────────────────────────────────────────────
# 1. SparkSession
# ──────────────────────────────────────────────

def create_or_get_spark_session(app_name: str) -> SparkSession:
    """
    Khởi tạo hoặc lấy SparkSession hiện có.
    HDFS native client và Kafka connector JARs được nạp tại đây.
    """
    jars_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../jars"))

    # Liệt kê tất cả JAR trong thư mục jars/ ngoại trừ gcs-connector
    jar_files = []
    if os.path.exists(jars_dir):
        jar_files = [
            os.path.join(jars_dir, f)
            for f in os.listdir(jars_dir)
        ]
    jars_str = ",".join(jar_files)

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
    )

    if jars_str:
        builder = builder.config("spark.jars", jars_str)

    return builder.getOrCreate()


# ──────────────────────────────────────────────
# 2. Kafka Read Stream
# ──────────────────────────────────────────────

def create_kafka_read_stream(
    spark: SparkSession,
    kafka_address: str,
    kafka_port: str,
    topic: str,
    starting_offset: str = "earliest",
    max_offsets_per_trigger: int = 10_000,
):
    """
    Tạo một Structured Streaming DataFrame đọc từ Kafka topic.

    Parameters
    ----------
    spark                   : SparkSession hiện tại
    kafka_address           : địa chỉ Kafka broker (e.g. "kafka" hoặc "localhost")
    kafka_port              : port Kafka (e.g. "9092" hoặc "9093")
    topic                   : tên Kafka topic
    starting_offset         : "earliest" | "latest"
    max_offsets_per_trigger : giới hạn số message mỗi micro-batch
    """
    bootstrap = f"{kafka_address}:{kafka_port}"

    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offset)
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
        .load()
    )


# ──────────────────────────────────────────────
# 3. Parse Event
# ──────────────────────────────────────────────

def parse_event(kafka_df, schema):
    """
    Parse raw Kafka value (bytes) thành các cột theo schema.
    Giữ lại Kafka metadata để debug / tracing (topic, partition, offset, kafka_timestamp).
    Thêm các cột phân vùng: year, month, day, hour.

    Parameters
    ----------
    kafka_df : raw DataFrame từ create_kafka_read_stream
    schema   : StructType schema của event tương ứng
    """
    return (
        kafka_df
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("value").cast("string").alias("raw_json"),
        )
        # Parse JSON sang struct rồi flatten
        .withColumn("event", F.from_json(F.col("raw_json"), schema))
        .select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "raw_json",
            "event.*",
        )
        # Chuyển ts từ milliseconds → timestamp
        .withColumn("ts", (F.col("ts") / 1000).cast("timestamp"))
        # Cột phân vùng HDFS
        .withColumn("year",  F.year(F.col("ts")))
        .withColumn("month", F.month(F.col("ts")))
        .withColumn("day",   F.dayofmonth(F.col("ts")))
        .withColumn("hour",  F.hour(F.col("ts")))
        # Watermark để xử lý late data
        .withWatermark("ts", "30 minutes")
    )


# ──────────────────────────────────────────────
# 4. HDFS Write Stream
# ──────────────────────────────────────────────

def create_hdfs_write_stream(
    df,
    topic: str,
    hdfs_url: str = "hdfs://namenode:9000",
    base_path: str = "/data/events",
    checkpoint_base: str = "/checkpoint",
    trigger_interval: str = "10 seconds",
):
    """
    Ghi Structured Streaming DataFrame ra HDFS dưới dạng Parquet.
    Phân vùng theo year/month/day/hour.

    Parameters
    ----------
    df               : parsed DataFrame từ parse_event
    topic            : tên topic (dùng làm tên thư mục trên HDFS)
    hdfs_url         : endpoint NameNode (ví dụ hdfs://namenode:9000 hoặc hdfs://localhost:9000)
    base_path        : thư mục gốc chứa events (/data/events)
    checkpoint_base  : thư mục gốc lưu checkpoint (/checkpoint)
    trigger_interval : chu kỳ trigger micro-batch

    Returns
    -------
    StreamingQuery   : đối tượng query (cần awaitAnyTermination)
    """
    hdfs_url = hdfs_url.rstrip("/")
    base_path = base_path.strip("/")
    checkpoint_base = checkpoint_base.strip("/")

    output_path     = f"{hdfs_url}/{base_path}/{topic}"
    checkpoint_path = f"{hdfs_url}/{checkpoint_base}/{topic}"

    return (
        df.writeStream
        .format("parquet")
        .partitionBy("year", "month", "day", "hour")
        .outputMode("append")
        .option("path", output_path)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=trigger_interval)
        .start()
    )
