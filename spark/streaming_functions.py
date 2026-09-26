import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ──────────────────────────────────────────────
# 1. SparkSession
# ──────────────────────────────────────────────

def create_or_get_spark_session(app_name: str) -> SparkSession:
    """
    Khởi tạo hoặc lấy SparkSession hiện có.
    GCS connector JAR + fs.gs.impl được config tại đây.
    Các numeric overrides (block.size, buffersize) được đặt ở spark-defaults.conf.
    """
    gcs_jar = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../jars/gcs-connector-hadoop3-latest.jar")
    )

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0")
        .config("spark.jars", gcs_jar)
        # GCS FileSystem implementation
        .config("spark.hadoop.fs.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl",
                "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        # Auth — Application Default Credentials (gcloud auth application-default login)
        .config("spark.hadoop.google.cloud.auth.null.enable", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    return spark


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
    spark               : SparkSession hiện tại
    kafka_address       : địa chỉ Kafka bootstrap server (e.g. "localhost")
    kafka_port          : port Kafka (e.g. "9093")
    topic               : tên Kafka topic
    starting_offset     : "earliest" | "latest"
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
    Giữ lại Kafka metadata để debug / tracing.
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
        # Cột phân vùng GCS
        .withColumn("year",  F.year(F.col("ts")))
        .withColumn("month", F.month(F.col("ts")))
        .withColumn("day",   F.dayofmonth(F.col("ts")))
        .withColumn("hour",  F.hour(F.col("ts")))
        # Watermark để xử lý late data
        .withWatermark("ts", "30 minutes")
    )


# ──────────────────────────────────────────────
# 4. GCS Write Stream
# ──────────────────────────────────────────────

def create_gcs_write_stream(
    df,
    topic: str,
    bucket: str,
    trigger_interval: str = "10 seconds",
):
    """
    Ghi Structured Streaming DataFrame ra GCS dưới dạng Parquet.
    Phân vùng theo year/month/day/hour.

    Parameters
    ----------
    df               : parsed DataFrame từ parse_event
    topic            : tên topic (dùng làm tên thư mục trên GCS)
    bucket           : tên GCS bucket (không bao gồm gs://)
    trigger_interval : chu kỳ trigger micro-batch

    Returns
    -------
    StreamingQuery : đối tượng query (chưa block, cần awaitAnyTermination)
    """
    output_path     = f"gs://{bucket}/{topic}"
    checkpoint_path = f"gs://{bucket}/checkpoint/{topic}"

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
