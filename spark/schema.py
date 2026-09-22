from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
    FloatType,
    BooleanType
)

auth_events = StructType([
    StructField("ts", LongType(), True),
    StructField("sessionId", LongType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("state", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lon", FloatType(), True),
    StructField("lat", FloatType(), True),
    StructField("userId", IntegerType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", LongType(), True),
    StructField("success", BooleanType(), True),
])

listen_events = StructType([
    StructField("artist", StringType(), True),
    StructField("song", StringType(), True),
    StructField("duration", FloatType(), True),
    StructField("ts", LongType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("auth", StringType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("state", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lon", FloatType(), True),
    StructField("lat", FloatType(), True),
    StructField("userId", IntegerType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", LongType(), True),
])

page_view_events = StructType([
    StructField("ts", LongType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("page", StringType(), True),
    StructField("auth", StringType(), True),
    StructField("method", StringType(), True),
    StructField("status", IntegerType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("state", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lon", FloatType(), True),
    StructField("lat", FloatType(), True),
    StructField("userId", IntegerType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", LongType(), True),
    StructField("artist", StringType(), True),
    StructField("song", StringType(), True),
    StructField("duration", FloatType(), True),
])

status_change_events = StructType([
    StructField("ts", LongType(), True),
    StructField("sessionId", IntegerType(), True),
    StructField("auth", StringType(), True),
    StructField("level", StringType(), True),
    StructField("itemInSession", IntegerType(), True),
    StructField("city", StringType(), True),
    StructField("zip", StringType(), True),
    StructField("state", StringType(), True),
    StructField("userAgent", StringType(), True),
    StructField("lon", FloatType(), True),
    StructField("lat", FloatType(), True),
    StructField("userId", IntegerType(), True),
    StructField("lastName", StringType(), True),
    StructField("firstName", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("registration", LongType(), True),
])

EVENTS = {
    "auth_events": auth_events,
    "listen_events": listen_events,
    "page_view_events": page_view_events,  # Kafka topic name là "page_view_events"
    "status_change_events": status_change_events
}