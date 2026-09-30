FROM python:3.11-slim

# Cài đặt Java Runtime và các tiện ích cần thiết
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cài đặt PySpark 4.2.0 tương thích với JARs và các thư viện cần thiết
RUN pip install --no-cache-dir pyspark==4.2.0 python-dotenv kafka-python confluent-kafka

CMD ["python", "spark/connect_kafka.py"]
