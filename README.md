# eventsim-analystic-pipeline

## Kafka cluster local: 3 broker

Compose chuẩn nằm tại `infra/docker-compose.yml`. Ba node KRaft kiêm
broker/controller dùng **2 partition/topic nghiệp vụ, RF=2, min ISR=2**.
Topic nội bộ Kafka giữ số partition mặc định riêng. Không đặt giới hạn CPU/RAM
hoặc override JVM heap. Ba container trên một máy không chịu được mất cả máy.

| Broker | Từ host Windows | Từ container trong `eventsim-net` | Volume mới |
|---|---|---|---|
| kafka-1 | 127.0.0.1:9093 | kafka-1:9092 | eventsim-kafka-v2-1-data |
| kafka-2 | 127.0.0.1:19093 | kafka-2:9092 | eventsim-kafka-v2-2-data |
| kafka-3 | 127.0.0.1:29093 | kafka-3:9092 | eventsim-kafka-v2-3-data |

INTERNAL/CONTROLLER không publish ra host; EXTERNAL chỉ bind IPv4 loopback.
Ordering chỉ được bảo đảm trong từng partition. Giữ key theo user hiện có.
Với producer `acks=all`, partition thiếu một replica đồng bộ sẽ tạm ngừng nhận
ghi; đây là hành vi mong muốn của RF=2/min ISR=2. Quorum controller còn 2/3
voter không có nghĩa mọi partition còn ghi được.

### Setup và start (PowerShell, từ root repository)

```powershell
if (-not (Test-Path .env)) { Copy-Item .env.example .env }
poetry install --no-root
```

Khi tạo cluster lần đầu, sinh **một** UUID mới, ghi vào `KAFKA_CLUSTER_ID`
trong `.env`, giữ nguyên UUID đó ở mọi lần restart:

```powershell
docker run --rm --entrypoint /opt/kafka/bin/kafka-storage.sh apache/kafka:4.3.1 random-uuid
```

UUID trong `.env.example` chỉ là ví dụ; không dùng storage của cluster cũ.
Nếu `.env` đã có nội dung, chỉ bổ sung/cập nhật các khóa cần thiết, không copy
đè file. Cấu hình host cần dùng:

```dotenv
KAFKA_CLUSTER_ID=<UUID-moi-da-sinh>
KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:9093,127.0.0.1:19093,127.0.0.1:29093
HDFS_BASE_PATH=/data/events-kafka-v2
HDFS_CHECKPOINT_PATH=/checkpoint/kafka-v2
```

Không đổi cluster ID khi các volume mới đã có dữ liệu. Kafka sẽ từ chối storage
không khớp ID; không format/xóa volume để chữa lỗi. Broker cũ tên `kafka` và các
volume cũ được giữ nguyên. Nếu broker cũ còn chạy và chiếm port 9093, dừng nó
bằng `docker stop kafka` trước khi khởi động cluster mới.

```powershell
docker compose --env-file .env -f .\infra\docker-compose.yml config --quiet
docker compose --env-file .env -f .\infra\docker-compose.yml up -d --wait kafka-1 kafka-2 kafka-3
poetry run python .\scripts\test_kafka_cluster.py --provision --check
```

Lệnh provision tạo/validate bốn topic nghiệp vụ, không xóa hoặc thay assignment
của topic có policy sai. Auto-create dùng cùng default 2 partition/RF=2/min ISR=2.
Spark cũng validate policy trước khi chạy và thoát lỗi sau deadline 60s.
Ví dụ Spark/JAR hiện dùng: PySpark 4.2.0, spark-sql-kafka-0-10 và
spark-token-provider-kafka-0-10 Scala 2.13 phiên bản 4.2.0, kafka-clients 3.9.2.

Khởi động HDFS mới và Spark:

```powershell
docker compose --env-file .env -f .\infra\docker-compose.yml up -d --wait namenode datanode
docker compose --env-file .env -f .\infra\docker-compose.yml up -d --build spark-streaming
.\scripts\eventsim_start.ps1
```

Eventsim vẫn chạy độc lập. Script mặc định tạo `eventsim-v2`, không xóa/recreate
container đã tồn tại. Có thể override `EVENTSIM_CONTAINER`, `EVENTSIM_IMAGE`,
`EVENTSIM_KAFKA_NETWORK`, `EVENTSIM_KAFKA_BOOTSTRAP_SERVERS` bằng `$env:...`.
Các script Eventsim đọc biến môi trường của process, không tự source `.env`.
Sample với image `eventsim:2.0` hiện có đã kết nối và gửi dữ liệu thành công;
runtime log cho thấy producer bên trong dùng **`acks=1`** (Kafka client 2.6.0).
CLI hiện có không cung cấp tùy chọn acks, nên chưa đổi assembly trong nhiệm vụ
này. Không áp bảo đảm delivery của producer kiểm chứng `acks=all` cho Eventsim.

Output mới là `/data/events-kafka-v2/<topic>`, checkpoint mới là
`/checkpoint/kafka-v2/<topic>`. Giữ nguyên hai path khi restart. Không dùng
checkpoint hoặc output `_spark_metadata` của cluster cũ. `startingOffsets`
chỉ áp dụng khi chưa có checkpoint hợp lệ.

### Smoke test và kiểm tra phục hồi

```powershell
poetry run python -m unittest discover -s .\scripts\tests -p test_kafka_clients.py -v
poetry run python .\scripts\test_kafka_cluster.py --check
poetry run python .\scripts\test_kafka_cluster.py --policy-test
# Hai lệnh sau chủ động stop/start broker mới; chạy khi không có workload khác:
poetry run python .\scripts\test_kafka_cluster.py --fault-test --restart-test
# HDFS đang hoạt động: dùng namespace test riêng trên HDFS đó:
poetry run python .\scripts\pipeline_smoke.py
# Hoặc dùng cả HDFS test mới, không đụng storage HDFS hiện có:
poetry run python .\scripts\pipeline_smoke.py --isolated-hdfs
```

Smoke tạo topic prefix riêng, fixture 2 auth/4 listen/2 page/2 status; đối chiếu
offset ở cả hai partition với từng dòng Parquet trong commit log. Sau restart
Spark, fixture được gửi thêm ở offset mới: raw tăng gấp đôi, không replay offset
đã commit; dbt staging dedup business event nên dashboard vẫn có **4 lượt nghe,
2 người nghe, 360 giây**. Không suy ra exactly-once cho toàn pipeline.

Ingestion chạy hai lần, so sánh số dòng và ledger; chỉ tải file đã commit theo
`_spark_metadata`. `dbt build` dùng project/profile/database test; dashboard query
layer đọc database test sau khi dbt đóng writer. Warehouse thật không bị ghi.
`scripts/run_pipeline_tests.py` và `scripts/test_recovery_and_batch.py` là các
entrypoint tương ứng cho smoke không recovery và recovery không downstream.
Không tuyên bố kiểm chứng quarantine: parser hiện tại chưa có sink quarantine.

Artifacts được giữ tại `data/cluster-smoke/<run-id>/` gồm manifest, database,
dbt log, Spark log và kết quả. HDFS/topics/volume test cũng được giữ để kiểm tra;
container test được dừng, không tự xóa dữ liệu. `--isolated-hdfs` cần hai port
host **19870 và 9864** còn trống, tạo network và volume `cluster-hdfs-smoke-*`
riêng; không chạy đồng thời nhiều smoke dùng tùy chọn này.

**HDFS đã làm mới theo yêu cầu người dùng:** volume `eventsim-hdfs-v2-name`
và `eventsim-hdfs-v2-data` mount đúng `/opt/hadoop/dfs/name` và
`/opt/hadoop/dfs/data` như `hdfs-site.xml`. Service `hdfs-init` chỉ đặt owner
hai thư mục gốc cho user hadoop. NameNode chỉ format storage trống khi thiếu
`current/VERSION`; storage không trống mà mất VERSION sẽ bị từ chối. Restart
với VERSION hợp lệ giữ metadata. DataNode healthcheck yêu cầu đăng ký với
NameNode; Spark chờ cả Kafka, NameNode và DataNode healthy.

Lúc chuyển đổi, startup guard cũ kiểm tra sai đường dẫn và đã format metadata
NameNode khi service được khởi động. Sau khi được thông báo, người dùng xác
nhận không cần metadata cũ, sẽ chạy lại pipeline và yêu cầu xóa volume cũ.
Các volume Kafka/HDFS cũ được xóa theo yêu cầu đó; HDFS v2 là filesystem mới.

### Stop, restart và ingestion

```powershell
docker compose --env-file .env -f .\infra\docker-compose.yml stop
docker compose --env-file .env -f .\infra\docker-compose.yml up -d --wait kafka-1 kafka-2 kafka-3
# Eventsim độc lập, dừng riêng khi cần:
docker stop eventsim-v2
```

Không dùng `down -v`, `--remove-orphans`, xóa checkpoint hoặc đổi UUID để restart.
Khi chuyển dữ liệu mới vào warehouse thật sau review: đặt `HDFS_BASE_PATH` mới,
đóng Streamlit và mọi DuckDB writer (kể cả container CLI `duckdb_analytics`),
chạy ingestion rồi dbt lần lượt, sau đó refresh dashboard. Không tự full-refresh.

```powershell
poetry run python .\spark\batch\hdfs_to_duckdb.py
poetry run dbt build --project-dir .\dbt\duckdb_warehouse
```

Volume Kafka/HDFS cũ đã được người dùng yêu cầu xóa, nên không có rollback
dữ liệu cũ bằng những volume đó. Có thể lấy cấu hình cũ bằng
`git show HEAD:kafka/docker-compose.yml` để tham khảo; không gắn volume v2 vào
cluster ID cũ. Manifest các volume đã nghỉ nằm trong `data/cluster-smoke/`.

Repository hiện đã track `.env`; `.gitignore` không tự gỡ tracking file đó.
Không stage/commit file này hoặc đưa secret vào log/tài liệu.

## Dashboard Streamlit

Báo cáo tổng hợp từ tầng `dashboard` (`dashboard.song_plays` và `dashboard.filter_options`):
top 5 tiểu bang, top 5 bài hát, heatmap thứ/giờ UTC, bài hát đứng đầu theo giới tính,
xu hướng lượt nghe và top nghệ sĩ.

Thứ tự vận hành chuẩn:
1. Upstream marts hoàn tất hợp lệ.
2. Dựng tầng dashboard bằng dbt (đóng writer sau khi xong):
```powershell
poetry run dbt build --project-dir .\dbt\duckdb_warehouse --select path:models/dashboard
```
(Nếu cần build lại cả upstream marts, dùng: `poetry run dbt build --project-dir .\dbt\duckdb_warehouse --select +path:models/dashboard`).
3. Khởi chạy Streamlit app từ root repository bằng PowerShell:
```powershell
poetry install --no-root
poetry run streamlit run .\streamlit\app.py --server.address 127.0.0.1 --server.port 8501
```

Mở http://localhost:8501; dừng bằng `Ctrl+C`. Database mặc định:
`infra/duckdb/data/warehouse.duckdb`. Bấm **Làm mới dữ liệu** sau mỗi lần build dbt thành công.
Xem [hướng dẫn dashboard](streamlit/README.md) để đổi database, đọc định nghĩa
thời lượng, tránh xung đột writer và chạy tests.

## On-going
<img src="./images/architecture_design.png" alt="architecture_design" width="400"/>


- Lý do tách eventsim ra khỏi docker-compose vì em muốn để kafka luôn chạy trước rồi mới chạy eventsim thay vì để 2 service gắn với nhau để phù hợp với thực tế.

- Hiện tại đang script đang dedupe pipeline ngay bước ingest (lý do là dedupe record trùng hoàn toàn, còn record bị trùng về nghiệp vụ sẽ được xử lý khi transform bằng dbt)

- Insight: end_time khi phát có thể null nếu nó là event cuối cùng của session.

- Phân loại:
Night: 00:00–04:59
Morning: 05:00–11:59
Afternoon: 12:00–16:59
Evening: 17:00–20:59
Night: 21:00–23:59
