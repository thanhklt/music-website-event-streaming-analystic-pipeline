# Kafka 3 Broker Cluster Implementation Plan

> Trạng thái: **đã được duyệt và đang triển khai** bằng `superpowers:executing-plans`. Không stage, commit, push, tạo PR hoặc deploy.
>
> Cập nhật phạm vi được người dùng xác nhận sau khi duyệt: không cần metadata HDFS cũ, sẽ chạy lại pipeline, **xóa volume cũ**. Cấu hình HDFS v2 dùng volume mới và mount/guard đúng đường dẫn; volume Kafka/HDFS cũ được kiểm tra rồi xóa có mục tiêu. Điều này thay thế các yêu cầu giữ volume/metadata cũ trong plan gốc bên dưới. Warehouse DuckDB thật không nằm trong phạm vi xóa/full-refresh. Chi tiết thực thi nằm trong tài liệu progress cùng thư mục.

**Goal:** Hợp nhất hai Compose vào `infra/docker-compose.yml`, chạy cluster Kafka mới gồm 3 broker, 2 partition/topic, replication factor 2 và min ISR 2.

**Architecture:** Ba node KRaft kiêm broker/controller, có node ID và volume riêng, cùng cluster ID và controller quorum. Spark và Eventsim kết nối qua danh sách bootstrap gồm cả ba broker. HDFS vẫn là data lake, DuckDB/dbt/dashboard giữ kiến trúc hiện có.

**Tech Stack:** `apache/kafka:4.3.1`, Docker Compose, Spark/PySpark `4.2.0`, Kafka connector Spark `4.2.0` cho Scala `2.13`, Kafka client JAR `3.9.2`, Hadoop `3.3.6`, confluent-kafka hiện có. Không nâng dependency hoặc đổi công nghệ trong nhiệm vụ này.

**Spec:** Yêu cầu và các lựa chọn người dùng đã chốt trong hội thoại: cluster mới với volume riêng; RF=2; min ISR=2; tài nguyên mặc định; **2 partition/topic**. Thiết kế và các giả định để duyệt được ghi ngay dưới đây.

## Global Constraints

- Tạo cluster mới với volume riêng; giữ nguyên volume Kafka cũ, HDFS, DuckDB hiện có và checkpoint cũ.
- Có 3 broker; topic nghiệp vụ và topic test mới có 2 partition, RF=2, `min.insync.replicas=2`.
- Giữ mặc định CPU/RAM/JVM của Docker/image; không thêm resource limits/reservations, không đổi cấu hình Docker Desktop/WSL.
- Producer dùng để kiểm chứng phải dùng `acks=all`, có timeout hữu hạn và kiểm tra delivery callback/flush.
- Chỉ triển khai sau khi người dùng duyệt plan. Không stage, commit, push, tạo PR, deploy hoặc tự accept thay đổi.
- Giữ nguyên những thay đổi đang có trong working tree; chỉ sửa phần liên quan Kafka/đường dẫn và tài liệu vận hành.
- Không xóa volume, format storage cũ, xóa checkpoint, reset offset hoặc full-refresh warehouse thật để test.
- Test ghi vào topic, HDFS path, local staging và DuckDB test riêng; đóng writer trước khi dbt/dashboard đọc database.

## Review Focus

1. Đổi cluster nhưng dùng lại checkpoint/output cũ: phải dùng namespace mới cho cả hai, tránh offset không tồn tại và xung đột `_spark_metadata`.
2. Broker thiếu hoặc khởi động chậm: healthcheck thực sự gọi Kafka API; retry có deadline; lỗi phải trả exit code khác 0.
3. Có topic sai RF/partition/min ISR: phát hiện và báo lỗi rõ ràng, không coi topic tồn tại là đã đúng và không tự xóa/tạo lại.
4. Một replica của partition dừng: producer `acks=all` không được báo thành công khi thiếu min ISR; khôi phục broker và chờ ISR đủ trước khi kết luận phục hồi.
5. Client từ host và container: phải nhận metadata với địa chỉ truy cập được cho mọi broker, kể cả khi broker bootstrap đầu tiên dừng.

## Thiết kế đề xuất để duyệt

### Topology và kết nối

Chọn **3 node combined broker/controller** cho môi trường local. Phương án 3 controller riêng + 3 broker tăng số container không cần thiết cho mục tiêu này; mở rộng cluster cũ không phù hợp lựa chọn tạo cluster mới.

Giữ static controller quorum, cùng kiểu cấu hình hiện tại và ví dụ chính thức của image Kafka 4.3.1. Cả ba node dùng cùng danh sách `1@kafka-1:9094,2@kafka-2:9094,3@kafka-3:9094`. Không trộn static voters và dynamic quorum bootstrap. Đổi membership controller sau này là phạm vi riêng.

| Service / node ID | Docker INTERNAL | Host EXTERNAL | Volume mới |
|---|---|---|---|
| `kafka-1` / 1 | `kafka-1:9092` | `localhost:9093` | `eventsim-kafka-v2-1-data` |
| `kafka-2` / 2 | `kafka-2:9092` | `localhost:19093` | `eventsim-kafka-v2-2-data` |
| `kafka-3` / 3 | `kafka-3:9092` | `localhost:29093` | `eventsim-kafka-v2-3-data` |

- Trong mỗi container: INTERNAL 9092, EXTERNAL 9093, CONTROLLER 9094; mapping host của broker 2/3 lần lượt là `19093:9093` và `29093:9093`.
- Chỉ publish EXTERNAL trên `127.0.0.1`; INTERNAL và CONTROLLER dùng Docker network `eventsim-net` hiện có.
- `KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL`; security protocol giữ PLAINTEXT cho local.
- `KAFKA_LOG_DIRS=/var/lib/kafka/data`, mount đúng volume mới vào đường dẫn này; metadata cũng nằm trên persistent storage.
- Một UUID mới, sinh một lần bằng `kafka-storage.sh random-uuid`, được lưu thành `KAFKA_CLUSTER_ID` trong `.env` local và truyền vào `CLUSTER_ID` của cả ba node. Không sinh lại UUID mỗi lần start và không chép UUID từ storage cũ. `.env.example` có UUID mẫu hợp lệ để validate cấu hình.
- `.env` chỉ thêm/cập nhật khóa cần thiết; không ghi đè toàn bộ nội dung hoặc in nội dung file ra log.

### Topic policy và startup

- `default.replication.factor=2`, `num.partitions=2`, `min.insync.replicas=2` trên cả ba broker.
- `offsets.topic.replication.factor=2`, `transaction.state.log.replication.factor=2`, `transaction.state.log.min.isr=2`.
- Hai partition chỉ áp dụng cho topic nghiệp vụ/test; topic nội bộ Kafka giữ số partition mặc định riêng. KRaft metadata quorum có 3 controller, không phải RF=2 của topic nghiệp vụ.
- Giữ auto-create topic nhưng cho nó dùng default 2 partition/RF=2. Spark tiếp tục là nơi bảo đảm bốn topic `auth_events`, `listen_events`, `page_view_events`, `status_change_events` tồn tại và đúng policy.
- Khi tạo bằng AdminClient, truyền tường minh 2 partition, RF=2 và topic config min ISR=2; xử lý race `TopicAlreadyExists` bằng đọc lại và validate.
- Mỗi broker có Kafka API healthcheck, interval 10s, timeout 10s, retries 12, start period 30s. Spark chỉ start sau khi cả ba broker healthy.
- Topic provisioning/validation có deadline tổng 60s. Topic sai policy hoặc hết deadline làm Spark thoát lỗi, thay cho chỉ in warning rồi tiếp tục.
- RF=2/min ISR=2 ưu tiên durability: thiếu một replica đồng bộ sẽ chặn ghi vào partition bị ảnh hưởng. Controller quorum vẫn có thể hoạt động với 2/3 controller; điều này không bảo đảm mọi partition còn ghi được.
- Hai partition không tạo ordering toàn topic; chỉ có ordering trong từng partition. Giữ message key hiện tại, không thay schema hoặc logic nghiệp vụ.

### Tách trạng thái streaming

- Đề xuất namespace mới mặc định: output `/data/events-kafka-v2`, checkpoint `/checkpoint/kafka-v2`, mỗi topic vẫn có nhánh riêng.
- Giữ nguyên `/data/events` và `/checkpoint` cũ; không tái sử dụng `_spark_metadata` hoặc offset cũ cho cluster mới.
- Đồng bộ `HDFS_BASE_PATH` giữa Compose, `.env.example`, ingestion và test scripts; Compose dùng biến môi trường thay cho giá trị hardcode. Namespace mới giữ nguyên qua restart.
- Ingestion hiện có đã hỗ trợ `HDFS_BASE_PATH`; chỉ đổi cấu hình/lệnh chạy, không viết lại ledger hoặc logic dbt.
- Warehouse thật giữ nguyên. Smoke test downstream dùng database/profile/staging test riêng. Đưa dữ liệu cluster mới vào warehouse thật là thao tác vận hành sau khi người dùng review kết quả test.
- Eventsim vẫn chạy độc lập sau Kafka. Script không còn suy network từ duy nhất container `kafka`.

## Phạm vi file

| File | Thay đổi dự kiến |
|---|---|
| `infra/docker-compose.yml` | Thay broker đơn bằng 3 node, volume, listener, policy, healthcheck; cập nhật Spark dependency/environment |
| `kafka/docker-compose.yml` | Bỏ Compose trùng sau khi các lệnh/tham chiếu đã chuyển sang `infra/` |
| `.env.example` | Thêm cluster ID mẫu, bootstrap list host, 2 partition/RF/min ISR và namespace streaming mới |
| `spark/connect_kafka.py` | Bootstrap list; tạo/validate topic đúng policy, lỗi và retry hữu hạn |
| `spark/streaming_functions.py` | Nhận bootstrap list trực tiếp thay cho ghép address/port |
| `kafka/test_producer.py` | Bootstrap list; tạo test topic 2 partition/RF=2/min ISR=2; phản ánh đúng delivery failure |
| `scripts/eventsim_start.ps1`, `scripts/eventsim_start.sh` | Bootstrap list, network rõ ràng, kiểm tra readiness của cluster |
| `scripts/run_pipeline_tests.py`, `scripts/test_recovery_and_batch.py` | Bỏ path hardcode, dùng cấu hình test; kiểm tra dữ liệu thay vì chỉ container running |
| `scripts/test_kafka_cluster.py` (mới) | Kiểm chứng metadata, hai partition, min ISR, failover/recovery bằng topic test riêng |
| `README.md` | Lệnh PowerShell từ root, chuyển đổi an toàn, startup/stop/restart và giới hạn |

## Task 1: Hợp nhất Compose và tạo cluster mới

**Files:** `infra/docker-compose.yml`, `.env.example`; cuối task loại `kafka/docker-compose.yml` khỏi working tree sau khi giữ được cấu hình cần thiết.

**Interfaces:** Xuất ba endpoint và ba volume đúng bảng topology; `KAFKA_CLUSTER_ID` bắt buộc, dùng `--env-file .env.example` khi validate cấu hình mẫu. Chưa đổi logic Spark trong task này.

- [ ] Kiểm tra Git, Docker daemon và image; đọc lại Compose cũ, ghi nhận tên/mount volume hiện hữu bằng output chọn lọc, không đọc secret. Nếu tên volume mới đã tồn tại với cluster khác, dừng và báo xung đột thay vì tái sử dụng/format.
- [ ] Sửa Compose theo topology, pin Kafka 4.3.1, cấu hình log directory, cluster ID, topic defaults/internal RF và healthcheck. Không đổi volume HDFS/DuckDB hoặc image khác.
- [ ] Chuyển dependency Spark từ `kafka` sang cả ba broker healthy; environment truyền bootstrap list và các HDFS path mới.
- [ ] Đồng bộ `.env.example`; ghi hướng dẫn sinh UUID một lần và cập nhật có chọn lọc `.env` local khi triển khai. Bỏ Compose trùng tại `kafka/`; giữ producer/fixtures trong thư mục này.
- [ ] Chạy `docker compose --env-file .env.example -f infra/docker-compose.yml config --quiet` và inspect `config --format json` chọn lọc. Kỳ vọng exit 0, ba service/ID/volume/host port riêng, không còn broker đơn `kafka`, không có resource/JVM overrides mới.
- [ ] Khi Docker khả dụng và thực thi đã được duyệt: dừng broker cũ nếu chiếm port 9093, giữ container/volume cũ; chạy riêng ba broker mới. Kiểm tra quorum có 3 voter, cùng cluster ID và log thực sự ghi vào volume mới.

## Task 2: Đồng bộ Spark và producer test với topic policy

**Files:** `spark/connect_kafka.py`, `spark/streaming_functions.py`, `kafka/test_producer.py`, `.env.example`.

**Interfaces:** `KAFKA_BOOTSTRAP_SERVERS` chứa chuỗi `host:port` phân cách bằng dấu phẩy. Spark trong Docker nhận danh sách INTERNAL; producer/test trên host nhận EXTERNAL. Các giá trị topic policy là 2 partition, RF=2 và min ISR=2.

- [ ] Ưu tiên `KAFKA_BOOTSTRAP_SERVERS`; giữ fallback address/port cũ khi biến mới không được đặt. Compose và `.env.example` dùng bootstrap list mới làm cấu hình chuẩn.
- [ ] Đổi `create_kafka_read_stream(spark, bootstrap_servers: str, topic: str, starting_offset: str = "earliest", max_offsets_per_trigger: int = 10_000)` và caller; truyền nguyên chuỗi vào `kafka.bootstrap.servers`. Không đổi parse/schema/watermark/transformation.
- [ ] Cập nhật `ensure_kafka_topics(bootstrap: str, topics: list) -> None`: tạo topic thiếu với policy mới, kiểm tra partition/RF/effective min ISR của topic có sẵn; mọi Admin operation có timeout trong deadline 60s. Không tự sửa assignment hay xóa topic sai policy.
- [ ] Cập nhật `test_producer.py` tạo topic test theo policy, giữ `acks=all`, dùng delivery timeout 30s/retry hữu hạn; chỉ báo thành công theo callback và `flush()==0`, trả exit khác 0 nếu có lỗi gửi.
- [ ] Kiểm chứng runtime trên topic mới: metadata có 2 partition, mỗi partition có 2 replica và ISR=2; config min ISR=2. Tạo topic test sai policy có tên riêng và xác nhận code từ chối rõ ràng. Rerun không tạo topic mới hoặc tăng partition ngoài ý muốn.

## Task 3: Cập nhật Eventsim và đường dẫn vận hành

**Files:** `scripts/eventsim_start.ps1`, `scripts/eventsim_start.sh`, `scripts/run_pipeline_tests.py`, `scripts/test_recovery_and_batch.py`.

**Interfaces:** Eventsim mặc định nhận `kafka-1:9092,kafka-2:9092,kafka-3:9092`, network `eventsim-net`; cho phép override bootstrap/network qua môi trường. Test scripts đọc `HDFS_BASE_PATH` và `KAFKA_BOOTSTRAP_SERVERS`.

- [ ] Thay kiểm tra duy nhất container `kafka` bằng kiểm tra ba broker và Kafka API readiness, timeout 60s; chọn network tường minh. Hướng dẫn startup dùng `docker compose --env-file .env -f infra/docker-compose.yml ...` từ root.
- [ ] Giữ Eventsim ngoài Compose; kiểm tra container Eventsim đang chạy trước khi thao tác để tránh chạy hai producer đồng thời. Không tự xóa hoặc recreate Eventsim đang chạy để chỉ kiểm tra cấu hình.
- [ ] Thay HDFS path hardcode trong test scripts bằng biến cấu hình, sử dụng namespace test riêng. Khi sửa bài recovery, ghi nhận offset/record test trước và sau restart để chứng minh đọc tiếp từ checkpoint; không chỉ kiểm tra container sống.
- [ ] Kiểm tra cú pháp PowerShell bằng parser và Bash bằng `bash -n` nếu có runtime; chạy thử Eventsim khi image local khả dụng. Repo không chứa assembly JAR/source đầy đủ để tự build Eventsim từ đầu: nếu thiếu image, báo blocker riêng, không tải binary không rõ nguồn.
- [ ] Ghi rõ acks của producer bên trong Eventsim chưa được xác nhận từ repository; bootstrap nhiều broker không chứng minh producer này dùng `acks=all`. Kiểm chứng min ISR dùng producer test có cấu hình tường minh.

## Task 4: Kiểm chứng cluster và recovery

**Files:** Tạo `scripts/test_kafka_cluster.py`; cập nhật test scripts ở Task 3 nếu cần cho namespace riêng.

**Interfaces:** `poetry run python scripts/test_kafka_cluster.py --check` kiểm tra metadata; `--fault-test` chủ động dừng/start một broker mới và luôn khôi phục bằng `finally`. Topic test có prefix `cluster_smoke_` và run ID; không tự xóa topic/volume sau test.

- [ ] Viết bài smoke có timeout/polling hữu hạn, dùng AdminClient/Producer/Consumer hiện có; exit khác 0 khi bất kỳ assertion nào fail. Kiểm tra bootstrap qua từng endpoint từ host và qua INTERNAL từ Docker; đủ 3 broker, 2 partition/topic, RF=2/min ISR=2.
- [ ] Gửi fixture có ID duy nhất vào **cả partition 0 và 1**, kiểm tra callback thành công và consumer đọc đúng ID. Không coi số lần gọi `produce()` là số record đã gửi thành công.
- [ ] Với `--fault-test`, đọc assignment và chọn đúng broker đang giữ replica của partition cần kiểm tra. Dừng broker, đợi ISR xuống dưới 2, xác nhận metadata quorum vẫn có leader và producer `acks=all` không được acknowledge thành công trong delivery deadline.
- [ ] Khôi phục broker trong `finally`, chờ cả hai replica vào ISR; gửi record ID mới và kiểm tra consumer nhận được. Record gửi trong lúc lỗi có kết quả không chắc chắn phải được đối soát riêng, không dùng để tuyên bố exactly-once hoặc mặc định chưa từng append.
- [ ] Restart cluster không xóa volume; đối chiếu cluster ID, topic ID/assignment và record đã acknowledge trước restart. Kiểm tra client vẫn bootstrap được qua hai endpoint còn hoạt động khi endpoint đầu tiên dừng.
- [ ] Chạy fixture hợp lệ qua Spark vào output/checkpoint **test riêng**. Restart Spark với chính checkpoint test đó, kiểm tra offset tiếp tục và không replay record đã commit. Kiểm tra các path cũ không bị dùng làm sink/checkpoint.

## Task 5: Smoke downstream và tài liệu bàn giao

**Files:** `README.md`; các command vận hành dựa trên `spark/batch/hdfs_to_duckdb.py` và dbt/dashboard hiện có, không đổi logic model/UI.

- [ ] Tạo staging local, DuckDB database và dbt profile tạm trong thư mục test riêng; trỏ ingestion tới output HDFS test. Nạp fixture có kết quả định trước vào landing, ghi lại số dòng/khóa và chạy lại ingestion: số dòng/ledger không tăng vì cùng file.
- [ ] Đóng ingestion connection, chạy `dbt build` bằng profile test, không full-refresh warehouse thật. Kiểm tra dashboard với database test sau dbt thành công; đối chiếu KPI theo fixture/định nghĩa hiện có, không tự đặt KPI mới.
- [ ] Ghi lệnh setup/start Kafka trước, Spark sau, Eventsim độc lập; mô tả bootstrap host/container, 2 partition và ordering, RF/min ISR, namespace mới, UUID ổn định, restart/recovery. Lệnh dừng không có `-v` và không xóa dữ liệu.
- [ ] Ghi hướng dẫn rollback bằng Compose cũ lưu trong lịch sử Git và volume cũ đã ghi nhận; dừng cluster mới trước khi dùng lại port cũ. Không tự rollback hoặc xóa volume mới.
- [ ] Chạy lại Compose validation, kiểm tra diff/whitespace và danh sách file thay đổi. Báo riêng các bước đã chạy/pass/fail/blocked; runtime bị chặn thì không kết luận cluster đã chạy thành công.

## Tiêu chí nghiệm thu

- [ ] Chỉ còn một Compose chuẩn tại `infra/`; các lệnh/script liên quan không phụ thuộc Compose ở `kafka/`.
- [ ] Ba node cùng cluster, 3 controller voter; volume mới bền vững sau restart, không dùng volume Kafka cũ.
- [ ] Bốn topic nghiệp vụ và topic test mới có **2 partition/topic**, RF=2/min ISR=2; các default và tạo topic tường minh nhất quán.
- [ ] Host và Docker client truy cập được mọi broker qua metadata được advertise đúng mạng.
- [ ] Bài mất replica cho kết quả ghi bị từ chối/timeout như policy; phục hồi ISR và ghi/đọc thành công sau restart broker.
- [ ] Spark dùng output/checkpoint mới và resume đúng checkpoint đó; downstream test đọc được dữ liệu mới, ingestion rerun không duplicate.
- [ ] README tái lập được thao tác local bằng PowerShell; dữ liệu và các thay đổi người dùng có sẵn được giữ nguyên.
- [ ] Kết thúc trong working tree để review; chưa stage, commit, push, PR hoặc deploy.

## Kiểm tra đã thực hiện khi lập plan và giới hạn

- Đã đọc hai Compose, Spark streaming/ingestion, producer, script Eventsim, cấu hình mẫu, README và trạng thái Git.
- Cả hai Compose hiện tại đã qua `docker compose config --quiet` ở bước khảo sát. Đây là validation cấu hình cũ, không phải kiểm chứng cluster đề xuất.
- Docker CLI có sẵn nhưng chưa kết nối được Docker daemon; chưa có runtime test, benchmark hay xác nhận đủ RAM/CPU.
- Ba container trên một máy không chịu được mất cả máy. RF=2/min ISR=2 không bảo đảm tiếp tục ghi mọi partition khi một broker dừng.
- Các vấn đề chất lượng dữ liệu có sẵn ngoài thay đổi này, như chính sách quarantine, không được coi là đã sửa chỉ vì Kafka cluster hoạt động. Nếu regression test phát hiện lỗi cũ, báo riêng và không gán PASS giả.

## Tài liệu chính thức đã đối chiếu

- [Kafka 4.3 KRaft: combined mode, static/dynamic quorum, cluster storage](https://kafka.apache.org/43/operations/kraft/).
- [Kafka 4.3 broker configs: replication defaults, min ISR, internal topics](https://kafka.apache.org/43/configuration/broker-configs/).
- [Docker image usage guide, tag 4.3.1](https://github.com/apache/kafka/blob/4.3.1/docker/examples/README.md).
- [Ví dụ cluster combined plaintext, tag 4.3.1](https://github.com/apache/kafka/blob/4.3.1/docker/examples/docker-compose-files/cluster/combined/plaintext/docker-compose.yml).

## Bước tiếp theo

Người dùng review plan này, đặc biệt topology/host port và namespace output/checkpoint mới. Chỉ bắt đầu sửa code/cấu hình sau khi được duyệt; đề xuất triển khai trực tiếp trong session theo từng task.
