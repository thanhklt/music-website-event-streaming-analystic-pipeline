# Hướng dẫn agent — Data pipeline

## Mục tiêu và phạm vi

Xây dựng pipeline có thể chạy và tái lập trong môi trường local:

```text
Source → Kafka → Spark Structured Streaming → HDFS
                                              ↓
                                  Ingestion → DuckDB raw
                                              ↓
                              dbt → staging → marts
                                              ↓
                                          Dashboard
```

HDFS lưu data lake; DuckDB là nơi phục vụ truy vấn phân tích; dbt thực hiện transformation trong DuckDB. Không thay đổi kiến trúc hoặc bổ sung cloud service khi chưa có yêu cầu. Source, schema và công nghệ dashboard chưa được chỉ định: đọc repository trước, ghi rõ giả định, dùng dữ liệu mẫu nhỏ khi cần; không tự suy đoán KPI nghiệp vụ.

## Quy tắc Git bắt buộc

- **Không tự commit và không tự push code lên remote repository.**
- Chỉ commit khi người dùng yêu cầu rõ ràng việc commit. Chỉ push khi người dùng yêu cầu rõ ràng việc push. Quyền commit không bao gồm quyền push.
- Những yêu cầu như “hoàn thành”, “sửa lỗi”, “build pipeline” hoặc “chạy test” không phải quyền commit/push.
- Không dùng script, hook, IDE, API hay công cụ khác để thực hiện commit/push thay cho lệnh Git.
- Không tạo PR, merge, publish hoặc deploy lên remote nếu chưa được yêu cầu rõ ràng.
- Không tự stage thay đổi. Có thể dùng `git status`, `git diff`, `git log` để kiểm tra.
- Không chạy `git reset --hard`, `git clean -fd`, force push hoặc ghi đè thay đổi của người dùng. Không sửa cấu hình remote hoặc thông tin tác giả Git.
- Kết thúc công việc với thay đổi trong working tree và báo cáo để người dùng review.

## Cách làm việc

1. Đọc README, các AGENTS.md liên quan, cấu hình và code hiện có. Kiểm tra trạng thái Git trước khi sửa; giữ nguyên thay đổi không thuộc nhiệm vụ.
2. Với công việc lớn, lập kế hoạch ngắn theo từng thành phần và tiêu chí kiểm chứng. Thực hiện từng bước đến khi yêu cầu hoàn tất.
3. Ưu tiên một luồng end-to-end nhỏ chạy được trước, sau đó thêm xử lý lỗi và tối ưu.
4. Tự thực hiện chỉnh sửa local có thể đảo ngược và test phù hợp. Chỉ hỏi khi thiếu thông tin ảnh hưởng trực tiếp đến nghiệp vụ hoặc cần thao tác phá hủy dữ liệu.
5. Kiểm tra tài liệu chính thức khi API hoặc khả năng tích hợp chưa chắc chắn. Không viết cấu hình theo suy đoán; pin phiên bản tương thích của Spark, Kafka connector, Hadoop, Java, DuckDB, dbt-core và dbt-duckdb.
6. Không báo “đã chạy thành công” khi chỉ đọc code. Nêu rõ bước đã chạy, kết quả và bước bị chặn bởi môi trường.

## Cấu trúc đề xuất

Giữ cấu trúc hiện có nếu phù hợp. Với repository mới, có thể dùng:

```text
infra/                  # Docker Compose và cấu hình Kafka/HDFS
src/producer/           # Đọc source, phát event
src/streaming/          # Spark Structured Streaming
src/ingestion/          # Chuyển dữ liệu HDFS vào DuckDB
dbt/                    # dbt_project.yml, models, tests, macros
dashboard/              # Ứng dụng dashboard
tests/                  # Unit/integration tests và fixtures
scripts/                # Bootstrap, smoke test và vận hành
docs/                   # Kiến trúc, data contract, runbook
.env.example            # Biến cấu hình mẫu, không chứa secret
README.md               # Hướng dẫn chạy từ đầu
```

## Source → Kafka

- Xác định data contract: tên trường, kiểu dữ liệu, nullability, business key, event ID, event time và timezone. Dùng UTC trong pipeline.
- Phân biệt event time với ingestion time. Nếu source không có event ID, xác định khóa dedup có ý nghĩa nghiệp vụ và ghi rõ giới hạn.
- Producer phải có cấu hình bootstrap servers/topic, retry có giới hạn và xử lý lỗi gửi. Chọn message key theo nhu cầu ordering/phân phối dữ liệu.
- Với môi trường local mới, ưu tiên Kafka KRaft; số broker, partitions và replication factor phải phù hợp tài nguyên thực tế. Không mô tả một broker là có high availability.
- Dùng cơ chế producer idempotence khi phù hợp; không suy ra toàn pipeline là exactly-once chỉ từ cấu hình producer.

## Kafka → Spark Structured Streaming → HDFS

- Parse payload bằng schema tường minh. Giữ metadata cần truy vết như topic, partition, offset, event time và ingestion time.
- Đưa payload lỗi hoặc vi phạm contract vào vùng quarantine kèm lý do; không âm thầm bỏ dữ liệu.
- Dùng Parquet cho dữ liệu hợp lệ trên HDFS, trừ khi yêu cầu khác. Chọn partition theo nhu cầu truy vấn, ví dụ ngày sự kiện; tránh partition có cardinality cao.
- Mỗi streaming query có checkpoint path riêng, bền vững, tách khỏi output path. Không xóa checkpoint để chữa lỗi nếu chưa giải thích ảnh hưởng và được cho phép.
- `startingOffsets` chỉ áp dụng khi query chưa có checkpoint hợp lệ. Khôi phục bằng checkpoint hiện có.
- Trigger là lịch kích hoạt xử lý; không phải cửa sổ event time. Cấu hình `maxOffsetsPerTrigger` theo khả năng xử lý và theo dõi lag.
- Watermark chỉ dùng khi cần stateful operation; nêu rõ chính sách late events và thời hạn giữ state. Không xem watermark là cách dedup vĩnh viễn.
- Hạn chế small files; tách compaction thành bước có thể kiểm chứng nếu cần. Không dùng `collect()` hoặc `toPandas()` trên toàn bộ dữ liệu lớn.
- Nếu dùng `foreachBatch`, xử lý retry/idempotency rõ ràng. Chỉ tuyên bố delivery guarantee sau khi xét source, sink, checkpoint và thử nghiệm recovery.

## HDFS

- Dùng persistent volumes cho metadata NameNode và dữ liệu DataNode. Không format NameNode mỗi lần khởi động.
- Với local một DataNode, cấu hình replication phù hợp và ghi rõ không chịu được mất node. Không giả định nhiều container trên một máy tạo khả năng chịu lỗi cấp máy.
- Tách đường dẫn dữ liệu hợp lệ, quarantine và checkpoints. Document quyền truy cập và retention.
- Không tự xóa volume, output hoặc format filesystem. Các thao tác như `docker compose down -v` cần yêu cầu rõ ràng khi ảnh hưởng dữ liệu đang có.

## HDFS → DuckDB ingestion

- **Không giả định DuckDB/dbt-duckdb đọc trực tiếp được `hdfs://`.** Kiểm chứng connector theo phiên bản trước khi triển khai.
- Nếu chưa có tích hợp phù hợp, dùng Hadoop client hoặc WebHDFS để tải/copy các Parquet đã hoàn tất sang staging local rồi nạp DuckDB. HDFS vẫn là nguồn dữ liệu lake.
- Chỉ đọc file/batch đã được xác nhận hoàn tất bằng cơ chế commit/manifest phù hợp; không đọc file đang ghi hay coi toàn bộ thư mục là snapshot nguyên tử.
- Có ingestion ledger/manifest: định danh file hoặc batch, trạng thái và thời điểm xử lý. Việc nạp dữ liệu và cập nhật ledger cần transaction khi có thể.
- Rerun cùng file/batch không được nhân bản dữ liệu. Không chỉ dùng `MAX(event_time)` vì có thể bỏ sót late events.
- DuckDB database file đặt trên local persistent storage; không đặt file database đang ghi trên HDFS. Thiết kế một writer được điều phối; không cho ingestion và dbt cùng ghi từ các process độc lập.
- Dashboard phải có chiến lược truy cập tương thích với writer: đóng kết nối khi refresh hoặc dùng snapshot chỉ đọc được xuất bản sau khi dbt thành công. Không giả định nhiều process đọc/ghi cùng database file luôn an toàn.

## Transformation bằng dbt

- Dùng dbt-duckdb. Khai báo raw tables bằng `source()`, liên kết model bằng `ref()`; không hardcode đường dẫn hoặc credential trong SQL.
- Các lớp: `raw` giữ dữ liệu đã nạp; `staging` chuẩn hóa kiểu, tên và chất lượng; `intermediate` dùng khi cần logic tái sử dụng; `marts` chứa facts/dimensions hoặc bảng KPI cho dashboard.
- Spark tập trung parse, validate và chuẩn bị dữ liệu để lưu; logic phân tích nghiệp vụ đặt trong dbt.
- Xác định grain, business key và quan hệ trước khi tạo fact/dimension. Document đơn vị, tiền tệ, timezone và cách xử lý null.
- Chọn materialization theo kích thước và cách sử dụng. Với incremental, xác định `unique_key`, update/delete policy và cơ chế xử lý late/update events.
- Chỉ dùng incremental strategy được adapter và phiên bản hiện tại hỗ trợ. Lookback window phải có căn cứ; dedup trong model và kiểm chứng rerun.
- Thêm tests phù hợp: not_null, unique hoặc composite uniqueness, relationships, accepted_values và kiểm tra nghiệp vụ thực tế. Document models/columns trong YAML.
- Không tự chạy full refresh trên dữ liệu hiện có khi chưa đánh giá tác động. Dùng dữ liệu test hoặc database tách biệt để kiểm chứng.

## Dashboard

- Chọn công nghệ theo repository hoặc yêu cầu; với project mới chưa có lựa chọn, có thể dùng Streamlit làm giả định local và ghi rõ trong README.
- Chỉ truy vấn marts hoặc snapshot đã kiểm chứng, tránh tính lại logic nghiệp vụ riêng trong UI.
- Hiển thị thời điểm dữ liệu cập nhật, bộ lọc, đơn vị KPI và trạng thái không có dữ liệu. Refresh sau khi ingestion và dbt hoàn tất thành công.
- Dashboard local không đồng nghĩa với việc được phép publish ra internet.

## Cấu hình, vận hành và bảo mật

- Tách code khỏi cấu hình; dùng biến môi trường và `.env.example`. Không ghi secret, payload nhạy cảm hoặc token vào log.
- Loại `.env`, credentials, DuckDB files, dữ liệu lớn, checkpoints và build artifacts khỏi Git bằng `.gitignore` phù hợp.
- Health check và retry có giới hạn; không chỉ dựa vào thứ tự container khởi động. Document hostname/port trong Docker và từ host.
- Theo dõi tối thiểu: producer errors, Kafka lag, streaming progress, quarantine count, ingestion failures, dbt test results và thời điểm marts cập nhật.
- Chỉ orchestration ingestion/dbt/refresh dashboard khi thứ tự phụ thuộc rõ ràng; không chạy dbt cho từng event.
- README phải có lệnh setup, start, stop không xóa dữ liệu, chạy sample, kiểm tra kết quả và phục hồi sau restart. Khi Windows có liên quan, cung cấp lệnh PowerShell đúng cú pháp.

## Kiểm chứng và tiêu chí hoàn thành

- Test logic parse/validation và transformation có rủi ro; dùng fixtures nhỏ, xác định trước kết quả kỳ vọng.
- Chạy smoke test end-to-end: phát sample → consume Kafka → thấy Parquet hợp lệ trên HDFS → nạp raw DuckDB → chạy `dbt build` → đối chiếu KPI dashboard.
- Kiểm chứng ít nhất: payload lỗi vào quarantine; restart streaming với checkpoint; chạy lại ingestion không duplicate; late/update event được xử lý theo policy; dashboard đọc dữ liệu sau refresh.
- Đối chiếu số dòng và khóa qua từng lớp, giải thích chênh lệch do quarantine, dedup hoặc aggregation. Không yêu cầu số dòng marts bằng raw khi grain khác nhau.
- Dùng môi trường/đường dẫn/database test riêng; không xóa dữ liệu người dùng để chạy test.
- Hoàn thành khi có code và cấu hình tái lập, data contract, hướng dẫn vận hành, kiểm chứng phù hợp và danh sách giới hạn còn lại.

## Báo cáo cuối mỗi nhiệm vụ

Trả lời bằng tiếng Việt, ngắn gọn: thay đổi gì, lý do, các bước đã kiểm chứng và kết quả, hạn chế hoặc blockers. Liệt kê file quan trọng để review. Nêu rõ **chưa commit và chưa push**. Không yêu cầu commit/push như một bước bắt buộc để hoàn tất nhiệm vụ.