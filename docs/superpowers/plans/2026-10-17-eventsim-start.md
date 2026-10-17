# Kế hoạch tổ chức và khởi động Eventsim

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans để thực hiện từng bước sau khi người dùng duyệt. Không triển khai trước khi duyệt plan.

**Goal:** Đưa hai script khởi động về `eventsim/`, build một Docker image và chạy trực tiếp với config hiện có, sau đó xóa `scripts/`.

**Architecture:** Eventsim chạy độc lập với Compose của hạ tầng. Script PowerShell và Bash có hành vi tương đương: kiểm tra điều kiện chạy, build image, tính khoảng thời gian UTC và gọi `docker run` trên network Kafka hiện có.

**Tech Stack:** PowerShell, Bash, Docker CLI, Dockerfile và JAR Eventsim hiện có.

## Điều chỉnh và kết quả thực hiện đã thống nhất

- Người dùng đã duyệt triển khai và giới hạn kiểm chứng ở CLI; không chạy mock, syntax test, build image hay batch thật trong phiên triển khai này.
- CLI help của đúng JAR trong repository liệt kê `--nocontinuous` với mô tả `run all at once`.
- Truyền trực tiếp `--no-continuous` trả exit code 1: `[scallop] Error: Unknown option 'no-continuous'`.
- Sau khi nhận kết quả, người dùng đã đồng ý dùng `--nocontinuous`; quyết định này thay thế yêu cầu cờ ban đầu ở các bước dưới.
- Đã chuyển và sửa hai launcher, bổ sung config vào Dockerfile, sửa entrypoint để giữ đối số, cập nhật README và xóa thư mục `scripts/` còn lại.
- Đã đọc lại diff và tìm tham chiếu hoạt động tới đường dẫn cũ. Không thay nội dung config của người dùng, không tạo Compose và không phát dữ liệu.
- Các bước kiểm chứng rộng hơn trong checklist gốc dưới đây được bỏ theo yêu cầu mới, không được coi là đã chạy hoặc đã pass. Chưa stage, commit hoặc push.

**Spec:** Yêu cầu trong hội thoại: mặc định 00:00 hôm nay đến 00:00 ngày mai, theo UTC đã trao đổi; dùng `eventsim/config/config.json`, `--no-continuous`, 20.000 users; không tạo Compose cho Eventsim.

## Ràng buộc chung

- Chỉ viết plan ở giai đoạn này; chờ người dùng duyệt trước khi chỉnh sửa triển khai.
- Không stage, commit, push, publish hoặc deploy.
- Giữ nguyên thay đổi của người dùng trong Compose, config và các file khác.
- Không xóa volume, checkpoint, output hay container đang có.
- Không tạo Docker Compose cho Eventsim; không thay đổi kiến trúc pipeline.
- Không tự thay cờ `--no-continuous` bằng `--nocontinuous` nếu JAR không hỗ trợ; báo rõ bằng chứng và điểm bị chặn.

## File dự kiến thay đổi

| File | Thay đổi |
| --- | --- |
| `eventsim/eventsim_start.ps1` | Chuyển từ `scripts/`, sửa để build image và chạy container |
| `eventsim/eventsim_start.sh` | Chuyển từ `scripts/`, giữ hành vi tương đương bản PowerShell |
| `eventsim/Dockerfile` | Copy thêm `config/` vào `/opt/eventsim/config/` |
| `eventsim/eventsim.sh` | Dùng `exec java ... "$@"` để giữ nguyên đối số và chuyển signal đúng |
| `eventsim/README.md` | Hướng dẫn build/run, cấu hình mới, UTC, logs và stop |
| `README.md` | Cập nhật đường dẫn khởi động; sửa hoặc bỏ hướng dẫn trỏ tới `scripts/` đã bị xóa |
| `scripts/` | Xóa sau khi đã chuyển hai launcher và kiểm tra toàn bộ nội dung còn lại |

## Các tình huống cần kiểm tra

- Chạy script từ thư mục bất kỳ, kể cả đường dẫn có dấu cách: dùng đường dẫn dựa trên vị trí script.
- Khoảng thời gian đi qua cuối tháng/năm: tính ngày theo UTC, end luôn lớn hơn start.
- Docker lỗi, Kafka chưa healthy, thiếu network/config/JAR hoặc build lỗi: dừng rõ ràng, không gọi run tiếp.
- Container cùng tên đã tồn tại: giữ nguyên và báo cách xử lý, không tự xóa/recreate.
- Cờ CLI không được JAR hỗ trợ hoặc runtime thoát lỗi: không báo khởi động thành công chỉ dựa vào container ID.

## Task 1: Chuẩn bị image và xác nhận CLI

- [ ] Đọc lại Git status và các file ngay trước khi sửa để giữ thay đổi mới của người dùng.
- [ ] Kiểm tra JSON hiện có hợp lệ, xác nhận config và JAR tồn tại; không sửa nội dung config nghiệp vụ.
- [ ] Thêm `COPY config /opt/eventsim/config` vào Dockerfile; sửa entrypoint shell dùng `exec` và `"$@"`.
- [ ] Build image mặc định `events:1.0` bằng build context `eventsim/`.
- [ ] Đọc CLI help của image/JAR, xác nhận cách truyền `--nusers 20000`, thời gian và đặc biệt `--no-continuous`. Nếu không hỗ trợ cờ yêu cầu, báo blocker trước khi quyết định sửa JAR hay thay flag.

## Task 2: Chuyển và sửa hai launcher

- [ ] Chuyển `scripts/eventsim_start.ps1` và `.sh` sang `eventsim/`.
- [ ] Giữ các biến override `EVENTSIM_IMAGE`, `EVENTSIM_CONTAINER`, `EVENTSIM_KAFKA_BOOTSTRAP_SERVERS`, `EVENTSIM_KAFKA_NETWORK`; mặc định lần lượt `events:1.0`, `eventsim-v2`, ba broker `kafka-1:9092,kafka-2:9092,kafka-3:9092`, `eventsim-net`.
- [ ] Thêm `EVENTSIM_START_TIME` và `EVENTSIM_END_TIME` để ghi đè thời gian UTC; khi không có override, tính cả hai từ một lần lấy ngày UTC hiện tại, định dạng `yyyy-MM-ddTHH:mm:ss`. Kiểm tra end > start.
- [ ] Kiểm tra Docker, config/JAR, container trùng tên, network và health của ba broker với deadline 60 giây trước khi phát dữ liệu.
- [ ] Mỗi lần gọi script đều chạy `docker build` (được dùng cache), kiểm tra exit code, rồi `docker run -d` với config `config/config.json`, `--nusers 20000`, hai mốc thời gian và `--no-continuous`.
- [ ] Bỏ các tham số hardcode của script cũ không có trong yêu cầu README: 100 users, ngày 2026-09-01, growth rate, userid và randomseed.
- [ ] Giữ volume output hiện có tại `data/` ở root repository, không chuyển hoặc xóa dữ liệu. Kiểm tra trạng thái/log ban đầu và in lệnh theo dõi logs; phân biệt đã tạo container với đã hoàn thành sinh dữ liệu.

## Task 3: Dọn cấu trúc, tài liệu và kiểm chứng

- [ ] Rà lại nội dung `scripts/` tại thời điểm thực thi. Hiện chỉ còn hai launcher và `__pycache__`; không khôi phục các file người dùng đã xóa. Nếu xuất hiện file mới có giá trị, xác định nơi giữ trước khi xóa thư mục.
- [ ] Cập nhật hai README: lệnh PowerShell/Bash từ root hoặc `eventsim/`, mặc định UTC, config path, 20.000 users, flag, build context, override, logs/stop và cách xử lý container đã tồn tại.
- [ ] Tìm mọi tham chiếu hoạt động tới `scripts/` trong code/tài liệu; sửa theo vị trí thực tế hoặc bỏ hướng dẫn đã lỗi thời. Không bịa đường dẫn test thay thế khi file không còn tồn tại.
- [ ] Xác minh đường dẫn tuyệt đối của thư mục cần xóa nằm đúng trong workspace, rồi xóa `scripts/` bằng PowerShell native sau khi đã chuyển launcher.
- [ ] Parse PowerShell bằng parser, kiểm tra Bash bằng `bash -n`; nếu môi trường thiếu Bash, ghi rõ giới hạn.
- [ ] Dùng Docker mock/stub để kiểm tra đối số của cả hai launcher: config đúng, 20.000 users, flag đúng, khoảng UTC, chạy từ thư mục khác, build thất bại, broker timeout, container trùng tên và runtime lỗi. Không phát batch 20.000 users chỉ để kiểm tra script.
- [ ] Build image thật và chạy help để kiểm chứng CLI, không tự phát dữ liệu vào Kafka đang dùng. Việc chạy batch thật được hướng dẫn trong README.
- [ ] Chạy `git diff --check`, review diff và tìm tham chiếu `scripts/` còn sót; xác nhận không tạo Compose và không đổi config của người dùng.
- [ ] Báo cáo bằng tiếng Việt: file thay đổi, các kiểm tra thực sự đã chạy, giới hạn/blocker và **chưa commit, chưa push**.
