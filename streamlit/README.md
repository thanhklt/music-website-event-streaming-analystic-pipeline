# Dashboard nghe nhạc — Streamlit

Dashboard local đọc `dashboard.song_plays` và `dashboard.filter_options` từ DuckDB.
Không đọc landing/staging/intermediate, không quét lại marts trực tiếp tại runtime,
không ghi warehouse và không tự chạy ingestion/dbt.

## Chạy từ thư mục repository (PowerShell)

```powershell
poetry install --no-root
poetry run streamlit run .\streamlit\app.py --server.address 127.0.0.1 --server.port 8501
```

Mở <http://localhost:8501>. Dừng bằng `Ctrl+C`; không xóa dữ liệu.
Streamlit, pandas và Altair đã được khai báo trong Poetry; phiên bản đã resolve
nằm trong `poetry.lock`. Không chạy `python streamlit/app.py` vì app cần runtime
Streamlit.

Mặc định database là `infra/duckdb/data/warehouse.duckdb`, tính từ root repository
kể cả khi launch từ thư mục khác. Có thể thay đường dẫn trong sidebar hoặc dùng:

```powershell
$env:DASHBOARD_DUCKDB_PATH = 'D:\duong-dan\warehouse.duckdb'
poetry run streamlit run .\streamlit\app.py --server.address 127.0.0.1 --server.port 8501
```

Biến môi trường được đọc từ process; app không tự load file `.env`.
Database phải đã có schema `dashboard` (`dashboard.song_plays` và `dashboard.filter_options`).
Không có database hoặc thiếu bảng dashboard thì app báo lỗi, không tạo database rỗng.

## Báo cáo

- Top 5 **tiểu bang** theo lượt nghe (thay cho quốc gia theo yêu cầu).
- Top 5 bài hát; phân biệt bằng `song_key`, hiển thị cả nghệ sĩ.
- Heatmap 7 thứ × 24 giờ UTC: đổi giữa giờ nghe ước tính và lượt nghe,
  cộng gộp các tuần trong khoảng ngày đã chọn.
- Một bài được nghe nhiều nhất cho mỗi giới tính, gồm Unknown nếu có dữ liệu.
- Tổng lượt nghe, người nghe, bài hát, giờ nghe ước tính; xu hướng theo ngày,
  top nghệ sĩ, khung giờ cao điểm và tỷ trọng bài dẫn đầu.
- Bộ lọc ngày UTC (bao gồm cả ngày kết thúc), tiểu bang, giới tính; trạng thái
  không có dữ liệu; thời điểm cập nhật fact và event mới nhất.

Xu hướng ngày hiển thị cả ngày 0 lượt trong khoảng đã chọn, không nối tắt
qua các ngày không có event.

## Định nghĩa và giới hạn

Một dòng fact = một lượt nghe. Bảng `dashboard.song_plays` được materialize ở grain
một dòng mỗi `play_key`, đã pre-join với các dimension để reader không phải join lại.
Người nghe đếm DISTINCT `user_id` (không đếm NULL/-1), nên nhiều phiên bản SCD2
không nhân số người nghe. Giới tính lấy theo phiên bản SCD2 tại thời điểm nghe.
Khóa `play_key` NULL hoặc trùng trên bảng dashboard làm app báo lỗi ngay.
Ranking đồng hạng chọn tên bài/nghệ sĩ/key theo thứ tự ổn định.

`listened_seconds` giữ nguyên định nghĩa của warehouse: khoảng cách đến event tiếp
theo trong phiên. Có thể gồm khoảng nghỉ và lớn hơn độ dài bài; không phải
thời gian nghe thực được đo hay tỷ lệ hoàn thành bài hát. NULL không được
suy đoán thời lượng, có tỷ lệ thiếu riêng; tổng chỉ cộng các giá trị đã biết.
Heatmap gán toàn bộ thời lượng vào **giờ bắt đầu** lượt nghe, không chia duration
qua giờ/ngày. Ô 0 có thể không có lượt hoặc thiếu duration; tooltip có số lượt
và số lượt có duration để phân biệt. Không tính trung bình theo số tuần.

Top tiểu bang loại nhóm Unknown; top bài/nghệ sĩ loại song_key `'-1'`.
Tổng lượt nghe và heatmap vẫn gồm các nhóm này. Lượt thiếu `play_started_at`
không thể lọc theo ngày nên không tham gia báo cáo; số bị loại được hiển thị.
Số nhóm dưới 5 được hiển thị đúng số có dữ liệu, không thêm dữ liệu giả.

Bảng `dashboard.song_plays` là bảng materialized (không phải dynamic view), nên
**không tự động cập nhật khi bảng marts thay đổi**. Cần build lại tầng dashboard để
phản ánh dữ liệu mới. Trường `updated_time` là audit timestamp từ fact, không phải
thời gian dbt build bảng dashboard.

## Làm mới và tránh xung đột writer

Thứ tự vận hành chuẩn:
1. Ingestion và upstream marts hoàn tất hợp lệ.
2. Build tầng dashboard bằng dbt (chỉ dựng tầng dashboard trên marts đã có):
   ```powershell
   poetry run dbt build --project-dir .\dbt\duckdb_warehouse --select path:models/dashboard
   ```
   (Nếu cần build cả upstream marts dẫn đến dashboard, dùng: `poetry run dbt build --project-dir .\dbt\duckdb_warehouse --select +path:models/dashboard`).
3. Xác nhận build và tests thành công; đóng tiến trình ghi (writer).
4. Bấm **Làm mới dữ liệu** trên Streamlit dashboard. Không refresh khi build bị lỗi.

Mỗi lần tải mở kết nối `read_only=True`, đọc trong transaction và luôn đóng
kết nối khi xong, cả khi lỗi. Chỉ cache kết quả tổng hợp, không cache kết nối;
TTL 60 giây, nút làm mới bỏ cache. Không tự polling khi writer đang chạy.
Chưa có cơ chế snapshot nguyên tử đa bảng; các truy vấn chạy tuần tự trong read transaction ngắn.

DuckDB có thể từ chối tải nếu ingestion/dbt/DBeaver đang giữ kết nối ghi từ
process khác. Chờ writer đóng, rồi làm mới; không xóa WAL, checkpoint hoặc
database để xử lý. Nếu cần dashboard hoạt động liên tục trong lúc pipeline
ghi, hãy dùng một snapshot riêng đã được xuất bản sau build thành công và
đặt `DASHBOARD_DUCKDB_PATH` vào snapshot đó. Phiên bản này chưa tự xuất snapshot.

Sau restart, chạy lại lệnh Streamlit và làm mới sau build thành công.
Dashboard bind localhost trong lệnh trên, không publish lên internet.

## Kiểm chứng

```powershell
poetry run python -m unittest discover -s .\streamlit\tests -v
```

Tests dùng DuckDB tạm: đếm lượt/unique user qua SCD2, bài trùng tên khác nghệ sĩ,
duration NULL/0, Unknown, 168 ô heatmap, filter ngày/bang/giới tính và phạm vi
rỗng. Streamlit AppTest chạy UI, lọc và reload sau cập nhật fixture. Test không
mở/ghi warehouse thật và kiểm tra kết nối đã đóng để writer tiếp theo dùng được.

API tham khảo: [Streamlit AppTest](https://docs.streamlit.io/develop/api-reference/app-testing/st.testing.v1.apptest),
[cache_data](https://docs.streamlit.io/develop/api-reference/caching-and-state/st.cache_data),
[DuckDB concurrency](https://duckdb.org/docs/current/connect/concurrency).
