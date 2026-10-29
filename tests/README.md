# Đối chiếu chất lượng dữ liệu HDFS → DuckDB

Chạy từ thư mục gốc dự án. Test dùng `hive_partitioning=False`, đối chiếu các
cột vật lý trong Parquet theo đúng phạm vi ingestion. `EXCEPT ALL` hai chiều
so toàn bộ giá trị và số lần xuất hiện của mỗi bản ghi. Bản ghi trùng hợp lệ,
`NULL` và timestamp đều được tính; không lấy mẫu hay dùng hash.

## Kiểm chứng tự động bằng dữ liệu tạm

```powershell
python -m unittest discover -s tests -p test_hdfs_duckdb_quality.py -v
```

Bao gồm trường hợp mất một bản ghi trùng; mất dòng và thêm dòng khác nhưng
tổng số dòng vẫn bằng nhau; thay đổi giá trị hoặc kiểu cột; không suy thêm cột
từ đường dẫn Hive; vẫn so cột ngày nếu nằm trong Parquet; mất nguyên file nguồn;
không tải được file nguồn; và chạy ingestion
hai lần không nạp lại dữ liệu. HTTP được mock, Parquet và DuckDB là dữ liệu thật
trong thư mục tạm. Các test này không xác nhận warehouse đang sử dụng đã đầy đủ.

## Đối chiếu HDFS và warehouse thật

```powershell
python tests/test_hdfs_duckdb_quality.py --live --topic listen_events
```

Mỗi lần kiểm tra một topic. Địa chỉ HDFS và đường dẫn warehouse lấy từ
`config/config.yml` qua `read_config()`. Có thể thay bằng `auth_events`,
`page_view_events` hoặc `status_change_events`.

Điều kiện chạy: HDFS truy cập được; ingestion đã bắt kịp nguồn; không có tiến
trình khác giữ khóa ghi warehouse. Đóng kết nối giữ khóa trước khi chạy.
Nếu có batch mới hoặc metadata log thay đổi trong lúc kiểm tra, kết quả bị
coi là chưa kết luận được và lệnh thất bại để người dùng chạy lại.

Nguồn đối chiếu là tập file đã commit trong `_spark_metadata`: đọc snapshot
`.compact` mới nhất và các batch tiếp theo, loại đường dẫn lặp. Không dùng
danh sách file mà `read_from_hdfs()` trả về để làm chuẩn, vì helper đó có thể
bỏ qua file khi tải lỗi. Bất kỳ lỗi đọc log/file nào đều làm audit thất bại.

Audit mở warehouse với `read_only=True`, trong một transaction. File Parquet
được tải về thư mục tạm rồi xóa; chỉ tạo SQL view tạm, không ghi vào warehouse,
không chạy ingestion và không thay đổi dịch vụ Docker.

JSON kết quả gồm:

- `hdfs_rows`, `duckdb_rows`: tổng số bản ghi ở hai bên.
- `missing_or_changed_rows`: bản ghi nguồn thiếu ở đích hoặc có giá trị khác.
- `extra_or_changed_rows`: bản ghi đích dư so với nguồn hoặc có giá trị khác.
- `schema_differences`: cột thiếu, thừa hoặc khác kiểu.
- `files_missing_from_ledger`: file nguồn chưa có trạng thái `SUCCESS` trong ledger.
- `ledger_files_absent_from_source`: ledger chứa file ngoài tập nguồn hiện tại.
- `passed`: chỉ đúng khi tất cả phép đối chiếu đều khớp. Exit code `0` khi đạt,
  `1` khi không khớp hoặc audit không thực hiện được.

Test không suy ra `year/month/day` từ thư mục Hive vì ingestion đọc bằng
`hive_partitioning=False`. Nếu các cột này thực sự nằm trong nội dung Parquet,
chúng vẫn phải khớp đầy đủ ở DuckDB. Các cột thừa trong schema đích vẫn được
báo khác biệt.

Kết quả chỉ xác nhận tập dữ liệu ở snapshot kiểm tra. Nếu retention đã loại
metadata nguồn cũ trong khi DuckDB còn giữ lịch sử đó, hai bên khác phạm vi và
audit toàn bảng có thể báo dòng dư. Audit không kiểm chứng dữ liệu Kafka trước
khi Spark ghi HDFS, cũng không bảo đảm các lần ingestion tương lai không lỗi.
