Welcome to your new dbt project!

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices

### Khóa của lớp intermediate

Các model intermediate materialize thành bảng và dựng lại từ staging mỗi lần chạy.
Khóa hash dùng `dbt_utils.generate_surrogate_key` (package `dbt-labs/dbt_utils` 1.4.1):

| Model | Khóa | Business key / grain |
|---|---|---|
| `int_dim_user` | `user_key` | `user_id`, `start_time` của phiên bản SCD2 |
| `int_dim_session` | `session_key` | `user_id`, `session_id` |
| `int_dim_song` | `song_key` | `song_artist_name`, `song_name`, `song_duration_seconds` |
| `int_dim_location` | `location_key` | `location_city`, `location_zip`, `location_state`, `location_lon`, `location_lat` |
| `int_fact_song_play` | `play_key` | `user_id`, `session_id`, `item_in_session` |
| `int_dim_date` | `date_key` | Ngày UTC, kiểu `DATE`, không hash |
| `int_dim_time` | `time_key` | Giờ UTC đến giây, kiểu `TIME`, không hash |

Cùng business key với cùng kiểu dữ liệu và thứ tự trường tạo cùng khóa hash qua rerun
và rebuild. Macro phân biệt `NULL` với chuỗi rỗng theo cấu hình mặc định;
các test `not_null` của business key vẫn được giữ. Không còn model intermediate nào
gọi `ensure_sequence`; macro cũ được giữ lại nhưng không tham gia cấp khóa.
`unique_key` trong config dbt không tự tạo constraint `PRIMARY KEY` của DuckDB.

Business key thay đổi sẽ đổi hash. Với SCD2, late event làm thay đổi `start_time`
của phiên bản có thể đổi `user_key`; fact được dựng lại để lookup phiên bản mới.
Việc đổi kiểu/định dạng trường hoặc thứ tự trường hash cũng có thể đổi khóa.
Hash có xác suất collision; test `unique` kiểm tra trên dữ liệu đã build.

Chuyển từ sequence sang hash đổi kiểu khóa từ số sang chuỗi. Khi áp dụng vào warehouse,
cần build đồng bộ các dimension và fact, rồi rebuild các model phụ thuộc và refresh
dashboard/snapshot sau khi build thành công. Không trộn fact dùng khóa số cũ với
dimension dùng hash mới. Database thực chưa được thay đổi trong quá trình kiểm chứng này.

Cài package và kiểm chứng bằng fixture nhỏ trong database tạm, từ thư mục repository
(PowerShell, môi trường Python đã có `duckdb`, `dbt-core` và `dbt-duckdb`):

```powershell
dbt deps --project-dir .\dbt\duckdb_warehouse
python -m unittest discover -s .\dbt\duckdb_warehouse\tests -p test_intermediate_models.py
```

Test tạo riêng bảng landing, build staging và intermediate ba lần, kiểm tra hash
theo business key, kiểu `DATE`/`TIME`, grain, SCD2 và quan hệ dimension/fact.
Lần thứ ba tạo lại schema intermediate trong database tạm để kiểm tra khóa không
phụ thuộc trạng thái bảng hoặc sequence. Không đọc hay ghi warehouse đang dùng.
