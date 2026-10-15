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
| `int_dim_time` | `time_key` | Số giây từ 00:00:00 UTC, kiểu `INTEGER`; `time_value` kiểu `TIME` |

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

### Lookup của int_fact_song_play

Nguồn là `stg_listen_events`, grain `(user_id, session_id, item_in_session)`.
Fact giữ business key nguồn, tạo `play_key`, lookup sáu dimension bằng `LEFT JOIN`
và giữ `play_started_at`. Khóa dimension không match là NULL; relationships tests
kiểm tra các khóa có giá trị. Fact có thời gian bắt đầu/kết thúc và khoảng cách
đến event kế tiếp theo giây; chưa tính vị trí bài hát hoặc gói tài khoản.

User được lookup theo khoảng SCD2 `[start_time, end_time)`. Session hiện là
dimension theo event: join cả user, session, timestamp và itemInSession, không chỉ
session ID. Time lookup bằng số giây từ đầu ngày; timestamp đầy đủ và mili giây
vẫn giữ trong play_started_at/play_ended_at. Song/location so sánh NULL
bằng `IS NOT DISTINCT FROM`. Dòng lookup trùng hoàn toàn được gộp; nếu vẫn có nhiều
match hoặc nguồn trùng grain, tests uniqueness/grain báo lỗi; không chọn tùy tiện một khóa.

Chạy fixture độc lập (`dbt build`, rerun, late event và các tình huống lỗi dự kiến, database tạm):

```powershell
python -m unittest discover -s .\dbt\duckdb_warehouse\tests -p test_fact_song_play.py -v
```

Theo dõi số lượt nghe thiếu dimension sau khi build trên database cần kiểm tra:

```sql
SELECT
    COUNT(*) AS play_count,
    COUNT(*) FILTER (WHERE user_key IS NULL) AS missing_user,
    COUNT(*) FILTER (WHERE song_key IS NULL) AS missing_song,
    COUNT(*) FILTER (WHERE session_key IS NULL) AS missing_session,
    COUNT(*) FILTER (WHERE location_key IS NULL) AS missing_location,
    COUNT(*) FILTER (WHERE play_date_key IS NULL) AS missing_date,
    COUNT(*) FILTER (WHERE play_time_key IS NULL) AS missing_time
FROM intermediate.int_fact_song_play;
```

Fixture có 4 lượt nghe và 4 play_key khác nhau; số thiếu tương ứng là
user=1, song=1, session=2, location=1, date=1, time=1. Các số thiếu có thể cùng
thuộc một lượt nghe, không cộng chúng để suy ra số lượt nghe bị thiếu dimension.

### play_ended_at và listened_seconds

Timeline hợp nhất `stg_listen_events` và `stg_page_view_events` bằng `UNION` trên
`(userId, sessionId, ts, itemInSession)` để event xuất hiện ở cả hai nguồn chỉ
được tính một lần. `LEAD(ts)` chạy trong cùng `(userId, sessionId)`, sắp xếp theo
`ts, itemInSession`, trước các join dimension. Nguồn dòng fact vẫn là listen;
Home/Logout chỉ giúp xác định thời điểm kết thúc, không tạo lượt nghe mới.

`play_ended_at` là timestamp UTC của event kế tiếp. `listened_seconds` được tính
bằng `EPOCH(play_ended_at - play_started_at)`, giữ phần lẻ của giây. Event cuối
trong user/session có cả hai cột NULL. Hai event cùng timestamp nhưng khác
itemInSession có thể cho 0 giây. Cùng event identity nhưng khác timestamp làm
model báo lỗi `Conflicting event timestamps in play timeline`.

Ví dụ nghe 08:00 → Home 08:01 → nghe 08:02: lượt nghe đầu kết thúc 08:01, có
`listened_seconds = 60`. Đây là khoảng cách tới event tiếp theo; không giới hạn
bằng duration bài hát và không đặt timeout. Fixture cũng kiểm tra 1,25 giây,
0 giây và khoảng cách 86.280 giây để xác nhận chính sách này.

Model được dựng lại từ staging mỗi lần build. Late event có thể làm thay đổi
end/duration của lượt nghe trước đó mà không đổi play_key. Singular test
`assert_fact_song_play_duration` kiểm tra cặp NULL, thời lượng không âm và đúng
công thức; fixture kiểm tra thêm nguồn event, user/session và thứ tự timeline.
Các kiểm chứng dùng database tạm, không chạy trên warehouse đang sử dụng.

### Marts

Marts đọc intermediate bằng `ref()`: `dim_user`, `dim_song`, `dim_session`,
`dim_location`, `dim_date`, `dim_time` và `fact_song_play`. Sáu dimension dựng lại
bằng `table`; fact dùng `incremental`, `unique_key='play_key'`, strategy
`delete+insert`, `on_schema_change='fail'`.

| Kiểu/khóa | Mặc định |
|---|---|
| Thuộc tính VARCHAR NULL | `'Unknown'` (giữ nguyên chuỗi rỗng) |
| Thuộc tính số NULL | `-1`, giữ kiểu số |
| DATE/TIMESTAMP NULL | `1970-01-01` lúc `00:00:00` |
| BOOLEAN NULL | `false` |
| TIME NULL | `00:00:00` |
| Khóa user/song/session/location Unknown | chuỗi `'-1'` |
| Khóa date Unknown | `DATE '1970-01-01'` |
| Khóa time Unknown | số nguyên `-1` |

Mỗi dimension có đúng một dòng Unknown. Primary key nguồn không COALESCE;
NULL hoặc reserved key có sẵn trong nguồn làm model báo lỗi. Ngày 1970-01-01
được dành riêng cho Unknown. Dòng trùng hoàn toàn được gộp, cùng PK khác thuộc
tính bị test unique phát hiện. User giữ toàn bộ phiên bản SCD2.

Ngay từ `int_dim_time`, `time_key = hour * 3600 + minute * 60 + second` là
INTEGER 0..86399, `time_value` là TIME cắt xuống đầu giây. Ví dụ 08:00:01.999
thành key 28801, time_value 08:00:01. Các ngày khác nhau/các phần lẻ trong cùng
giây dùng chung một dòng. Giờ nửa đêm thật có key 0, Unknown ở marts có key -1.
`int_fact_song_play.play_time_key` được lookup bằng cùng công thức và vẫn NULL
nếu thiếu dimension. Độ chính xác của timestamp/duration fact không đổi.

Fact marts chỉ COALESCE sáu khóa ngoại; giữ nguyên mọi cột khác, kể cả NULL.
Không thay FK có giá trị nhưng mồ côi thành Unknown: relationships test báo lỗi.
Trước khi nạp, model từ chối nguồn play_key NULL/trùng. Lần đầu nạp toàn nguồn;
incremental so sánh NULL-safe toàn bộ cột dữ liệu sau chuẩn hóa FK, chỉ ghi
dòng mới hoặc thay đổi. Không dùng MAX(event_time)/lookback nên late event,
duration cũ thay đổi và dimension đến muộn vẫn được cập nhật. Có quét toàn nguồn
để so sánh, chưa tối ưu lượng dữ liệu đọc.

Bốn cột audit: `inserted_time`, `inserted_by`, `updated_time`, `updated_by`.
Hai timestamp cùng lấy `run_started_at` UTC và hai cột by bằng `'dbt'`.
Dimension đổi audit mỗi build. Fact không đổi giữ audit cũ; dòng mới/cập nhật
đặt lại cả hai timestamp bằng thời điểm nạp hiện tại, không phải thời điểm
xuất hiện lần đầu.

Không tự xóa fact khi play_key biến mất khỏi intermediate. Test
`assert_marts_fact_preserved` đối chiếu hai chiều bằng EXCEPT ALL, phát hiện cả
dòng bị thiếu, bị sửa và target dư do source delete. Chỉ dùng kết quả sau khi
`dbt build` và tests thành công; nhiệm vụ này không có bước publish dashboard.
Schema không tương thích làm incremental dừng; không tự full refresh/migrate.

Kiểm chứng local, từ root (Python đã có duckdb/dbt-duckdb và dbt packages):

```powershell
python -m unittest discover -s .\dbt\duckdb_warehouse\tests -p test_time_dimension.py -v
python -m unittest discover -s .\dbt\duckdb_warehouse\tests -p test_fact_song_play.py -v
python -m unittest discover -s .\dbt\duckdb_warehouse\tests -p test_marts_models.py -v
```

Fixture tự tạo database/profile/log/target tạm, kiểm tra build đầu, rerun,
insert/update, late dimension, audit, Unknown, source delete và dữ liệu lỗi.
Không mở warehouse người dùng. Khi chủ động áp dụng kiểu khóa time mới lên
warehouse thật, cần ngắt DBeaver/ingestion writer, dựng lại `int_dim_time`,
`int_fact_song_play` rồi mới build marts; không chạy marts trên intermediate
còn khóa TIME. Các lệnh kiểm chứng ở trên không thực hiện migration này.
