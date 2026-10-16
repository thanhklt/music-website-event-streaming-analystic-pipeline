# Tầng dashboard dbt và lớp đọc Streamlit — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox syntax for tracking. Không tự stage, commit, push hoặc triển khai remote. Chỉ thực hiện implementation khi người dùng yêu cầu.

**Goal:** Chuyển join và chuẩn hóa thuộc tính báo cáo từ `streamlit/reports.py` sang dbt, giữ nguyên kết quả, bộ lọc và giao diện dashboard.

**Architecture:** dbt dựng bảng `dashboard.song_plays` ở grain một dòng mỗi `play_key`, đã join các dimension. Bảng `dashboard.filter_options` giữ danh sách lựa chọn từ toàn bộ dimension như hiện tại. `reports.py` đọc hai bảng, lọc bằng tham số và tổng hợp theo lựa chọn; không join lại fact/dimension.

**Tech Stack:** dbt-core và dbt-duckdb theo `poetry.lock` hiện có; DuckDB local, Streamlit, pandas, Altair, unittest và Streamlit AppTest. Không thêm dependency.

**Spec:** Phạm vi người dùng đã xác nhận trong cuộc trao đổi ngày 2026-10-15: giữ nguyên dashboard. Thiết kế và contract chi tiết nằm ngay trong tài liệu này; tham chiếu hành vi hiện tại tại `streamlit/README.md`, `streamlit/reports.py` và `streamlit/tests/test_reports.py`.

## Ràng buộc chung

- Giữ kiến trúc Kafka → Spark → HDFS → ingestion DuckDB → dbt → dashboard.
- Chỉ thay tầng phục vụ dashboard; không đổi logic nguồn, grain fact, định nghĩa duration hoặc strategy của marts.
- Giữ mọi KPI, biểu đồ, ranking, tie-break, bộ lọc và nhãn tiếng Việt hiện tại. Chỉ cập nhật thông báo nguồn/lỗi khi cần phản ánh bảng mới.
- UTC xuyên suốt; lọc từ đầu ngày bắt đầu đến trước đầu ngày sau ngày kết thúc.
- Dashboard chỉ đọc; kết nối mở theo lượt tải, có transaction và luôn đóng. Cache dữ liệu 60 giây, không cache connection.
- Ingestion và dbt chạy tuần tự với dashboard refresh; chưa xây dựng snapshot, scheduler hay cơ chế đọc trong lúc writer chạy.
- Không sửa các thay đổi có sẵn ngoài phạm vi. Kiểm tra Git trước mỗi đợt sửa; không tự stage/commit/push.
- Tất cả test tự động dùng database/profile/output tạm, không dùng warehouse thật, không full-refresh hoặc xóa dữ liệu hiện có.

## Thiết kế đã chọn

### 1. `dashboard.song_plays`

Đặt model tại `models/dashboard/song_plays.sql`, materialization `table`, schema `dashboard`. Dùng `ref()` tới `fact_song_play`, `dim_user`, `dim_song`, `dim_location`; join bằng các surrogate key tương ứng và giữ `LEFT JOIN` như truy vấn hiện tại. User join qua `user_key` để giữ phiên bản SCD2 tại thời điểm nghe.

| Cột | Contract |
|---|---|
| `play_key` | Khóa duy nhất, không NULL, giữ từ fact |
| `user_id`, `song_key`, `location_key` | Giữ nguyên giá trị và kiểu từ fact |
| `play_started_at` | Timestamp UTC, giữ NULL để metadata vẫn đếm được lượt thiếu thời gian |
| `listened_seconds` | Giữ nguyên kiểu và giá trị, gồm NULL và 0; không bù bằng độ dài bài hát |
| `state`, `gender`, `song`, `artist` | Chuẩn hóa `COALESCE(NULLIF(TRIM(value), ''), 'Unknown')` đúng như `reports.py` hiện tại |
| `play_date` | DATE từ `play_started_at`, NULL khi timestamp NULL |
| `weekday` | ISO thứ 1–7, NULL khi timestamp NULL |
| `hour` | Giờ UTC 0–23, NULL khi timestamp NULL |
| `updated_time` | Audit từ fact, tiếp tục phục vụ thông tin cập nhật fact |

Không dùng `DISTINCT` để che giấu join nhân dòng. Test uniqueness và bảo toàn fact phải phát hiện nguồn hoặc dimension không hợp lệ. Không tạo dòng Unknown giả trong bảng lượt nghe.

Chọn rebuild bảng phục vụ dashboard mỗi lần build. Cách này phản ánh thay đổi thuộc tính dimension ngay cả khi fact không đổi audit; không thêm incremental với watermark theo thời gian. Chỉ tối ưu khi có số đo chứng minh cần thiết.

### 2. `dashboard.filter_options`

Model `models/dashboard/filter_options.sql`, materialization `table`, schema `dashboard`. Hai cột VARCHAR: `filter_name` và `filter_value`.

- `filter_name='states'`: các giá trị state distinct đã chuẩn hóa từ toàn bộ `dim_location`.
- `filter_name='genders'`: các giá trị gender distinct đã chuẩn hóa từ toàn bộ `dim_user`.
- Cặp `(filter_name, filter_value)` duy nhất; hai cột không NULL. `filter_name` chỉ nhận `states`, `genders`.

Lý do có bảng riêng: dropdown hiện lấy toàn bộ dimension, gồm giá trị chưa có lượt nghe. Lấy options chỉ từ `song_plays` sẽ làm thay đổi hành vi dù KPI không đổi.

### 3. Contract Python giữ nguyên

- `DEFAULT_DATABASE`, `ROOT` giữ nguyên.
- `load_metadata(database) -> dict`: giữ các key `total_rows`, `missing_timestamp`, `min_date`, `max_date`, `updated_at`, `latest_event`, `states`, `genders`. Metadata fact tính từ `dashboard.song_plays`; options đọc `dashboard.filter_options`, sắp xếp như hiện tại.
- `load_report(database, start_date, end_date, states=(), genders=()) -> dict[str, pandas.DataFrame]`: giữ key và tên/thứ tự cột hiện tại cho `summary`, `states`, `songs`, `gender_songs`, `hourly`, `daily`, `artists`.
- Giữ parameter binding; không nội suy giá trị bộ lọc vào SQL.
- Giữ kiểm tra NULL/trùng `play_key` trên bảng dashboard để reader không trả số liệu sai. Kiểm tra dimension chuyển về dbt tests trước refresh; reader không quét lại dimension nguồn.
- Không fallback về join marts cũ khi thiếu bảng dashboard; báo cần chạy build tầng dashboard.

Phần SQL vẫn ở reader: COUNT, SUM, COUNT DISTINCT, GROUP BY và xếp hạng sau lọc. Không precompute top 5 toàn cục hoặc cộng distinct theo ngày. Sinh lưới 168 ô và lịch ngày để lấp nhóm rỗng vẫn nằm trong reader; các join với lưới nhỏ này được giữ, không phải join fact/dimension.

### 4. Định nghĩa số liệu giữ nguyên

- Lượt thiếu duration vẫn được đếm vào lượt nghe, người nghe và ranking nếu thỏa bộ lọc.
- Tổng duration bỏ qua NULL; tổng toàn NULL hiển thị 0, số lượt thiếu và coverage hiển thị riêng. Duration 0 là có dữ liệu.
- Người nghe DISTINCT `user_id`, loại NULL/-1, gộp các phiên bản SCD2.
- Bài hát phân biệt bằng `song_key`; ranking bài/nghệ sĩ loại khóa '-1', ranking state loại nhãn Unknown.
- Tất cả lượt vẫn có trong bảng dashboard. Báo cáo theo bộ lọc ngày loại timestamp NULL; metadata vẫn đếm các lượt này.
- Heatmap luôn 168 ô; toàn duration gán vào giờ bắt đầu. Xu hướng ngày có cả ngày 0 lượt.
- Metadata và report đọc riêng như hiện tại, không coi `MAX(updated_time)` là chứng nhận build thành công.

## File sẽ thay đổi khi triển khai

| File | Trách nhiệm |
|---|---|
| `dbt/duckdb_warehouse/models/dashboard/song_plays.sql` — mới | Bảng lượt nghe đã join |
| `dbt/duckdb_warehouse/models/dashboard/filter_options.sql` — mới | Options không phụ thuộc lượt nghe |
| `dbt/duckdb_warehouse/models/dashboard/_models.yml` — mới | Document contract, uniqueness, nullability, domain |
| `dbt/duckdb_warehouse/dbt_project.yml` | Schema/materialization của dashboard |
| `dbt/duckdb_warehouse/tests/assert_dashboard_song_plays_preserved.sql` — mới | Đối chiếu khóa, số dòng và giá trị fact |
| `dbt/duckdb_warehouse/tests/test_dashboard_models.py` — mới | Build SQL thật bằng dbt trong môi trường tạm |
| `streamlit/tests/dashboard_fixture.py` — mới | Fixture và helper dựng project dbt tạm dùng chung cho tests |
| `streamlit/tests/test_reports.py` | Hồi quy reader, UI và refresh trên bảng dashboard |
| `streamlit/reports.py` | Chỉ đọc dashboard, lọc và tổng hợp |
| `streamlit/app.py` | Chỉ cập nhật nguồn và hướng dẫn lỗi; không đổi UI/KPI |
| `README.md`, `streamlit/README.md`, `dbt/duckdb_warehouse/README.md` | Hướng dẫn build, refresh, giới hạn và test |

## Review Focus

1. Một user có nhiều phiên bản SCD2: không nhân lượt hoặc người nghe; thuộc tính lấy đúng `user_key` (Task 1, 2).
2. Dữ liệu dimension chưa có lượt nghe: dropdown vẫn giữ lựa chọn; lọc trả rỗng đúng cách (Task 1, 2).
3. Duration toàn NULL so với 0: tổng cùng 0 nhưng coverage và measured_plays khác nhau (Task 1, 2).
4. Fact hoặc dimension trùng khóa: build/test phải lỗi; không dùng DISTINCT che fan-out (Task 1).
5. Late event/dimension update: rebuild đưa thay đổi lên dashboard, refresh bỏ cache, rerun không duplicate (Task 1, 3).

## Task 1 — Dựng tầng dashboard và kiểm chứng contract

**Consumes:** Các model marts và hành vi mô tả ở trên.

**Produces:** `dashboard.song_plays`, `dashboard.filter_options`; helper fixture dùng được cho tests reader/UI.

- [ ] Tạo helper trong `streamlit/tests/dashboard_fixture.py`: `create_marts_fixture(path)` giữ fixture sáu dòng hiện có; `build_dashboard_fixture(path)` tạo project/profile dbt tạm, copy các model dashboard và macro schema thực từ repo, dựng các model đầu vào cùng tên từ bảng fixture rồi chạy `dbt build` bằng subprocess. Mọi log/target nằm trong thư mục tạm, đóng writer trước subprocess. Tham khảo cách cô lập trong `tests/test_marts_models.py`; không chạy upstream thật của pipeline.
- [ ] Viết `test_dashboard_models.py` dùng helper, thêm đường dẫn tests fixture rõ ràng. Xác nhận test thất bại khi model chưa tồn tại, không phải do profile/dependency không tìm thấy.
- [ ] Fixture gốc kỳ vọng: 6 dòng, 6 play_key, 1 timestamp NULL, 3 duration NULL; 5 lượt trong 12–13/10/2026, 2 người nghe, tổng 180 giây. Hai phiên bản user 1 vẫn cùng một người nghe; hai bài cùng tên khác nghệ sĩ giữ riêng song_key.
- [ ] Bổ sung fixture state/gender chưa có lượt nghe, chuỗi trắng/NULL chuẩn hóa thành Unknown; kiểm tra options đầy đủ và không trùng.
- [ ] Viết hai model, cấu hình `dashboard` và YAML theo contract. `play_date`, `weekday`, `hour` nullable có chủ đích; domain chỉ kiểm tra giá trị không NULL.
- [ ] Thêm singular test bảo toàn: số dòng bằng fact; không thiếu/thừa play_key; `user_id`, `song_key`, `location_key`, `play_started_at`, `listened_seconds`, `updated_time` bằng fact theo so sánh NULL-safe. Các test key đảm bảo so sánh không bị fan-out.
- [ ] Test lỗi chủ động: trùng key ở từng dimension được join và fact NULL/trùng key phải bị từ chối bởi dbt tests; không chấp nhận build/test thành công với bảng dashboard bị nhân dòng.
- [ ] Test rebuild: chuyển duration NULL thành giá trị, đổi nhãn dimension mà không sửa audit fact; build lại, đối chiếu giá trị mới, chạy lần nữa vẫn 6 dòng. Với dimension chỉ phục vụ lookup, không thay grain fact.
- [ ] Chạy từ root: `poetry run python -m unittest discover -s .\dbt\duckdb_warehouse\tests -p test_dashboard_models.py -v`. Kỳ vọng PASS; các tình huống lỗi là assertions về subprocess thất bại có chủ đích.

## Task 2 — Chuyển reader sang bảng dashboard

**Consumes:** Hai bảng và helper Task 1.

**Produces:** API `load_metadata`/`load_report` tương thích với `app.py`, không đọc marts trực tiếp.

- [ ] Chuyển fixture trong `test_reports.py` sang helper dùng chung, build bảng dashboard trước khi gọi reader. Giữ nguyên các assertions số liệu hiện có.
- [ ] Thêm test reader vẫn hoạt động khi các bảng marts được bỏ trong database fixture sau build; chứng minh không còn phụ thuộc join/runtime scan marts. Test riêng có bảng dashboard thiếu, DB thiếu, play_key NULL/trùng để giữ lỗi rõ ràng và không tạo DB ngoài ý muốn.
- [ ] Thêm test tổ hợp ngày/state/gender, ngày kết thúc với event sát nửa đêm, lọc Unknown và chuỗi chứa dấu nháy qua parameter binding. Kiểm tra distinct user qua nhiều ngày, top 5 tính sau lọc, thứ tự đồng hạng ổn định.
- [ ] Test nhóm toàn duration NULL và nhóm duration 0: cùng tổng 0 nhưng `duration_missing`/`measured_plays` khác; khoảng rỗng có summary 0, ranking rỗng, đủ ngày và 168 ô heatmap.
- [ ] Chạy tests mới để xác nhận reader cũ thất bại vì còn đọc marts.
- [ ] Sửa `reports.py`: bỏ join và chuẩn hóa dimension; dùng cột đã chuẩn bị trong `dashboard.song_plays`. Metadata options đọc `dashboard.filter_options`. Giữ cấu trúc trả về, read-only transaction, đóng kết nối cả khi lỗi, và kiểm tra uniqueness play_key bảng dashboard.
- [ ] Chạy `poetry run python -m unittest discover -s .\streamlit\tests -p test_reports.py -v`; đóng mọi kết nối reader trước khi test mở writer. Kỳ vọng toàn bộ report tests PASS; hoàn thiện refresh UI ở Task 3.

## Task 3 — Kiểm chứng UI, refresh và hướng dẫn vận hành

**Consumes:** dbt models Task 1 và API reader Task 2.

**Produces:** Dashboard giữ hành vi, tests chạy được và README đúng nguồn dữ liệu.

- [ ] Cập nhật AppTest refresh: sửa marts fixture → build dashboard fixture lại → bấm refresh; không chỉ sửa marts rồi kỳ vọng bảng materialized tự thay đổi. Giữ kỳ vọng KPI từ 180 giây lên 3660 giây, giao diện hiện `1,02` giờ sau khi p1 chuyển từ 120 lên 3600 giây.
- [ ] Kiểm tra AppTest: render không exception, lọc NY có 1 lượt, kết hợp NY/F trả rỗng, thiếu bảng có hướng dẫn build, thiếu DB không tạo file, refresh xóa cache. Kiểm tra writer mở được sau lượt đọc và sau lỗi.
- [ ] Sửa nội dung nguồn và lỗi trong `app.py` để nhắc schema dashboard. Giữ key widget, KPI, biểu đồ và TTL hiện tại.
- [ ] Cập nhật README: upstream marts hợp lệ → build dashboard và tests thành công → writer đóng → refresh. Từ root, `poetry run dbt build --project-dir .\dbt\duckdb_warehouse --select path:models/dashboard` chỉ dựng tầng dashboard trên marts đã tồn tại; dùng profile local hiện có. Khi cần build upstream, dùng selector có dấu `+` phía trước, giải thích phạm vi rộng hơn; không tự chạy trên warehouse thật để xác minh plan.
- [ ] Document materialized table không tự cập nhật khi marts thay đổi, thời gian fact audit không phải thời gian build, không refresh sau build lỗi và chưa có cơ chế snapshot nguyên tử nhiều bảng. Giữ hướng dẫn bind localhost, stop Ctrl+C và xung đột writer.
- [ ] Chạy hai lệnh unittest của Task 1 và Task 2; thêm regression marts: `poetry run python -m unittest discover -s .\dbt\duckdb_warehouse\tests -p test_marts_models.py -v` khi helper/cấu hình có ảnh hưởng tới dbt project. Chỉ claim những lệnh thực sự chạy và PASS; ghi blocker môi trường nếu có.
- [ ] Review diff: không đổi upstream models, không thêm dependency, không có DB/profile dữ liệu thật trong Git diff. Bàn giao danh sách file, kết quả test và giới hạn; không stage/commit/push.

## Tiêu chí hoàn thành implementation

- dbt build trong fixture kiểm chứng hai bảng dashboard và test dữ liệu thực thi thành công.
- Bảo toàn grain và giá trị fact; lỗi dimension không bị che giấu.
- KPI, filter, ranking, Unknown/NULL, ngày rỗng, heatmap và SCD2 giữ đúng kết quả xác định trước.
- Reader hoạt động không cần truy cập bảng marts sau build; app không tính lại join/chuẩn hóa dimension.
- Cập nhật dữ liệu sau rebuild + refresh được xác minh bằng AppTest.
- Tài liệu và API Python nhất quán. Chưa benchmark nên chưa tuyên bố mức tăng tốc; chưa publish/deploy.

## Trạng thái kế hoạch

Chỉ tạo tài liệu, chưa triển khai model hoặc Python, chưa chạy tests sản phẩm. Phương án tự thực hiện tuần tự phù hợp vì ít task và dùng chung contract. Người dùng review kế hoạch trước khi yêu cầu triển khai.
