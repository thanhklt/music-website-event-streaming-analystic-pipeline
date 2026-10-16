# Marts Layer Implementation Plan

> Người dùng đã duyệt và yêu cầu thực thi. Triển khai trực tiếp bằng skill superpowers:executing-plans trong working tree hiện tại, không stage/commit/push.

**Goal:** Tạo sáu dimension và fact_song_play trong marts, có dòng Unknown, khóa ngoại mặc định và audit; fact dùng incremental.

**Architecture:** Mỗi mart đọc intermediate bằng ref(), giữ cách viết config và CTE hiện có. Time key chuyển thành số giây ngay tại intermediate, time_value lưu giờ thực tế theo grain giây. Sáu dim marts dùng table; fact marts dùng incremental theo play_key.

**Tech Stack:** dbt-core, dbt-duckdb 1.11.0 đang cài, DuckDB và packages hiện có; không thêm dependency/service.

**Spec:** Yêu cầu trong cuộc hội thoại và bổ sung mới: fact marts incremental; int_dim_time có time_key số giây từ đầu ngày và time_value TIME; không thay các cột fact ngoài khóa ngoại; audit theo thời điểm build.

## Global Constraints

- Chỉ sửa intermediate liên quan time: int_dim_time, join time trong int_fact_song_play, YAML/tests tương ứng. Không sửa staging, logic timeline/duration hoặc các join khác.
- Không stage, commit, push, deploy hoặc tự accept thay đổi; giữ các chỉnh sửa hiện có của người dùng.
- Không tự full refresh, xóa dữ liệu hoặc ghi warehouse thật để kiểm chứng; dùng fixture/database/profile tạm.
- Giữ tên cột hiện có, bổ sung time_value tại intermediate/marts và bốn cột audit tại marts.
- Khóa hash user/song/session/location giữ VARCHAR và giá trị gốc; Unknown dùng '-1'. Không sinh lại play_key.
- Chỉ COALESCE NULL; không thay chuỗi rỗng hoặc giá trị hợp lệ. FK có giá trị nhưng mồ côi phải bị relationships test phát hiện.

## Thiết kế dimension

| Mart | Nguồn | PK | Khóa Unknown |
|---|---|---|---|
| dim_user | int_dim_user | user_key VARCHAR | '-1' |
| dim_song | int_dim_song | song_key VARCHAR | '-1' |
| dim_session | int_dim_session | session_key VARCHAR | '-1' |
| dim_location | int_dim_location | location_key VARCHAR | '-1' |
| dim_date | int_dim_date | date_key DATE | DATE '1970-01-01' |
| dim_time | int_dim_time | time_key INTEGER | -1 |

- Sáu dim materialized='table'. Ngoài PK/audit: VARCHAR NULL -> 'Unknown'; số NULL -> -1 đúng kiểu nguồn; DATE/TIMESTAMP NULL -> 1970-01-01 lúc 00:00:00; BOOLEAN NULL -> FALSE.
- Mỗi dim UNION ALL thêm đúng một dòng Unknown với mặc định trên. time_value của dòng Unknown dùng TIME '00:00:00'; khác nửa đêm thật nhờ key -1 và 0.
- PK nguồn không COALESCE. PK NULL, reserved key '-1'/ngày 1970-01-01 có sẵn trong nguồn phải làm kiểm chứng thất bại; không âm thầm bỏ hoặc ghi đè.
- DATE '1970-01-01' dành riêng cho Unknown. Nếu nguồn cần hỗ trợ ngày thực đó, phải thay thiết kế date key, không trộn với Unknown.
- DISTINCT loại dòng dim trùng hoàn toàn; cùng PK nhưng khác thuộc tính phải bị test unique phát hiện.
- dim_user giữ mọi phiên bản SCD2, không lọc chỉ is_active. Không đổi grain của dim_session.

## Time key từ intermediate

- int_dim_time thêm time_value kiểu TIME và đổi time_key từ TIME sang INTEGER.
- Grain một giây: time_key = hour * 3600 + minute * 60 + second, miền 0..86399. Intermediate chưa thêm dòng Unknown.
- Chuẩn hóa giờ xuống đầu giây, không làm tròn: 08:00:01.999 -> time_value 08:00:01, key 28801. Timestamp chính xác, gồm phần lẻ, vẫn giữ ở fact.
- int_dim_time DISTINCT sau chuẩn hóa; nhiều ngày hoặc mili giây khác nhau trong cùng giây tạo một dòng. Các thuộc tính giờ/phút/giây và time_period tính nhất quán theo giờ đã chuẩn hóa.
- int_fact_song_play đổi join time: số giây từ play.ts = int_dim_time.time_key. Vẫn LEFT JOIN, lấy khóa từ dimension; không match thì NULL. Không đổi play_started_at/play_ended_at/listened_seconds, play_key hoặc các khóa khác.
- dim_time marts lấy trực tiếp time_key/time_value từ intermediate, không tạo lại khóa. Fact marts chỉ COALESCE(play_time_key, -1), không suy ra từ play_started_at khi FK nguồn NULL.
- Khi áp dụng lên warehouse thật phải build lại int_dim_time rồi int_fact_song_play trước marts. Không chạy marts trên bảng intermediate còn kiểu khóa cũ; không tự thực hiện migration/full refresh trong nhiệm vụ kiểm chứng.

## Fact incremental

```jinja
{{ config(
    materialized='incremental',
    unique_key='play_key',
    incremental_strategy='delete+insert',
    on_schema_change='fail'
) }}
```

- Adapter dbt-duckdb 1.11.0 local đã được kiểm tra có strategy delete+insert. Khi triển khai vẫn phải kiểm chứng đường incremental bằng database tạm, không chỉ lần tạo bảng đầu tiên.
- Nguồn int_fact_song_play; grain/play_key giữ nguyên. user_key/song_key/session_key/location_key NULL -> '-1'; play_date_key NULL -> DATE '1970-01-01'; play_time_key INTEGER NULL -> -1.
- Giữ nguyên play_key, user_id, session_id, item_in_session, play_started_at, play_ended_at, listened_seconds, kể cả NULL. Không DISTINCT, aggregate, COALESCE hoặc tính lại các cột này.
- Lần đầu nạp toàn nguồn. Khi is_incremental(): so sánh nguồn đã chuẩn hóa FK với {{ this }} theo play_key; chỉ nạp dòng mới hoặc dòng có bất kỳ cột dữ liệu nào thay đổi bằng IS DISTINCT FROM. Không đưa audit vào phép so sánh.
- delete+insert thay dòng cùng play_key, thêm dòng mới. Nguồn cần kiểm tra play_key unique/not_null trước upsert để không nạp trùng.
- Không chỉ dùng MAX(play_started_at) hoặc lookback tùy ý: late event, dimension đến muộn và user SCD2 có thể đổi duration/FK của play cũ. Quét so sánh toàn nguồn để bắt các thay đổi; giảm số dòng ghi, chưa tối ưu số dòng đọc.
- Không tự xóa fact khi play_key mất khỏi snapshot intermediate. Test anti-join target -> source và kiểm tra EXCEPT ALL phải báo lỗi lệch snapshot, chặn công bố kết quả. Chính sách source delete/reconcile cần yêu cầu riêng; không tuyên bố hỗ trợ xóa nguồn.
- on_schema_change='fail': nếu bảng cũ không tương thích thì báo migration cần xử lý, không tự thay schema hoặc full refresh.
- Tham khảo cơ chế: [dbt incremental models](https://docs.getdbt.com/docs/build/incremental-models).

## Audit và chi tiết cần duyệt cùng plan

- inserted_time, updated_time kiểu TIMESTAMP UTC, cùng lấy run_started_at; inserted_by, updated_by đều VARCHAR 'dbt'.
- Dim dựng lại bằng table: mọi dòng, kể cả Unknown, có hai timestamp bằng thời điểm build hiện tại.
- Fact incremental: dòng mới/được cập nhật có cả hai timestamp bằng thời điểm build hiện tại; dòng không đổi giữ audit của lần nạp trước. Dòng cập nhật không giữ inserted_time cũ.
- Đây là điều chỉnh so với plan table trước đó: audit phản ánh lần nạp dòng, không buộc mọi dòng fact phải đổi audit ở mỗi invocation, vì như vậy sẽ phải ghi lại toàn bảng.

## Review Focus

1. TIME cùng giây nhưng khác mili giây/ngày: một dim_time, key duy nhất; fact không nhân dòng.
2. Business key/metrics NULL của fact: giữ nguyên; test cột bắt buộc vẫn báo lỗi, không chữa bằng COALESCE.
3. Reserved date/hash key có trong nguồn hoặc dim cùng PK khác thuộc tính: báo lỗi, không tạo PK trùng hoặc ghi đè.
4. FK mồ côi hoặc nguồn mất play_key: build tests phải báo lỗi, không coi incremental là snapshot đúng khi còn dòng dư.
5. Rerun không đổi không ghi lại fact; late/update event và FK đến muộn cập nhật đúng play cũ, audit đổi chỉ trên dòng được nạp.

## Task 0: Đồng bộ intermediate time

**Files** (tương đối từ dbt/duckdb_warehouse): models/intermediate/int_dim_time.sql, int_fact_song_play.sql, _models.yml; tests/test_fact_song_play.py; thêm tests/test_time_dimension.py.

**Consumes:** stg_listen_events.ts TIMESTAMP.
**Produces:** int_dim_time(time_key INTEGER, time_value TIME, thuộc tính giờ hiện có); int_fact_song_play.play_time_key INTEGER hoặc NULL.

- [x] Viết fixture trước: nửa đêm, cuối ngày, mili giây và cùng giờ qua nhiều ngày; chạy để xác nhận key hiện còn TIME.
- [x] Thêm time_value theo grain giây, tính time_key bằng công thức đã chốt, DISTINCT sau chuẩn hóa; không thêm Unknown ở intermediate.
- [x] Đồng bộ LEFT JOIN time trong intermediate fact; giữ nguyên mọi cột khác.
- [x] Cập nhật YAML và fixture lookup hiện có sang INTEGER; kiểm chứng keys 0/28801/86399, time_value, uniqueness và không làm tròn phần lẻ.
- [x] Chạy test_time_dimension.py và test_fact_song_play.py trên database tạm; duration/timeline/SCD2/play_key không đổi.

## Task 1: Sáu dimension và audit

**Files:** models/marts/dim_user.sql, dim_song.sql, dim_session.sql, dim_date.sql, dim_time.sql; điền file dim_location.sql hiện trống; thêm tests/test_marts_models.py.

**Consumes:** Intermediate hiện có và time contract từ Task 0.
**Produces:** Sáu dim theo bảng mapping, thuộc tính mặc định, time_value và audit.

- [x] Viết fixture NULL từng kiểu, dim trùng hoàn toàn, user SCD2 hai phiên bản và time theo giây; chạy trước để xác nhận thiếu marts.
- [x] Viết CTE <model>__source -> <model>__coalesce -> <model>__unknown -> <model>__audit -> SELECT. UNION ALL theo thứ tự cột, cast tường minh.
- [x] Kiểm tra PK/thuộc tính hợp lệ không đổi; NULL đúng mặc định và kiểu; mỗi dim đúng một dòng Unknown; user giữ đủ phiên bản và time keys -1/0 không lẫn nhau.
- [x] Kiểm tra bốn audit có giá trị, by='dbt', timestamp bằng run_started_at cùng invocation.

## Task 2: Fact incremental, tests và tài liệu

**Files:** models/marts/fact_song_play.sql; điền models/marts/_models.yml hiện trống; mở rộng tests/test_marts_models.py; thêm tests/assert_marts_fact_preserved.sql, assert_marts_unknown_rows.sql, assert_marts_audit.sql; cập nhật README.md.

**Consumes:** Intermediate fact khóa time số và sáu dim từ Task 1.
**Produces:** Fact incremental theo play_key, bảo toàn nguồn khi không có source delete, audit theo lần nạp dòng; tests/tài liệu cho cả bảy marts.

- [x] Viết fixture trước cho FK đủ/NULL, metrics NULL, giây lẻ và business key nguồn; chạy xác nhận thiếu fact.
- [x] Viết config incremental như trên; CTE source -> coalesce_foreign_keys -> changed_rows (is_incremental) -> audit -> SELECT. Chiếu cột tường minh, chỉ thay sáu FK/thêm audit.
- [x] YAML mô tả grain/cột/defaults/audit; PK not_null/unique, FK not_null/relationships, audit by accepted_values; time key -1 hoặc 0..86399.
- [x] assert_marts_fact_preserved dùng EXCEPT ALL hai chiều trên cột giữ nguyên; bắt mất/nhân/sửa dòng và target dư khi source mất key. Kiểm tra FK chuẩn hóa riêng trong fixture.
- [x] assert_marts_unknown_rows kiểm tra đúng một Unknown và mặc định đúng ở mọi dim, reserved keys không có trong nguồn. assert_marts_audit kiểm tra hai timestamp bằng nhau và by='dbt', không ép dòng fact không đổi bằng invocation mới.
- [x] Fixture lỗi: reserved keys, PK NULL/trùng khác thuộc tính, FK mồ côi và source delete phải bị build/tests phát hiện.
- [x] Build marts lần đầu rồi rerun không đổi: PK/count/content/audit fact giữ nguyên, không duplicate; audit dim đổi theo invocation.
- [x] Thêm play mới rồi build: chỉ thêm dòng mới. Sửa end/duration/FK của play cũ rồi build: cập nhật đúng dòng, không nhân bản; hai timestamp của dòng cập nhật bằng invocation hiện tại.
- [x] Kiểm tra late event cũ, dimension đến muộn/user_key đổi vẫn cập nhật fact. Kiểm tra artifacts/log thể hiện delete+insert incremental thực sự được chạy.
- [x] README ghi time key mới từ intermediate, mapping defaults, incremental insert/update/rerun, giới hạn source delete, audit và lệnh kiểm chứng.
- [x] Chạy unittest cho test_time_dimension.py, test_fact_song_play.py, test_marts_models.py từ root; expected PASS. Fixture dbt build chọn marts và tests liên quan với database/profile/target/log tạm.
- [x] git diff --check các file nhiệm vụ; review và báo kết quả/giới hạn/file cho người dùng, không stage/commit/push.

## Trạng thái

- Plan cập nhật theo yêu cầu mới: fact marts incremental, time_key số ngay từ intermediate và bổ sung time_value.
- Đã triển khai SQL/YAML/tests/README; chưa chạy build hoặc migration trên warehouse thật.

## Ghi nhận thực thi và quyết định

- Task 0: test trước sửa thất bại vì thiếu time_value. Sau sửa, test_time_dimension.py PASS; test_fact_song_play.py PASS 2 tests, gồm lookup/time key, duration, timeline và các tình huống dữ liệu lỗi.
- Task 1/2: test trước sửa thất bại vì chưa có schema marts. Đã xây sáu dimension, fact incremental, YAML, ba singular tests và macro audit dùng chung macros/marts_audit_columns.sql.
- Ruling: dùng macro audit dùng chung để mọi bảng có cùng biểu thức UTC run_started_at; không thêm dependency.
- Ruling: giữ nguyên phần guard ambiguity đã được người dùng sửa ở intermediate fact; fixture kiểm tra lỗi uniqueness/grain của dbt thay vì yêu cầu thông báo từ guard cũ. Không phục hồi code đã bị người dùng bỏ.
- Ruling: fixture intermediate loại assert_marts_fact_preserved khỏi selection vì không tạo downstream marts; kiểm chứng bảo toàn hai lớp chạy trong fixture marts riêng.
- Review phát hiện DISTINCT sau COALESCE có thể che xung đột NULL/sentinel. Đã chuyển DISTINCT lên dữ liệu nguồn trước COALESCE ở cả sáu dim; test regression đã chạy FAIL rồi PASS. Review lại không còn finding.
- Kiểm chứng marts trên database tạm gồm build đầu, rerun, insert/update, dimension đến muộn, audit, source delete, reserved/NULL keys, conflicting dimension và orphan FK. Lượt cuối: PASS 2 tests (99,873 giây), gồm năm dbt build hợp lệ và ba lần lỗi chất lượng dữ liệu dự kiến; test nguồn PK không hợp lệ và regression NULL/sentinel cũng đạt.
- git diff --check các file tracked thuộc nhiệm vụ và kiểm tra whitespace/newline 14 file mới: PASS.
- Giới hạn: incremental quét so sánh toàn nguồn, chưa tự đồng bộ source delete; dữ liệu thật cần migration time key theo thứ tự đã ghi trong README. Không chạy full refresh, không publish dashboard.
