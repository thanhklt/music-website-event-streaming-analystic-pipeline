# Plan: play_ended_at và listened_seconds

**Trạng thái:** Người dùng đã yêu cầu thực thi; SQL, YAML, tests và README đã được cập nhật. Kiểm chứng trên database tạm đã đạt; không chạy trên warehouse thật.

**Mục tiêu:** Bổ sung play_ended_at là timestamp của event kế tiếp và listened_seconds là chênh lệch theo giây với play_started_at.

**Phạm vi:** Chỉ bổ sung hai cột vào int_fact_song_play. Giữ nguồn lượt nghe stg_listen_events, grain, play_key và sáu LEFT JOIN hiện tại. Triển khai trực tiếp bằng superpowers:executing-plans sau khi duyệt.

## Quyết định đã xác nhận

1. Event tiếp theo dựa trên cả stg_listen_events và stg_page_view_events, bao gồm event khác như Home/Logout. Hợp nhất hai nguồn bằng UNION trên (userId, sessionId, ts, itemInSession), loại tuple trùng trước khi tính LEAD(); lượt nghe thiếu trong page_view vẫn có mặt trên timeline.
2. Chỉ tìm event tiếp theo trong cùng (userId, sessionId). Event cuối để play_ended_at và listened_seconds đều NULL, không lấy event của user/session khác và không thay bằng duration bài hát.

## Thiết kế triển khai

- Tính timeline trước khi join dimension để dimension không ảnh hưởng LEAD().
- Dùng LEAD(ts) OVER (PARTITION BY userId, sessionId ORDER BY ts, itemInSession).
- Cùng timestamp nhưng itemInSession khác nhau được xếp theo itemInSession; listened_seconds có thể bằng 0.
- Nếu timeline có nhiều timestamp khác nhau cho cùng (userId, sessionId, itemInSession), báo lỗi dữ liệu xung đột, không tự chọn event.
- LEFT JOIN kết quả timeline hợp nhất về listen bằng bốn cột user, session, timestamp, itemInSession; giữ nguyên mọi lượt nghe nguồn. Null business key vẫn chịu tests chất lượng dữ liệu hiện có.
- Đưa play_ended_at qua các CTE lookup hiện tại, rồi tính listened_seconds bằng chênh lệch timestamp đổi sang giây, giữ phần lẻ. Không dùng phép đếm ranh giới giây làm tròn.
- Không giới hạn listened_seconds bằng duration bài hát hoặc tự đặt timeout; số này biểu thị khoảng cách tới event kế tiếp theo định nghĩa người dùng.
- Model hiện materialized table nên lần build sau tính lại timeline và cập nhật event cuối khi có event mới hoặc late event. Không tự chạy full refresh warehouse thật.

## Các bước triển khai sau khi duyệt

1. Sửa tests/test_fact_song_play.py trước, chạy để xác nhận thiếu hai cột.
2. Sửa models/intermediate/int_fact_song_play.sql: thêm ref('stg_page_view_events'), CTE timeline hợp nhất hai nguồn bằng UNION và CTE next_event; đưa play_ended_at vào lookup và thêm listened_seconds ở output. Nguồn dòng fact vẫn là stg_listen_events, không tạo dòng fact từ các page view riêng lẻ.
3. Sửa models/intermediate/_models.yml: mô tả nguồn event, phạm vi user/session, timezone UTC, đơn vị giây, NULL và ý nghĩa thời lượng; không đặt not_null cho hai cột.
4. Thêm singular dbt test tests/assert_fact_song_play_duration.sql: hai cột cùng NULL hoặc cùng có giá trị, end >= start và chênh lệch giây đúng công thức.
5. Cập nhật README.md: định nghĩa, ví dụ, xử lý event cuối và cách kiểm chứng.
6. Chạy unittest và dbt build chọn fact/tests trên database tạm; kiểm tra diff, cập nhật kết quả plan và báo cáo.

Các đường dẫn SQL/YAML/tests/README trên tương đối từ dbt/duckdb_warehouse.

## Fixture và tiêu chí đạt

- Ví dụ listen 08:00 → Home 08:01 → listen 08:02 trong cùng user/session: lượt nghe đầu có play_ended_at = 08:01 và listened_seconds = 60; Home không tạo thêm dòng fact.
- Chênh lệch 1,250 giây giữ nguyên phần lẻ; cùng timestamp và item khác nhau cho 0 giây theo thứ tự đã chốt.
- Event cuối có hai cột NULL; event của user hoặc session khác không kết thúc lượt nghe.
- Event trùng giữa listen/page_view không sinh khoảng 0 giả hoặc nhân dòng.
- Lượt nghe không có bản ghi page_view tương ứng vẫn được giữ và có vị trí trên timeline hợp nhất.
- Thêm late event vào giữa hai event rồi build lại: end/duration được cập nhật, play_key và số lượt nghe cũ giữ nguyên.
- Rerun không thay đổi kết quả; các kiểm chứng lookup/SCD2/grain/ambiguity hiện có vẫn đạt.
- Tests bắt được timeline xung đột, giá trị âm và sai công thức; dữ liệu người dùng không bị thay đổi trong kiểm chứng.

**Lệnh dự kiến (PowerShell từ root):** python -m unittest discover -s .\dbt\duckdb_warehouse\tests -p test_fact_song_play.py -v. Fixture chạy dbt build bằng profile/database tạm.

**Git:** Không stage, commit hoặc push. Giữ nguyên chỉnh sửa working tree khác.

## Ghi nhận thực thi

- [x] Test trước triển khai: thất bại vì thiếu cột play_ended_at, đúng lỗi cần bổ sung.
- [x] Timeline UNION hai nguồn, kiểm tra timestamp xung đột, LEAD trong user/session và LEFT JOIN về nguồn listen trước lookup dimension.
- [x] Bổ sung play_ended_at và EPOCH(interval) cho listened_seconds; giữ nguyên grain và play_key.
- [x] YAML và README đã ghi đơn vị giây, UTC, NULL, khoảng 0 giây, không giới hạn bởi duration và xử lý late event.
- [x] Singular test kiểm tra cặp NULL, khoảng thời gian không âm và đúng công thức.
- [x] Fixture dbt: PASS (63,486 giây). Ba dbt build hợp lệ gồm build đầu, rerun và late event; bốn build lỗi dự kiến gồm timeline xung đột, dimension song mơ hồ, SCD2 overlap và nguồn trùng grain.
- [x] Fixture singular test độc lập: PASS; chấp nhận NULL/1,25 giây/0 giây và phát hiện đúng bốn dòng sai (thiếu end, thiếu seconds, âm, sai công thức).
- [x] git diff --check trên các file chỉnh sửa thuộc nhiệm vụ: PASS.
- Ruling: thực hiện trong working tree hiện tại theo phạm vi người dùng đã chỉ định; giữ các thay đổi chưa commit từ nhiệm vụ trước, không tạo branch/worktree hoặc stage.
- Ruling: kiểm tra xung đột trên toàn timeline hợp nhất, trước khi xuất fact; không loại âm thầm event chỉ có ở page_view.
