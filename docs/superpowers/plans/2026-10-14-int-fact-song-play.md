# Plan: hoàn thiện int_fact_song_play

> Thực hiện sau khi người dùng duyệt plan và chốt các câu hỏi bên dưới. Dùng skill superpowers:executing-plans để triển khai trực tiếp trong phiên hiện tại.

**Mục tiêu:** Một dòng cho mỗi lượt nghe từ stg_listen_events, lookup khóa của sáu dimension bằng các cột tại int_dim_date__retrieve.

**Thiết kế:** Dùng ref() và LEFT JOIN, giữ lượt nghe khi không tìm thấy dimension. User lookup theo khoảng hiệu lực SCD2. Không thay đổi kiến trúc pipeline hoặc ghi warehouse thật trong bước kiểm chứng.

**Công nghệ:** dbt-duckdb, DuckDB, dbt_utils; giữ dependency hiện có.

**Phạm vi:** Yêu cầu trong chat ngày 2026-10-14; đây là thay đổi giới hạn trong flow hiện có.

## Ràng buộc

- Giữ các chỉnh sửa working tree hiện có; không stage, commit, push hoặc deploy.
- Grain dự kiến: (user_id, session_id, item_in_session).
- Chỉ hoàn thiện lookup dimension; chưa bổ sung phép tính thời gian nghe hoặc KPI khi chưa được yêu cầu.
- Test dùng database tạm; không full refresh warehouse thật.

## Cần duyệt

1. Session hiện có grain theo event: join cả user_id, session_id, session_started_time = ts và session_number = itemInSession. Có giữ cách này không?
2. Đề xuất giữ lượt nghe thiếu dimension bằng LEFT JOIN và khóa NULL, báo số lượng không match. Điều này cần đồng bộ với not_null hiện có trên các khóa ngoại trong YAML.
3. Giới hạn lần này ở các khóa join, business key, play_key và play_started_at; chưa triển khai các metric khác đang được mô tả trong YAML.

## Bước 1: hoàn thiện lookup

**Files:** models/intermediate/int_fact_song_play.sql, tests/test_fact_song_play.py (đường dẫn tương đối từ dbt/duckdb_warehouse).

- [x] Điều chỉnh fixture hiện có cho nguồn stg_listen_events và phạm vi lookup; chạy trước để ghi nhận lỗi hiện tại.
- [x] Sửa dấu phẩy giữa duration/sessionId và giữa các CTE; bổ sung itemInSession để giữ grain.
- [x] Đổi tên int_dim_date__retrieve thành int_fact_song_play__columns hoặc giữ tên nếu người dùng muốn; dùng CTE này làm đầu vào join.
- [x] Thêm ref() tới user, song, session, location, date, time.
- [x] Date: CAST(ts AS DATE) = date_key; time: ts = time_key, giữ độ chính xác timestamp hiện có.
- [x] User: userId = user_id AND ts >= start_time AND ts < end_time.
- [x] Song: artist/song/duration tương ứng song_artist_name/song_name/song_duration_seconds; so sánh NULL bằng IS NOT DISTINCT FROM.
- [x] Location: city/zip/state/lon/lat tương ứng location_*; so sánh NULL bằng IS NOT DISTINCT FROM.
- [x] Session: áp dụng điều kiện đã được người dùng chốt.
- [x] Loại dòng dimension trùng hoàn toàn trong lookup song/time/session khi cần; kiểm tra và báo lỗi nếu có nhiều khóa khác nhau cùng match, không chọn tùy tiện một khóa.
- [x] Xuất play_key hash từ (user_id, session_id, item_in_session), ba business key, sáu khóa dimension và play_started_at.

## Bước 2: kiểm chứng và tài liệu

**Files:** tests/test_fact_song_play.py, models/intermediate/_models.yml; README.md nếu cần ghi giới hạn.

- [x] Chốt mô tả và tests YAML tương ứng đúng phạm vi đã duyệt; không để tests tham chiếu metric chưa tồn tại.
- [x] Fixture kiểm tra lookup đủ sáu dimension, ranh giới SCD2, timestamp có mili giây, NULL trong natural key và lượt nghe không match.
- [x] Kiểm tra dimension trùng không nhân số lượt nghe, grain/play_key unique và khóa ổn định qua rerun.
- [x] Chạy PowerShell: python -m unittest discover -s .\dbt\duckdb_warehouse\tests -p test_fact_song_play.py -v.
- [x] Chạy dbt build chọn riêng fact và tests liên quan trên database fixture; báo rõ nếu dependency hoặc môi trường chặn.
- [x] Chạy git diff --check giới hạn các file thuộc nhiệm vụ; báo kết quả và file để review.

## Điểm cần chú ý khi review

- Session không match vì page_view và listen khác timestamp hoặc itemInSession.
- Time và song hiện chưa DISTINCT nên join trực tiếp có thể nhân dòng.
- Khoảng SCD2 chồng lấn hoặc nhiều trạng thái user tại cùng timestamp.
- Cùng business key lượt nghe nhưng nội dung nguồn khác nhau: báo lỗi uniqueness, không tự chọn bản thắng.
- LEFT JOIN giữ dữ liệu thiếu dimension trong khi YAML hiện yêu cầu not_null.


## Ghi nh?n th?c thi

- Ng??i d?ng ?? duy?t c? ba ?i?m: session join b?n c?t; LEFT JOIN gi? kh?a ngu?n, kh?a dimension kh?ng match l? NULL; ch? lookup, ch?a th?m metrics.
- Th?c hi?n tr?c ti?p tr?n working tree ???c ch? ??nh ?? gi? c?c ch?nh s?a SQL hi?n c?; kh?ng stage, commit, push ho?c t?o worktree/branch m?i.
- Ruling: d?ng COUNT(*) theo grain sau join v? error() c?a DuckDB ?? t? ch?i lookup nhi?u match ho?c source tr?ng grain. C?c d?ng lookup gi?ng nhau tr?n kh?a v? c?t join ???c DISTINCT tr??c khi join.
- B??c 1: test ban ??u th?t b?i do SQL thi?u d?u ph?y gi?a CTE; sau s?a, fixture ban ??u qua hai dbt build v? b?t ???c l?i song ambiguity.
- B??c 2: fixture m? r?ng g?m session l?ch t?ng c?t join, dimension tr?ng, natural key NULL, missing date/time, SCD2 overlap v? source tr?ng grain. K?t qu?: unittest PASS (57,118 gi?y); hai dbt build th?nh c?ng, ba dbt build t? ch?i ??ng l?i song ambiguity, SCD2 overlap v? source tr?ng grain. C? 4 l??t nghe v? 4 kh?a unique, rerun gi? nguy?n k?t qu?.
- Review ??c l?p: kh?ng ph?t hi?n l?i SQL/YAML; c?c ?? xu?t b? sung coverage ?? ???c ??a v?o fixture m? r?ng.

- git diff --check tr?n c?c file thu?c nhi?m v?: PASS. Kh?ng ch?y build tr?n warehouse th?t.
