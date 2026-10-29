# eventsim-analystic-pipeline



--- INSIGHTS:

- dataset do đã được đóng gói thành JAR chỉ cho phép sử dụng giao thức PLAINTEXT
- Vì đây là dữ liệu mock, thời gian đi vào hệ thống không gần với thời gian sinh ra events nên hệ thống set WaterMark bằng kafka_timestamp
- tiến trình hadoop chỉ chạy bằng user hadoop
-- KNOWN PROBLEM:
- Vì hdfs_to_duckdb.py đang sử dụng HDFSWeb với requests nên dashboard sẽ bị chậm với thực tế
=> Có 1 solution là dùng spark structure streaming 
- VÌ mỗi SessionId được reset lại theo thời gian. Vì thế việc xác định sessionId tiếp theo ứng với sessionID sẽ vô nghĩa

- SessionID Unique trong 1 lần chạy eventsim
## On-going
<img src="./images/architecture_design.png" alt="architecture_design" width="400"/>


- Lý do tách eventsim ra khỏi docker-compose vì em muốn để kafka luôn chạy trước rồi mới chạy eventsim thay vì để 2 service gắn với nhau để phù hợp với thực tế.

- Hiện tại đang script đang dedupe pipeline ngay bước ingest (lý do là dedupe record trùng hoàn toàn, còn record bị trùng về nghiệp vụ sẽ được xử lý khi transform bằng dbt)

- Insight: end_time khi phát có thể null nếu nó là event cuối cùng của session.

- Phân loại:
Night: 00:00–04:59
Morning: 05:00–11:59
Afternoon: 12:00–16:59
Evening: 17:00–20:59
Night: 21:00–23:59

- Cấu trúc một WebHDFS Response
HTTP/1.1 200 OK
Content-Type: application/json
Transfer-Encoding: chunked

{
  "FileStatuses": {
    "FileStatus": [
      {
        "pathSuffix": "0",
        "type": "FILE",
        "length": 550
      },
      {
        "pathSuffix": "1",
        "type": "FILE",
        "length": 550
      }
    ]
  }
}


## Phiên bản:

v0.1 đọc dữ liệu từ WebHDFS -> local (staging) -> DuckDB
