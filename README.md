# eventsim-analystic-pipeline

## On-going
<img src="./images/architecture_design.png" alt="architecture_design" width="400"/>


- Lý do tách eventsim ra khỏi docker-compose vì em muốn để kafka luôn chạy trước rồi mới chạy eventsim thay vì để 2 service gắn với nhau để phù hợp với thực tế.

- Hiện tại đang script đang dedupe pipeline ngay bước ingest (lý do là dedupe record trùng hoàn toàn, còn record bị trùng về nghiệp vụ sẽ được xử lý khi transform bằng dbt)

# Bước 1: Login
gcloud auth application-default login
# Bước 2: Set project
gcloud config set project YOUR_PROJECT_ID
