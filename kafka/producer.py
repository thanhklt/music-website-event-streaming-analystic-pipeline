import json
import time
from pathlib import Path
from confluent_kafka import Producer

# Xác định đường dẫn thư mục data/ (tương thích dù chạy script từ bất kỳ đâu)
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"

# Cấu hình Producer
producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "client.id": "eventsim-file-producer",
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 20,
    "compression.type": "snappy"
})

sent_count = 0
failed_count = 0

def delivery_report(error, message):
    global sent_count, failed_count
    if error:
        failed_count += 1
        print(f"[ERROR] Delivery failed: {error}")
    else:
        sent_count += 1
        # In log mỗi 100 message để không làm tràn màn hình
        if sent_count % 100 == 0:
            print(f"[INFO] Delivered {sent_count} messages to {message.topic()} [partition {message.partition()}]")


def stream_file_to_kafka(file_name: str, topic_name: str, delay_seconds: float = 0.05):
    """
    Đọc từng dòng từ file trong data/ và đẩy vào Kafka topic tương ứng.
    """
    file_path = DATA_DIR / file_name
    if not file_path.exists():
        print(f"[WARN] File không tồn tại: {file_path}")
        return

    print(f"\n🚀 Bắt đầu stream dữ liệu từ '{file_path.name}' vào topic '{topic_name}'...")

    with open(file_path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                print(f"[WARN] Dòng {line_num} không phải JSON hợp lệ, bỏ qua.")
                continue

            # Lấy key: ưu tiên userId, nếu không có thì lấy sessionId, cuối cùng là None
            user_id = event.get("userId")
            session_id = event.get("sessionId")
            message_key = str(user_id) if user_id is not None and str(user_id) != "" else (str(session_id) if session_id else None)

            # Gửi vào Kafka
            producer.produce(
                topic=topic_name,
                key=message_key.encode("utf-8") if message_key else None,
                value=line.encode("utf-8"), # Dùng trực tiếp raw JSON string để tối ưu hiệu năng
                callback=delivery_report
            )

            # Phục vụ các callback nhận kết quả từ broker
            producer.poll(0)

            # Giả lập độ trễ giữa các event (ví dụ 0.05s = ~20 events/giây)
            if delay_seconds > 0:
                time.sleep(delay_seconds)

    print(f" Đã đọc xong file {file_name}. Đang đợi hoàn tất gửi các message còn lại...")
    producer.flush(10)
    print(f" Hoàn tất: Đã gửi thành công {sent_count} messages, thất bại: {failed_count}.")


if __name__ == "__main__":
    try:
        # 1. Stream file listen_events vào topic listen_events
        # (delay_seconds=0.01: khoảng 100 event/giây, đổi thành 0 nếu muốn bắn nhanh nhất)
        stream_file_to_kafka(
            file_name="auth_events",
            topic_name="auth_events",
            delay_seconds=0.02
        )

        # Bạn có thể gọi thêm các file khác nếu muốn:
        # stream_file_to_kafka("page_view_events", "page_view_events", delay_seconds=0.02)
        # stream_file_to_kafka("auth_events", "auth_events", delay_seconds=0.02)

    except KeyboardInterrupt:
        print("\n[STOP] Đã dừng producer bởi người dùng.")
    finally:
        producer.flush(10)
