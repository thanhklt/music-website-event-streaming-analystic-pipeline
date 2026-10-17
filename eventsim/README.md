## Eventsim

Eventsim is a program that generates event data to replicate page requests for a fake music web site (picture something like Spotify); the results look like real use data, but are totally fake. You can find the original repo [here](https://github.com/Interana/eventsim). My docker image is borrowed from [viirya's clone](https://github.com/viirya/eventsim) of it, as the original project has gone without maintenance for a few years now.

### Build và chạy

Chạy từ root repository, sau khi khởi động ba broker Kafka trong hạ tầng:

```powershell
docker compose --env-file .env -f .\infra\docker-compose.yml up -d --wait kafka-1 kafka-2 kafka-3
.\eventsim\eventsim_start.ps1
```

Với Bash trên Linux hoặc Git Bash (cần GNU `date`):

```bash
bash ./eventsim/eventsim_start.sh
```

Nếu đang ở thư mục `eventsim/`, dùng `./eventsim_start.ps1` hoặc
`bash ./eventsim_start.sh`. Script xác định build context theo vị trí của chính nó,
không theo thư mục làm việc hiện tại.

Mỗi script build image `events:1.0` bằng Dockerfile trong thư mục này (có dùng
build cache), sau đó gọi `docker run -d`. Không có Compose riêng cho Eventsim.
Dockerfile đóng gói `config/config.json` vào `/opt/eventsim/config/config.json`;
sửa config trên host sẽ được đưa vào image ở lần chạy script tiếp theo.
JAR đầu vào phải có tại `target/eventsim-assembly-2.0.jar`.

Cấu hình chạy mặc định:

| Tham số | Giá trị |
| --- | --- |
| Config | `-c config/config.json` |
| Số người dùng ban đầu | `--nusers 20000` |
| Khoảng thời gian | 00:00 UTC hôm nay → 00:00 UTC ngày mai |
| Chế độ | `--nocontinuous` — sinh dữ liệu theo batch, không chờ thời gian thực |
| Kafka | `kafka-1:9092,kafka-2:9092,kafka-3:9092` |
| Docker network | `eventsim-net` |
| Container | `eventsim-v2` |
| Output | `data/` tại root repository, mount vào `/opt/eventsim/output_data` |

JAR hiện tại **không hỗ trợ `--no-continuous`**; kiểm tra CLI trả về
`Unknown option 'no-continuous'`. Cờ được hỗ trợ và đã thống nhất dùng là
`--nocontinuous`. Container dùng `TZ=UTC`.

Hai script đọc biến môi trường của process, không tự nạp `.env`:

| Biến | Mặc định |
| --- | --- |
| `EVENTSIM_IMAGE` | `events:1.0` |
| `EVENTSIM_CONTAINER` | `eventsim-v2` |
| `EVENTSIM_KAFKA_NETWORK` | `eventsim-net` |
| `EVENTSIM_KAFKA_BOOTSTRAP_SERVERS` | Danh sách ba broker ở trên |
| `EVENTSIM_START_TIME` | 00:00 hôm nay UTC |
| `EVENTSIM_END_TIME` | 00:00 ngày mai UTC |

Ghi đè thời gian bằng định dạng `YYYY-MM-DDTHH:mm:ss`, hiểu là UTC;
thời điểm kết thúc phải sau thời điểm bắt đầu. Nên đặt cả hai khi chạy khoảng ngày khác:

```powershell
$env:EVENTSIM_START_TIME = '2026-10-17T00:00:00'
$env:EVENTSIM_END_TIME = '2026-10-18T00:00:00'
$env:EVENTSIM_CONTAINER = 'eventsim-batch-20261017'
.\eventsim\eventsim_start.ps1
```

```bash
EVENTSIM_START_TIME=2026-10-17T00:00:00 \
EVENTSIM_END_TIME=2026-10-18T00:00:00 \
EVENTSIM_CONTAINER=eventsim-batch-20261017 \
bash ./eventsim/eventsim_start.sh
```

Script chờ healthcheck của ba broker tối đa 60 giây, kiểm tra network và dừng nếu
build lỗi. Container cùng tên đã tồn tại sẽ được giữ nguyên; chọn tên mới bằng
`EVENTSIM_CONTAINER` để chạy batch mới. Chạy lại một container đã dừng bằng
`docker start` sẽ chạy lại khoảng thời gian cũ và có thể phát lại dữ liệu.

Theo dõi và dừng (thay tên nếu đã override):

```powershell
docker logs -f eventsim-v2
docker inspect eventsim-v2 --format '{{.State.Status}} {{.State.ExitCode}}'
docker stop eventsim-v2
```

Script chỉ kiểm tra trạng thái ban đầu sau khi tạo container. `running` chưa có
nghĩa batch đã hoàn tất hoặc Kafka đã nhận đủ dữ liệu; cần kiểm tra log và exit code
khi container kết thúc. Output và container không bị tự xóa.

### Data Profile

#### Event `auth`

Event ghi nhận một lần thử đăng nhập và kết quả của lần đăng nhập đó.

| Thuộc tính | Ý nghĩa | Kiểu dữ liệu/miền giá trị theo profile |
|---|---|---|
| `ts` | Thời điểm đăng nhập | Unix timestamp |
| `sessionId` | ID phiên của người dùng; profile ghi nhận phiên mới có thể được tạo sau 30 phút không tương tác | Số nguyên, tăng dần theo mô tả nguồn |
| `level` | Loại tài khoản của người dùng | `free`, `paid` |
| `itemInSession` | Thứ tự event trong phiên | Số nguyên, bắt đầu từ `0` |
| `city` | Thành phố của người dùng | Chuỗi |
| `zip` | ZIP code của thành phố | Chuỗi/định danh |
| `state` | Mã tiểu bang của người dùng | Mã tiểu bang 2 ký tự |
| `userAgent` | User-Agent của trình duyệt | Chuỗi |
| `lon` | Kinh độ | Số thực |
| `lat` | Vĩ độ | Số thực |
| `userId` | ID người dùng | Số nguyên/định danh |
| `lastName` | Họ người dùng | Chuỗi |
| `firstName` | Tên người dùng | Chuỗi |
| `gender` | Giới tính | `M`, `F` |
| `registration` | Thời điểm tạo tài khoản | Unix timestamp |
| `success` | Kết quả đăng nhập | Profile chưa nêu miền giá trị |

#### Event `listen`

Event ghi nhận một lần người dùng bắt đầu nghe một bài hát.

| Thuộc tính | Ý nghĩa | Kiểu dữ liệu/miền giá trị theo profile |
|---|---|---|
| `artist` | Tên nghệ sĩ | String |
| `song` | Tên bài hát | String |
| `duration` | Thời lượng bài hát | Float |
| `ts` | Thời điểm bắt đầu nghe | Unix timestamp |
| `sessionId` | ID phiên của người dùng | Int |
| `auth` | Trạng thái xác thực | String; profile ghi nhận event này luôn là `Logged In` |
| `level` | Loại tài khoản | `free`, `paid` |
| `itemInSession` | Thứ tự event trong phiên | Int |
| `city` | Thành phố của người dùng | String |
| `zip` | ZIP code | String |
| `state` | Mã tiểu bang | String, 2 ký tự |
| `userAgent` | User-Agent của trình duyệt | String |
| `lon` | Kinh độ | Float |
| `lat` | Vĩ độ | Float |
| `userId` | ID người dùng trong cơ sở dữ liệu | Int |
| `lastName` | Họ người dùng | String |
| `firstName` | Tên người dùng | String |
| `gender` | Giới tính | `M`, `F` |
| `registration` | Thời điểm tạo tài khoản | Unix timestamp |

#### Event `page_view`

Event ghi nhận một lần người dùng truy cập trang. Với trang `NextSong`, bản ghi có thể chứa thêm thông tin bài hát.

| Thuộc tính | Ý nghĩa | Kiểu dữ liệu/miền giá trị theo profile |
|---|---|---|
| `ts` | Thời điểm xảy ra event | Unix timestamp |
| `sessionId` | ID phiên hiện tại | Int |
| `page` | Trang người dùng đang xem | `Home`, `NextSong`, `Login`, `Logout` theo profile |
| `auth` | Trạng thái xác thực | String |
| `method` | HTTP method của request | `POST`, `PUT` theo profile |
| `status` | HTTP response status code | Int |
| `level` | Loại tài khoản | `free`, `paid` |
| `itemInSession` | Thứ tự event trong phiên | Int |
| `city` | Thành phố tại thời điểm phát sinh event | String |
| `zip` | ZIP code | String |
| `state` | Mã tiểu bang | String, 2 ký tự |
| `userAgent` | User-Agent tại thời điểm phát sinh event | String |
| `lon` | Kinh độ | Float |
| `lat` | Vĩ độ | Float |
| `userId` | ID người dùng trong cơ sở dữ liệu | Int |
| `lastName` | Họ người dùng | String |
| `firstName` | Tên người dùng | String |
| `gender` | Giới tính | `M`, `F` |
| `registration` | Thời điểm tạo tài khoản | Unix timestamp |
| `artist` | Tên nghệ sĩ khi trang là `NextSong` | String, có thể null ở trang khác |
| `song` | Tên bài hát khi trang là `NextSong` | String, có thể null ở trang khác |
| `duration` | Thời lượng bài hát khi trang là `NextSong` | Float, có thể null ở trang khác |

#### Event `status_change`

Event thể hiện trạng thái gói tài khoản trong luồng thay đổi gói. Theo ghi chú trong profile, một luồng nâng cấp có thể xuất hiện theo thứ tự: `status_change` với `level = free` → trang xác nhận gửi yêu cầu → `Home` với `level = paid`. Luồng hạ cấp diễn ra tương tự theo chiều ngược lại.

| Thuộc tính | Ý nghĩa | Kiểu dữ liệu/miền giá trị theo profile |
|---|---|---|
| `ts` | Thời điểm xảy ra event | Unix timestamp |
| `sessionId` | ID phiên hiện tại | Int |
| `auth` | Trạng thái xác thực | String; `Logged In` theo profile |
| `level` | Trạng thái gói tại thời điểm event | `free`, `paid` |
| `itemInSession` | Thứ tự event trong phiên | Int |
| `city` | Thành phố của người dùng | String |
| `zip` | ZIP code | String |
| `state` | Mã tiểu bang | String; cần kỳ vọng mã tiểu bang 2 ký tự |
| `userAgent` | User-Agent của trình duyệt | String |
| `lon` | Kinh độ | Float |
| `lat` | Vĩ độ | Float |
| `userId` | ID người dùng trong cơ sở dữ liệu | Int |
| `lastName` | Họ người dùng | String |
| `firstName` | Tên người dùng | String |
| `gender` | Giới tính | `M`, `F` |
| `registration` | Thời điểm tạo tài khoản | Unix timestamp |
