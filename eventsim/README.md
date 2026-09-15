## Eventsim

Eventsim is a program that generates event data to replicate page requests for a fake music web site (picture something like Spotify); the results look like real use data, but are totally fake. You can find the original repo [here](https://github.com/Interana/eventsim). My docker image is borrowed from [viirya's clone](https://github.com/viirya/eventsim) of it, as the original project has gone without maintenance for a few years now.

### Setup

#### Docker Image
```bash
docker build -t events:1.0 .
```

#### Run With Kafka Configured On Localhost
```bash
docker run -it \
  --network host \
  events:1.0 \
    -c "examples/example-config.json" \
    --start-time "`date +"%Y-%m-%dT%H:%M:%S"`" \
    --end-time "2022-03-18T17:00:00" --nusers 20000 \
    --kafkaBrokerList localhost:9092 \
    --continuous
```

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
