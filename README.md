# Messenger Bridge Telegram

Messenger Bridge Telegram là bridge hai chiều giữa Facebook Messenger và Telegram Forum Topics. Mỗi hội thoại Messenger được ánh xạ vào một topic riêng trong Telegram supergroup, giúp theo dõi nhiều cuộc trò chuyện ở một nơi mà vẫn giữ được ngữ cảnh, reply, reaction và trạng thái realtime.

Dự án dùng `fbchat-v2` để đăng nhập, lắng nghe và gửi tin Messenger, bao gồm cả luồng E2EE. Telegram được vận hành bằng `python-telegram-bot`.

## Nội dung

- [Tính năng](#tính-năng)
- [Kiến trúc](#kiến-trúc)
- [Yêu cầu](#yêu-cầu)
- [Cài đặt nhanh](#cài-đặt-nhanh)
- [Cài fbchat-v2](#cài-fbchat-v2)
- [Cài E2EE bridge](#cài-e2ee-bridge)
- [Cấu hình](#cấu-hình)
- [Chuẩn bị Telegram](#chuẩn-bị-telegram)
- [Chạy bridge](#chạy-bridge)
- [Lệnh Telegram](#lệnh-telegram)
- [Cách hoạt động](#cách-hoạt-động)
- [Trạng thái hỗ trợ](#trạng-thái-hỗ-trợ)
- [Vận hành và bảo mật](#vận-hành-và-bảo-mật)
- [Troubleshooting](#troubleshooting)

## Tính năng

- Bridge hai chiều Messenger <-> Telegram Forum Topics.
- Tự động tạo topic cho hội thoại Messenger mới.
- Hỗ trợ Messenger regular và Messenger E2EE qua `fbchat-v2`.
- Forward text, reply, reaction, edit, unsend và các activity realtime quan trọng.
- Upload ảnh Messenger trực tiếp lên Telegram thay vì chỉ gửi URL CDN.
- Forward Telegram text/caption/sticker sang Messenger theo mapping topic.
- Forward reaction Telegram sang reaction Messenger nếu tin Telegram có mapping.
- Dedupe reaction Messenger để tránh spam các message `reacted with ...` bị lặp.
- Chống lặp tin nhắn: tin bridge gửi từ Telegram không bị Messenger echo ngược lại vào topic, tin Messenger nhận lại lần hai cũng bị bỏ qua.
- Typing và read receipt có thể bật/tắt bằng `.env`; khi bật sẽ tự xoá sau thời gian ngắn.
- Tự phục hồi khi Telegram topic bị xoá/đóng hoặc mapping bị stale.
- Lưu mapping topic/message trong `data/bridge-store.json` để reply và activity bám đúng tin gốc.

## Kiến trúc

```text
Facebook Messenger
  -> fbchat-v2 listener / E2EE Go bridge
  -> src/messenger/client.py
  -> src/bridge.py
  -> python-telegram-bot
  -> Telegram supergroup forum topics
```

| File | Vai trò |
|---|---|
| `src/main.py` | Entry point, load config, logging và Telegram polling. |
| `src/config.py` | Đọc `.env` và cấu hình package/binary E2EE. |
| `src/messenger/client.py` | Quản lý async lifecycle của `fbchat-v2`, gửi text/reaction/sticker/media. |
| `src/messenger/events.py` | Chuẩn hoá raw event Messenger thành model nội bộ. |
| `src/bridge.py` | Điều phối hai chiều, topic mapping, recovery, retry, upload ảnh, activity. |
| `src/store.py` | Lưu topic/message/quote mapping vào JSON. |
| `src/tg/handlers.py` | Đăng ký lệnh Telegram, message handler và reaction handler. |
| `src/utils/formatting.py` | Format HTML cho tin nhắn/activity Telegram. |

## Yêu cầu

- Windows, Linux hoặc macOS.
- Python 3.10 trở lên. Workspace này đã chạy với Python 3.13.
- Telegram bot token từ BotFather.
- Telegram supergroup đã bật Topics.
- Bot Telegram là admin và có quyền Manage Topics, gửi tin nhắn, gửi media.
- Cookie Facebook của tài khoản Messenger dùng để bridge.
- `fbchat-v2` 2.3.x được cài từ PyPI.
- Kết nối Internet ở lần chạy E2EE đầu tiên để package tải binary đúng phiên bản trên nền tảng được hỗ trợ.

## Cài đặt nhanh

Trong PowerShell:

```powershell
cd ...
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Mở `.env` và điền tối thiểu:

```env
TG_TOKEN=1234567890:replace_me
TG_GROUP_ID=-1001234567890
FACEBOOK_COOKIE=c_user=...; xs=...; fr=...; datr=...;
DATA_DIR=./data
```

`requirements.txt` đã cài luôn `fbchat-v2` 2.3.x; không cần clone hay thêm source local vào `PYTHONPATH`.

## Cài fbchat-v2

Bridge chỉ dùng package [`fbchat-v2` phát hành trên PyPI](https://pypi.org/project/fbchat-v2/) từ [repository chính thức](https://github.com/m008v/fbchat-v2). Version được giới hạn trong nhánh `2.3.x` để tránh tự nuốt breaking change của minor tiếp theo.

```powershell
cd ...
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade -r requirements.txt
```

Kiểm tra version và đường dẫn import:

```powershell
python -c "import fbchat_v2; print(fbchat_v2.__version__); print(fbchat_v2.__file__)"
```

Đường dẫn phải nằm trong `site-packages/fbchat_v2`, không được trỏ sang folder `fbchat-v2/src` cạnh repository này.

## Cài E2EE bridge

E2EE có hai phần:

- Python wrapper nằm trong `fbchat-v2`.
- Go binary `fbchat-bridge-e2ee.exe` xử lý Messenger E2EE, chạy như subprocess qua JSON-RPC.

`fbchat-v2` 2.3.x tự tải asset release phù hợp cho Windows x64, Linux x64/ARM64 và macOS x64/ARM64, rồi kiểm tra version, kích thước và SHA-256 trước khi chạy. Vì vậy cấu hình mặc định không cần source Go, Git hay `FBCHAT_E2EE_BIN`.

Cấu hình E2EE trong `.env`:

```env
FBCHAT_ENABLE_E2EE=1
FBCHAT_E2EE_MEMORY_ONLY=1
FBCHAT_E2EE_LOG_LEVEL=none
FBCHAT_E2EE_READY_TIMEOUT=90
FBCHAT_E2EE_SEND_TIMEOUT=180
```

Chỉ đặt `FBCHAT_E2EE_BIN` khi chủ động quản lý một binary tương thích đúng protocol của package:

```env
FBCHAT_E2EE_BIN=C:/path/to/fbchat-bridge-e2ee.exe
```

Nếu muốn lưu device/key E2EE xuống file thay vì chỉ giữ trong RAM:

```env
FBCHAT_E2EE_MEMORY_ONLY=0
FBCHAT_E2EE_DEVICE_PATH=./data/e2ee-device.json
```

Nếu chỉ cần Messenger regular và muốn tắt E2EE:

```env
FBCHAT_ENABLE_E2EE=0
```

## Cấu hình

`.env` được đọc từ thư mục `Messenger-Bridge-Telegram`. Không commit file này vì chứa token, cookie và có thể chứa key E2EE.

| Biến | Bắt buộc | Mặc định | Mô tả |
|---|---:|---|---|
| `TG_TOKEN` | Có | - | Token bot Telegram từ BotFather. |
| `TG_GROUP_ID` | Có | - | ID supergroup Telegram, thường có dạng `-100...`. |
| `FACEBOOK_COOKIE` | Có* | - | Cookie Facebook của tài khoản Messenger. |
| `FACEBOOK_COOKIE_FILE` | Có* | - | File chứa cookie, dùng thay cho `FACEBOOK_COOKIE`. |
| `LOG_LEVEL` | Không | `DEBUG` | Log terminal. Dùng `INFO` nếu muốn gọn hơn. |
| `DATA_DIR` | Không | `./data` | Nơi lưu `bridge-store.json`. |
| `FBCHAT_E2EE_BIN` | Không | package tự quản lý | Đường dẫn override tới binary tương thích; thường nên để trống. |
| `FBCHAT_ENABLE_E2EE` | Không | `1` | Bật/tắt listener/send E2EE. |
| `FBCHAT_E2EE_MEMORY_ONLY` | Không | `1` | Giữ device/key E2EE trong RAM. |
| `FBCHAT_E2EE_DEVICE_PATH` | Không | - | File lưu device/key khi `FBCHAT_E2EE_MEMORY_ONLY=0`. |
| `FBCHAT_E2EE_LOG_LEVEL` | Không | `none` | Log level của E2EE bridge. |
| `FBCHAT_E2EE_READY_TIMEOUT` | Không | `90` | Thời gian chờ listener và E2EE handshake sẵn sàng khi khởi động. |
| `FBCHAT_E2EE_SEND_TIMEOUT` | Không | `180` | Timeout cho RPC E2EE media/reaction, tính bằng giây. |
| `IGNORE_SELF_MESSAGES` | Không | `1` | Bỏ qua tin do chính tài khoản Facebook bridge gửi. Đặt `0` để sync cả tin bạn gửi từ app Messenger khác (điện thoại, web); tin do bridge gửi từ Telegram vẫn được lọc trùng nên không bị lặp. |
| `MESSAGE_CACHE_LIMIT` | Không | `3000` | Số mapping message giữ lại để reply/reaction/activity. |
| `TG_CONNECT_TIMEOUT` | Không | `15` | Timeout kết nối Telegram Bot API. |
| `TG_READ_TIMEOUT` | Không | `45` | Timeout đọc response Telegram Bot API. |
| `TG_WRITE_TIMEOUT` | Không | `45` | Timeout gửi request Telegram Bot API. |
| `TG_POOL_TIMEOUT` | Không | `30` | Timeout chờ connection pool Telegram. |
| `FORWARD_MESSENGER_REACTIONS` | Không | `1` | Forward reaction Messenger sang Telegram, có dedupe chống spam. |
| `FORWARD_TYPING_ACTIVITY` | Không | `0` | Forward typing indicator Messenger sang Telegram. |
| `FORWARD_READ_RECEIPTS` | Không | `0` | Forward read receipt Messenger sang Telegram. |

`FACEBOOK_COOKIE` và `FACEBOOK_COOKIE_FILE` là hai cách thay thế nhau; chỉ cần dùng một.

## Chuẩn bị Telegram

1. Tạo bot bằng BotFather và lấy token.
2. Tạo Telegram group rồi convert thành supergroup nếu cần.
3. Bật Topics trong group settings.
4. Thêm bot vào group.
5. Promote bot thành admin.
6. Bật quyền Manage Topics, gửi tin nhắn và gửi media.
7. Lấy group ID dạng `-100...` và điền vào `TG_GROUP_ID`.
8. Sau khi chạy bot, dùng `/checktopics` trong group để kiểm tra quyền.

## Chạy bridge

```powershell
cd ...
.\.venv\Scripts\Activate.ps1
python .\src\main.py
```

Khi khởi động thành công, bot gửi thông báo vào Telegram group. Khi có hội thoại Messenger mới, bridge tạo topic mới và bắt đầu forward tin/activity vào topic đó.

## Lệnh Telegram

| Lệnh | Mô tả |
|---|---|
| `/help` | Hiển thị hướng dẫn cơ bản. |
| `/status` | Xem listener status, Facebook ID và số topic đã map. |
| `/checktopics` | Kiểm tra supergroup, Topics và quyền Manage Topics. |
| `/topic list` | Liệt kê các topic đã map. |
| `/topic info` | Xem mapping của topic hiện tại. |
| `/topic delete` | Xoá mapping của topic hiện tại. |

Tin Telegram chỉ được forward về Messenger khi gửi trong topic đã có mapping.

## Cách hoạt động

### Messenger sang Telegram

1. `fbchat-v2` listener nhận event Messenger.
2. `src/messenger/events.py` parse thành `IncomingMessengerMessage` hoặc `IncomingMessengerActivity`.
3. Bridge tìm topic theo `transport + messenger_id`.
4. Nếu chưa có mapping, bot tạo topic mới.
5. Tin nhắn được gửi vào topic. Nếu event có reply mapping, Telegram message sẽ reply đúng tin gốc.
6. Attachment ảnh được tải xuống và upload lên Telegram. Nếu tải/upload lỗi, bridge fallback bằng link.
7. Reaction/edit/unsend/read/typing được gửi như activity theo cấu hình `.env`.

### Telegram sang Messenger

1. Người dùng gửi tin trong topic đã map.
2. Bridge lấy topic mapping để biết Messenger thread/chat JID đích.
3. Text/caption được gửi sang Messenger.
4. Sticker Telegram được tải từ Telegram rồi gửi sang Messenger best-effort.
5. Reaction trên Telegram được gửi sang Messenger nếu tin Telegram có quote mapping.
6. Message ID trả về từ Messenger được lưu để reply/reaction/activity sau này bám đúng tin gốc.

## Trạng thái hỗ trợ

| Chức năng | Trạng thái |
|---|---|
| Messenger text sang Telegram | Hỗ trợ |
| Telegram text/caption sang Messenger | Hỗ trợ |
| Messenger reply sang Telegram reply | Hỗ trợ nếu có mapping |
| Telegram reply sang Messenger reply | Hỗ trợ nếu có mapping |
| Messenger image sang Telegram | Upload trực tiếp ảnh lên Telegram, fallback link nếu lỗi |
| Messenger link/file/video sang Telegram | Hiển thị link/mô tả theo dữ liệu event |
| Messenger reaction/edit/unsend sang Telegram | Hỗ trợ, reaction có dedupe |
| Telegram reaction sang Messenger | Hỗ trợ nếu tin Telegram có mapping |
| Messenger typing/read receipt sang Telegram | Có thể bật; typing/read tự xoá sau thời gian ngắn |
| Telegram sticker sang Messenger | Hỗ trợ best-effort |
| Telegram photo/video/file sang Messenger | Chưa phải luồng chính; hiện ưu tiên text/caption/sticker |
| E2EE text/reaction/sticker/media | Hỗ trợ theo khả năng binary E2EE hiện có |

## Dữ liệu lưu trữ

Runtime data mặc định nằm trong:

```text
data/bridge-store.json
```

File này chứa:

- Mapping Telegram topic sang Messenger conversation.
- Mapping Messenger message ID sang Telegram message ID.
- Quote data để gửi reply và route reaction/edit/unsend.

Không nên xoá file này khi bridge đang chạy. Nếu xoá, bot vẫn chạy nhưng mất mapping cũ và có thể tạo topic mới cho các hội thoại đã từng map.

## Vận hành và bảo mật

- Không commit `.env`, cookie Facebook, thư mục `data/` hoặc file E2EE device/key.
- Nên dùng tài khoản Facebook phụ cho bridge vì Messenger có thể checkpoint session khi dùng API không chính thức.
- Cookie Facebook hết hạn hoặc checkpoint sẽ làm listener/send thất bại; cần cập nhật cookie mới.
- Telegram topic ID có thể stale nếu topic bị xoá/đóng; bridge có cơ chế tạo topic mới và retry.
- Typing/read receipt có tần suất cao. Chỉ bật khi thật sự cần realtime status.
- Read receipt tự xoá sau 5 giây. Typing tự xoá khi ngưng nhập, có tin nhắn mới hoặc hết TTL.
- Chỉ set `FBCHAT_E2EE_BIN` khi có binary tương thích đã được quản lý riêng; mặc định nên để package tự chọn.
- Sau khi đổi `.env`, luôn restart bot để config mới có hiệu lực.

## Troubleshooting

### `Missing required environment variable: TG_TOKEN`

Kiểm tra `.env` đã tồn tại trong thư mục `Messenger-Bridge-Telegram` và đã điền `TG_TOKEN`.

### `TG_GROUP_ID` sai hoặc bot không phản hồi

Đảm bảo group ID có dạng `-100...`, bot đã được thêm vào đúng group và bot có quyền gửi tin nhắn.

### Bot không tạo topic mới

Chạy `/checktopics`. Group phải là supergroup, đã bật Topics, bot phải là admin và có quyền Manage Topics.

### `ModuleNotFoundError: No module named 'fbchat_v2'`

Đảm bảo đang dùng đúng virtual environment rồi cài dependency từ PyPI:

```powershell
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade -r requirements.txt
python -c "import fbchat_v2; print(fbchat_v2.__version__); print(fbchat_v2.__file__)"
```

### Không tìm thấy `fbchat-bridge-e2ee.exe`

Để trống `FBCHAT_E2EE_BIN` và kiểm tra máy có thể tải GitHub Release asset. Package 2.3.x sẽ tự chọn, tải và xác minh binary. Nếu đang đặt override thủ công, bỏ giá trị cũ hoặc thay bằng binary tương thích đúng version.

### E2EE không nhận/gửi được tin

- Kiểm tra `FBCHAT_ENABLE_E2EE=1`.
- Để `FBCHAT_E2EE_BIN` trống, trừ khi đang chủ động quản lý binary tương thích.
- Kiểm tra mạng có truy cập được GitHub Release ở lần tải đầu.
- Nếu vừa nâng package/binary, restart bot.
- Nếu vẫn timeout, tăng `FBCHAT_E2EE_SEND_TIMEOUT` hoặc restart để tạo lại bridge process.

### Topic hiện ID hoặc tên người gửi thay vì tên nhóm

Bridge ưu tiên `threadName` từ event và sẽ rename topic khi lấy được tên thật. Nếu topic cũ bị đặt sai, chờ tin nhắn mới trong nhóm hoặc dùng `/topic delete` trong topic đó để xoá mapping và tạo lại.

### Không thấy typing hoặc read receipt

Kiểm tra `.env`:

```env
FORWARD_TYPING_ACTIVITY=1
FORWARD_READ_RECEIPTS=1
```

Sau đó restart bot. Read receipt tự xoá sau 5 giây nên chỉ hiện ngắn.

### Reaction Messenger bị spam

Bridge đã dedupe reaction theo conversation, actor và message. Nếu muốn tắt hẳn reaction activity từ Messenger sang Telegram:

```env
FORWARD_MESSENGER_REACTIONS=0
```

### Reaction Telegram không cập nhật sang Messenger

Chỉ những tin Telegram có mapping trong `bridge-store.json` mới gửi reaction ngược sang Messenger. Bot cũng phải chạy với polling `message_reaction`, đã cấu hình trong `src/main.py`.

### Telegram báo timeout liên tục

Giữ typing/read receipt ở mức cần thiết và tăng timeout:

```env
TG_CONNECT_TIMEOUT=15
TG_READ_TIMEOUT=60
TG_WRITE_TIMEOUT=60
TG_POOL_TIMEOUT=45
```

### Muốn xem log chi tiết

```env
LOG_LEVEL=DEBUG
```

Nếu log quá nhiều:

```env
LOG_LEVEL=INFO
```

## Kiểm tra nhanh

Compile Python:

```powershell
cd ...
python -m compileall src
```

Kiểm tra package PyPI và async contract:

```powershell
python -c "import inspect, fbchat_v2; from fbchat_v2 import dataGetHome, listeningE2EEEvent; print(fbchat_v2.__version__, fbchat_v2.__file__); assert inspect.iscoroutinefunction(dataGetHome); assert inspect.iscoroutinefunction(listeningE2EEEvent.connect_mqtt)"
```

## Giới hạn hiện tại

- Dự án dùng API/behavior không chính thức của Facebook Messenger nên có thể bị ảnh hưởng khi Meta thay đổi giao thức.
- E2EE cần binary Go và session/key hợp lệ; lần đầu connect có thể mất thời gian.
- Media hai chiều chưa đồng đều tuyệt đối: Messenger image đã upload sang Telegram, Telegram sticker đã gửi sang Messenger best-effort, còn video/file nâng cao vẫn phụ thuộc dữ liệu event và khả năng transport.
- Reaction custom emoji của Telegram có thể không tương thích Messenger; emoji phổ biến được gửi best-effort.
