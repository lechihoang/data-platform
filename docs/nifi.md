# Hướng dẫn thiết kế NiFi Flow (Phiên bản Đơn giản - 1 DAG)

Tài liệu này hướng dẫn cách thiết lập một luồng (Flow/DAG) duy nhất, tối giản trên NiFi để tải đúng 2 file:
1. Dữ liệu chuyến đi tháng 01/2024 (`yellow_tripdata_2024-01.parquet`)
2. Dữ liệu khu vực (`taxi_zone_lookup.csv`)

Vì không cần tải hàng loạt nhiều tháng, ta bỏ qua kiến trúc Fan-out (không cần code Python) và gắn trực tiếp (hardcode) URL để Flow ngắn gọn, dễ cấu hình nhất.

## 1. Cấu hình bảo mật AWS (Làm trước tiên)

Bạn vẫn nên dùng Controller Service để NiFi đọc thông tin kết nối MinIO từ file `.env`, tránh việc phải gõ Access Key thủ công:

1. Chuột phải ra vùng trống trên Canvas -> Chọn **Configure** -> Tab **Controller Services**.
2. Thêm mới service: **`AWSCredentialsProviderControllerService`**.
3. Edit (⚙️) -> Properties: 
   - **`Access Key ID`**: `admin`
   - **`Secret Access Key`**: `admin123`
4. Bấm Apply và bật biểu tượng tia sét ⚡ (Enable).

---

## 2. Thiết kế Luồng (DAG) - Kiến trúc Enterprise (Configuration-Driven)

Với kiến trúc này, toàn bộ link API và cấu hình được lưu ở file ngoài. NiFi chỉ làm nhiệm vụ Động cơ xử lý. Rất dễ mở rộng sau này.

### Bước 1: Chuẩn bị file cấu hình JSON
File cấu hình đã được tạo sẵn tại `infra/nifi/config/ingestion_sources.json` và đã được mount vào NiFi tại thư mục `/opt/nifi/nifi-current/config_data/ingestion_sources.json`.
File này chứa một mảng JSON với thông tin: `url`, `bucket`, và `object_key` của từng file.

### Bước 2: Kéo thả và Cấu hình 6 Hộp (Processors)

1. **`GenerateFlowFile`** (Bộ lập lịch - Trigger):
   - Cấu hình (Properties) -> `Custom Text`: Tùy ý (gõ "start" hoặc để trống).
   - Thêm 2 thuộc tính (Bấm dấu `+` ở góc trên bên phải):
     - Property Name: `absolute.path` -> Value: `/opt/nifi/nifi-current/config_data/`
     - Property Name: `filename` -> Value: `ingestion_sources.json`
   - Tab **Scheduling** -> Cứ để mặc định là `Timer driven`. Khi nào cần chạy, bạn chỉ cần ra ngoài màn hình, **click chuột phải vào hộp này và chọn Run Once**.
   - Nối dây `success` sang hộp số 2.

2. **`FetchFile`** (Đọc file cấu hình):
   - Cấu hình (Properties) -> `File to Fetch`: Cứ để nguyên mặc định là `${absolute.path}/${filename}` (Nó sẽ tự động nhận giá trị từ hộp số 1 truyền sang).
   - `Completion Strategy`: `None` (Quan trọng: Không xóa file).
   - Nối dây `success` sang hộp số 3.

3. **`SplitJSON`** (Tách danh sách thành nhiều luồng):
   - Cấu hình (Properties) -> `JsonPath Expression`: `$.*` (Tách mảng JSON thành từng object riêng lẻ).
   - Nối dây `split` sang hộp số 4. Các dây `original`, `failure` cấu hình Auto-terminate (Tích vào cột `terminate`).

4. **`EvaluateJsonPath`** (Rút trích URL và S3 Key):
   - Cấu hình (Properties) -> `Destination`: `flowfile-attribute` (Lưu thành biến).
   - Thêm Property mới (Dấu +): 
     - Tên: `api_url` -> Value: `$.url`
     - Tên: `s3.bucket` -> Value: `$.bucket`
     - Tên: `filename` -> Value: `$.object_key`
   - Nối dây `matched` sang hộp số 5. Dây `unmatched`, `failure` cấu hình Auto-terminate.

5. **`InvokeHTTP`** (Gọi API kéo Data):
   - Cấu hình (Properties) -> `HTTP URL`: `${api_url}`
   - `HTTP Method`: `GET`
   - Nối dây `Response` sang hộp số 6. Các dây còn lại Auto-terminate.

6. **`PutS3Object`** (Lưu vào MinIO):
   - Cấu hình (Properties) -> `Object Key`: Cứ để mặc định là `${filename}`
   - `Bucket`: Điền `${s3.bucket}`
   - `AWS Credentials Provider service`: Chọn service đã tạo ở phần 1.
   - `Endpoint Override URL`: `http://minio:9000` (Quan trọng để trỏ về MinIO thay vì AWS thật)
   - `Signer Override`: `AWSS3V4SignerType`
   - Auto-terminate: Tích cột `terminate` cho `success` và `failure`.

---
**Tóm tắt:** Bất cứ khi nào bạn muốn kéo thêm API mới, bạn chỉ cần mở file `ingestion_sources.json` thêm 1 dòng. Đến đúng giờ (Cron), NiFi sẽ tự động đọc file và tạo ra ngần ấy luồng tải song song!

---
## 3. Nâng cấp: Tối ưu hóa - Bỏ qua (Skip) file đã tồn tại trên MinIO (Native Method)

Để tránh lãng phí băng thông tải lại những file dung lượng lớn khi chạy luồng nhiều lần, bạn có thể thiết lập cơ chế kiểm tra trước trên MinIO. Để tiết kiệm CPU tuyệt đối (không cần gọi lệnh OS bên ngoài), ta sẽ dùng chính client mạng nội tại của NiFi để gửi request dạng `HEAD` (chỉ lấy siêu dữ liệu, không lấy data).

**Cách thực hiện (Thêm vào giữa Bước 4 và Bước 5 ở trên):**
Sau hộp số 4 (`EvaluateJsonPath`), thay vì nối thẳng sang `InvokeHTTP` kéo data, hãy chèn thêm hộp sau:

1. **`InvokeHTTP`** (Đóng vai trò làm `Check S3 Object`):
   - Cấu hình (Properties):
     - `HTTP Method`: **`HEAD`** (Quan trọng: Phải dùng HEAD, không dùng GET)
     - `HTTP URL`: `http://minio:9000/${s3.bucket}/${filename}`
   - Hộp này sẽ hỏi MinIO xem file có tồn tại không. Kết quả trả về sẽ nằm trong thuộc tính `invokehttp.status.code`.
   - Nối dây `Response` sang hộp điều hướng. Các dây còn lại Auto-terminate.

2. **`RouteOnAttribute`** (Rẽ nhánh luồng dữ liệu):
   - Cấu hình (Properties):
     - Thêm 2 thuộc tính mới (Dấu +):
       - `file_exists` -> Value: `${invokehttp.status.code:equals('200')}`
       - `file_not_found` -> Value: `${invokehttp.status.code:equals('404')}`
   - Điều hướng dây:
     - Dây `file_not_found` (chưa có trên MinIO): Nối vào hộp số 5 (`InvokeHTTP` - Method GET) ban đầu để bắt đầu tải data về.
     - Dây `file_exists` (đã có sẵn): Auto-terminate (hoặc nối vào LogMessage để ghi log "Đã bỏ qua").

Bằng cách này, NiFi kiểm tra trạng thái cực kỳ nhẹ nhàng qua mạng nội bộ, có thể đạt hàng ngàn file mỗi giây mà không gây tốn CPU cho máy chủ!

---
## 4. Nâng cấp: Tối ưu hóa - Tăng tốc độ tải file (Performance Tuning)

Khi làm việc với các file Parquet dữ liệu taxi lớn (vài trăm MB đến cả GB), cấu hình mặc định của NiFi sẽ tải tuần tự và khá chậm. Để "ép xung" tốc độ, bạn cần cấu hình lại một vài thông số ở các Processor cốt lõi:

### 4.1. Tăng xử lý song song (Concurrent Tasks)
Mặc định mỗi Processor chỉ chạy 1 luồng (1 Thread). Nghĩa là dù bạn có 10 link API, nó vẫn tải từng cái một.
- Chuột phải vào **`InvokeHTTP`** và **`PutS3Object`** -> Tab **`Scheduling`**.
- Sửa mục **`Concurrent Tasks`** từ `1` lên `4` hoặc `6` (Tùy số lượng CPU core đang cấp cho Docker).
- **Kết quả:** NiFi sẽ bung ra 4-6 kết nối mạng tải và lưu file song song, thời gian tải rút ngắn nhiều lần.

### 4.2. Tối ưu bộ nhớ với Multipart Upload
Nếu tải file quá lớn, luồng dữ liệu truyền qua mạng có thể làm tràn RAM (OOM) của container NiFi.
- Tại hộp **`PutS3Object`**, kiểm tra Properties:
- `Multipart Threshold`: Chỉnh thành `50 MB` (Mặc định là 5GB).
- `Multipart Part Size`: Chỉnh thành `50 MB`.
- **Kết quả:** Vừa kéo data từ `InvokeHTTP` về, NiFi sẽ vừa cắt khúc (chunk) 50MB và ném thẳng lên MinIO thay vì phải ôm trọn toàn bộ file 1GB trong RAM.

### 4.3. Nới lỏng Timeout để tránh đứt gánh giữa đường
Với các nguồn tải chậm (như API công cộng), tải được 99% mà bị quá giờ (timeout) thì sẽ phải tải lại từ đầu.
- Tại hộp **`InvokeHTTP`**, chỉnh lại:
- `Read Timeout`: Tăng lên `15 mins` (hoặc cao hơn).
- `Connection Timeout`: Tăng lên `1 mins`.
- Đảm bảo tải các file nặng không bị ngắt kết nối oan uổng.
