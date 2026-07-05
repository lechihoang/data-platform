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
