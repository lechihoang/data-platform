# Hướng dẫn thiết kế NiFi Flow (Data Ingestion)

Tài liệu này ghi chú lại cách thiết kế luồng (Flow) tải dữ liệu trên Apache NiFi. 
Mục tiêu là xây dựng một Data Pipeline chuẩn Data Engineering, có khả năng tự động Scale-up để tải hàng trăm file song song thay vì phải tạo thủ công từng link một (Hardcode).

## 1. Kiến trúc Fan-out (Dynamic Scaling)

Thay vì thiết lập URL tĩnh trong processor `InvokeHTTP`, chúng ta sử dụng thiết kế **Fan-out**:
Sinh ra một danh sách các biến (tháng/năm) -> Cắt danh sách thành nhiều luồng nhỏ -> Gắn URL tự động cho từng luồng -> Tải file song song.

### Cấu trúc các Processor:
`ExecuteScript` ➡️ `UpdateAttribute` ➡️ `InvokeHTTP` ➡️ `PutS3Object`

### Bước 1: Sinh danh sách tháng động (ExecuteScript)
- **Nhiệm vụ:** Thay vì phải kéo 3 Node rườm rà (Tạo Text -> Cắt dòng -> Trích xuất biến), vì bạn đã là Data Engineer rành code, chúng ta dùng đúng 1 Node chạy mã Python để "đẻ" ra 10 FlowFile chứa sẵn biến `month`.
- **Cấu hình (Properties):**
  - `Script Engine`: `python`
  - `Script Body`: Copy và dán đoạn code Python cực kỳ quen thuộc này vào:
    ```python
    # Chỉ cần sửa 3 biến này khi muốn tải thời gian khác
    year = 2024
    start_month = 1
    end_month = 6
    
    for i in range(start_month, end_month + 1):
        m = "%d-%02d" % (year, i)
        ff = session.create() # Đẻ ra 1 FlowFile
        ff = session.putAttribute(ff, "month", m) # Gắn biến month (ví dụ: 2024-01)
        session.transfer(ff, REL_SUCCESS) # Chuyển đi
    ```
- **Lưu ý:** Node này cũng chỉ nên chạy 1 lần. Hãy chuột phải và chọn **Run Once** khi cần kích hoạt.

### Bước 2: Tạo URL động (UpdateAttribute)
- **Nhiệm vụ:** Sử dụng biến `month` vừa lấy được để động hóa URL và tên file.
- **Cấu hình (Properties):**
  - Nhấn nút `+` (Add Property):
    - `download_url` = `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${month}.parquet`
    - `filename` = `yellow_tripdata_${month}.parquet`

### Bước 3: Gọi API Tải File (InvokeHTTP)
- **Nhiệm vụ:** Tải file thực tế từ Internet.
- **Cấu hình (Properties):**
  - `HTTP URL`: `${download_url}` (Nó sẽ tự động điền URL tương ứng cho từng file).
  - `HTTP Method`: `GET`

### Bước 4: Đẩy vào Data Lakehouse (PutS3Object)
- **Nhiệm vụ:** Tải file Parquet vào hệ thống MinIO lưu trữ của dự án.
- **Cấu hình (Properties):**
  - `Object Key`: `bronze/${filename}`
  - `Bucket`: `lakehouse`

---

## 3. Quản lý Mật khẩu bảo mật (Không Hardcode AWS Key)

Trong NiFi Registry, vì lý do bảo mật, các thuộc tính nhạy cảm như `Access Key` và `Secret Key` sẽ không được lưu vào kho chứa (sẽ bị xóa trắng khi tải sang máy khác).
Thay vì gõ cứng mật khẩu vào `PutS3Object`, chúng ta sẽ cấu hình để NiFi tự đọc mật khẩu từ file `.env` của hệ thống Docker.

**Cách thực hiện:**
1. Chuột phải ra vùng trống trên Canvas -> Chọn **Configure** -> Chuyển sang tab **Controller Services**.
2. Bấm nút `+` và thêm service tên là: **`AWSCredentialsProviderControllerService`**.
3. Bấm vào icon ⚙️ (Edit), chuyển qua tab **Properties**.
4. Sửa property **`Use Default Credentials`** thành **`true`**.
5. Bấm `Apply`, sau đó bật icon tia sét ⚡ (Enable).
6. Quay trở lại processor `PutS3Object`, bỏ trống các ô Access Key/Secret Key. Tại mục **`AWS Credentials Provider service`**, chọn service vừa tạo ở trên.

Với thiết lập này, NiFi sẽ tự động bắt các biến môi trường (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) từ file `.env` do Docker cung cấp. Bạn sẽ không bao giờ phải gõ lại mật khẩu khi import luồng sang hệ thống mới.

---

## 4. Tải file Taxi Zone Lookup (Dữ liệu Dimension)

Trong bài toán NYC Taxi, ngoài dữ liệu chuyến đi (Trips), bạn bắt buộc phải tải thêm file danh mục các khu vực (Zone Lookup) để sau này dùng PySpark join dữ liệu.

Vì file này tĩnh (chỉ có 1 file duy nhất), bạn không cần dùng thiết kế Fan-out như trên. Hãy tạo một nhánh nhỏ, đơn giản ngay cạnh luồng chính:

### Cấu trúc nhánh con:
`GenerateFlowFile` ➡️ `InvokeHTTP` ➡️ `PutS3Object`

### Cấu hình chi tiết:
1. **GenerateFlowFile**: 
   - Không cần Custom Text, chỉ dùng để kích hoạt chạy 1 lần.
2. **InvokeHTTP**:
   - `HTTP URL`: `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`
   - `HTTP Method`: `GET`
3. **PutS3Object**:
   - `Object Key`: `taxi_zone_lookup.csv`
   - `Bucket`: Tên bucket của bạn (ví dụ: `raw-data`)
   - `AWS Credentials Provider service`: Chọn lại đúng cái Service đã tạo ở Phần 3 để khỏi phải nhập Key.
