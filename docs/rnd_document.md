# Tài Liệu Nghiên Cứu Và Phát Triển
**Công nghệ:** Apache NiFi, MinIO, Apache Spark, Apache Iceberg, Project Nessie

## Mục lục
- [I. Giới thiệu](#i-giới-thiệu)
- [II. Triển khai](#ii-triển-khai)
  - [1. Cấu trúc thư mục tổng quan](#1-cấu-trúc-thư-mục-tổng-quan)
  - [2. Triển khai các dịch vụ với Docker Compose](#2-triển-khai-các-dịch-vụ-với-docker-compose)
  - [3. Chi tiết quá trình Data Ingestion & Data Processing](#3-chi-tiết-quá-trình-data-ingestion--data-processing)
    - [3.1. Quá trình Data Ingestion (Apache NiFi)](#31-quá-trình-data-ingestion-apache-nifi)
    - [3.2. Quá trình Data Processing (Apache Spark, Iceberg & Nessie)](#32-quá-trình-data-processing-apache-spark-iceberg--nessie)
    - [3.3. Kiểm tra chất lượng (Data Quality) và Hợp nhất (Merge)](#33-kiểm-tra-chất-lượng-data-quality-và-hợp-nhất-merge)
- [III. Kết quả](#iii-kết-quả)

## I. Giới thiệu

Tài liệu này trình bày việc nghiên cứu vai trò của các công nghệ sau:
- **Apache NiFi:** Đóng vai trò là công cụ thu thập và tích hợp dữ liệu, chịu trách nhiệm lấy và tự động tải dữ liệu thô từ các nguồn bên ngoài vào kho lưu trữ.
- **MinIO:** Hoạt động như một hệ thống lưu trữ đối tượng (tương thích S3), cung cấp khả năng lưu trữ phân tán cho cả dữ liệu thô và dữ liệu đã qua xử lý.
- **Apache Spark:** Đóng vai trò là công cụ xử lý dữ liệu lõi, thực hiện các tác vụ trích xuất, biến đổi và tải (ETL) để làm sạch, biến đổi và tổng hợp dữ liệu. Điểm mạnh của Spark là tốc độ xử lý vượt trội nhờ khả năng tính toán trực tiếp trên bộ nhớ (in-memory), tính linh hoạt khi hỗ trợ đa ngôn ngữ (Python, Scala, SQL) và khả năng chịu lỗi cao.
- **Apache Iceberg:** Đóng vai trò là định dạng bảng dữ liệu hiệu suất cao, mang lại các tính năng giao dịch an toàn (như cập nhật, xóa, truy vấn theo thời gian) cho dữ liệu phi cấu trúc.
- **Project Nessie:** Đóng vai trò quản lý và lưu trữ siêu dữ liệu (metadata) cấp toàn hệ thống, hỗ trợ phân chia và truy vấn dữ liệu theo phiên bản. Công nghệ này cho phép kiểm soát vòng đời dữ liệu bằng các thao tác rẽ nhánh (branch), hợp nhất (merge) và hoàn tác (rollback) độc lập, tương tự như mã nguồn Git.

Mục đích chính của tài liệu là xây dựng một **giải pháp quản lý và kiểm soát chất lượng dữ liệu (CI/CD cho Dữ liệu)** thông qua mẫu thiết kế **Write-Audit-Publish (WAP)**. Cụ thể, giải pháp này giải quyết triệt để vấn đề dữ liệu lỗi vô tình bị đưa lên hệ thống báo cáo (BI) bằng một quy trình 3 bước:
1. **Viết/Chỉnh sửa (Write):** Mọi tác vụ tải và biến đổi dữ liệu (ETL) bằng Spark không ghi đè trực tiếp lên dữ liệu gốc, mà được thực hiện trên một nhánh rẽ (branch) ẩn hoàn toàn độc lập.
2. **Kiểm định (Audit):** Tự động chạy các kịch bản kiểm tra chất lượng (Data Quality Checks) trên nhánh ẩn này để quét các dòng dữ liệu rỗng, sai định dạng hoặc vi phạm logic nghiệp vụ.
3. **Phát hành (Publish):** Chỉ khi dữ liệu trên nhánh phụ vượt qua toàn bộ khâu kiểm định, hệ thống mới hợp nhất (merge) nhánh này vào nhánh chính (main). Người dùng cuối ngay lập tức truy cập được dữ liệu sạch mà không gặp tình trạng gián đoạn (zero-downtime).

Từ đó, tài liệu này đóng vai trò là kim chỉ nam hướng dẫn cấu hình thực tiễn để thiết lập quy trình WAP, biến nền tảng Data Lakehouse trở thành một hệ thống cực kỳ an toàn và đáng tin cậy.

## II. Triển khai

### 1. Cấu trúc thư mục tổng quan

Dự án được tổ chức thành các thư mục rõ ràng:

```text
nyc-taxi-lakehouse/
├── data/               # Nơi chứa raw dump data
├── docs/               # Tài liệu dự án
├── infra/              # Cấu hình hạ tầng Docker
│   ├── dagster/        # Orchestration pipeline với Dagster
│   ├── jupyter/        # Cấu hình Jupyter Notebook
│   ├── minio/          # MinIO custom image
│   ├── nessie/         # Nessie custom image
│   ├── nifi/           # NiFi & Registry custom image, flow, database
│   └── spark/          # Cấu hình Spark (Master & Worker)
├── notebook/           # Chứa file Jupyter Notebook
├── docker-compose.yml  # Triển khai toàn bộ cụm
├── .env                # Các biến môi trường
└── README.md
```

### 2. Triển khai các dịch vụ với Docker Compose

Tất cả các thành phần được đóng gói dưới dạng container và được quản lý bằng Docker Compose. Nội dung cấu hình các biến môi trường tại file `.env` như sau:

```env
# MinIO Credentials
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=admin123

# Nessie Configuration
NESSIE_CATALOG_DEFAULT_WAREHOUSE=s3://lakehouse
NESSIE_CATALOG_SERVICE_ICEBERG_ENABLED=true
QUARKUS_LOG_LEVEL=INFO

# NiFi Credentials & Config
NIFI_WEB_HTTP_PORT=8080
NIFI_SINGLE_USER_CREDENTIALS_USERNAME=admin
NIFI_SINGLE_USER_CREDENTIALS_PASSWORD=admin12345678

# AWS Configuration (dành cho S3 API của MinIO)
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=admin
AWS_SECRET_ACCESS_KEY=admin123
```

File `docker-compose.yml` định nghĩa một custom network `lakehouse` để các services có thể giao tiếp nội bộ với nhau:

- **minio:** Expose port 9000 (API) và 9001 (UI).
- **nessie:** Expose port 19120. Cấu hình dùng RocksDB làm Version Store.
- **spark-master & spark-worker:** Expose port 8080, 7077 (Master) và 8081 (Worker).
- **spark-history-server:** Expose port 18080 cho việc review logs ETL.
- **jupyter:** Expose port 8888, dùng làm môi trường Data Exploration.
- **nifi & nifi-registry:** Expose port 8082 (NiFi UI proxy) và 18081.
- **dagster:** Expose port 3000 để điều phối toàn bộ workflow.

Để triển khai toàn bộ hệ thống, chỉ cần chạy lệnh:
```bash
docker compose up -d
```

### 3. Chi tiết quá trình Data Ingestion & Data Processing

Quá trình ETL Pipeline được thiết kế theo kiến trúc Medallion (Bronze -> Silver -> Gold) và được điều phối bằng Dagster.

#### 3.1. Quá trình Data Ingestion (Apache NiFi)
Giai đoạn này chịu trách nhiệm kéo dữ liệu thô (Raw Data) từ các nguồn bên ngoài và đẩy vào **Bronze Layer** trên MinIO.
NiFi được thiết kế theo mô hình *Configuration-Driven* với một luồng (DAG) duy nhất xử lý động nhiều nguồn dữ liệu dựa trên file cấu hình JSON (`ingestion_sources.json`).
Các bước trong NiFi Flow bao gồm:
1. **GenerateFlowFile (Trigger):** Bộ lập lịch sẽ kích hoạt quá trình tải theo định kỳ (ví dụ Timer driven) và nạp đường dẫn tới file cấu hình.
2. **FetchFile:** Đọc file cấu hình JSON chứa danh sách các nguồn dữ liệu cần tải (bao gồm URL, bucket và object_key).
3. **SplitJSON:** Tách mảng JSON cấu hình thành từng object riêng lẻ để xử lý song song.
4. **EvaluateJsonPath:** Rút trích các thuộc tính `api_url`, `s3.bucket`, và `filename` lưu vào các FlowFile Attributes.
5. **InvokeHTTP:** Gửi HTTP GET request tới `api_url` để kéo dữ liệu (ví dụ: dữ liệu chuyến đi NYC Yellow Taxi tháng 01/2024 định dạng Parquet, và file tra cứu `taxi_zone_lookup.csv`).
6. **PutS3Object:** Lưu trữ file vừa kéo về vào MinIO S3 (chỉ định tới thư mục `bronze`) thông qua cấu hình credentials tập trung bằng `AWSCredentialsProviderControllerService`.

#### 3.2. Quá trình Data Processing (Apache Spark, Iceberg & Nessie)
Quá trình xử lý dữ liệu được chia làm 2 giai đoạn chính: **Bronze to Silver** và **Silver to Gold**, được thực thi bằng PySpark và ghi dưới định dạng Apache Iceberg, quản lý phiên bản bởi Project Nessie.

**Giai đoạn 1: Bronze to Silver (Làm sạch và Chuẩn hóa)**
- **Tạo Data Branch:** Trước khi bắt đầu xử lý, Spark thông qua Nessie sẽ tạo và checkout sang một nhánh mới (ví dụ: `etl_run_2024_01`) từ nhánh `main`. Điều này đảm bảo quá trình xử lý không làm ảnh hưởng đến dữ liệu đang vận hành (Data Isolation).
- **Đọc dữ liệu thô:** Đọc dữ liệu `yellow_tripdata` và `taxi_zone_lookup` từ Bronze bucket trên MinIO.
- **Làm sạch (Data Cleansing):** 
  - Lọc bỏ các chuyến đi có `passenger_count <= 0`, `trip_distance <= 0`, hoặc `total_amount < 0`.
  - Đảm bảo thời gian trả khách (`tpep_dropoff_datetime`) phải diễn ra sau thời gian đón khách (`tpep_pickup_datetime`).
  - Loại bỏ các dòng có giá trị null ở thời gian và ID vị trí đón/trả.
- **Làm giàu (Data Enrichment):**
  - Trích xuất thêm cột `Year`, `Month`, `trip_date`.
  - Tính toán thời gian di chuyển `trip_duration_seconds`.
  - Tính tỷ lệ phần trăm tiền tip `tip_percentage`.
  - Chuyển đổi mã loại thanh toán (`payment_type`) thành tên thân thiện (Credit card, Cash, Dispute,...).
- **Lưu trữ Silver:** Dữ liệu sau khi làm sạch được ghi vào Iceberg table `nessie.silver.cleaned_trips` và `nessie.silver.dim_location` thông qua tính năng overwrite và replace-where (partition theo tháng/năm) để đảm bảo idempotency.

**Giai đoạn 2: Silver to Gold (Tổng hợp và Báo cáo)**
- Checkout nhánh đang làm việc (ví dụ: `etl_run_2024_01`).
- Đọc dữ liệu từ `nessie.silver.cleaned_trips` và bảng tra cứu `dim_location`.
- **Tổng hợp nghiệp vụ (Aggregations):**
  - **`daily_trips` & `monthly_summary`:** Nhóm theo ngày/tháng để tính tổng số chuyến, tổng doanh thu, quãng đường, trung bình hành khách và phần trăm tiền tip.
  - **`revenue_by_zone`:** Nhóm doanh thu theo khu vực đón khách (`PULocationID`) và join với `dim_location` để lấy tên khu vực (Borough, ZoneName).
  - **`payment_type_summary`:** Phân tích doanh thu và thói quen tip dựa trên loại hình thanh toán.
- **Lưu trữ Gold:** Các báo cáo phân tích này được ghi đè (overwrite) vào các Iceberg tables tại namespace `nessie.gold` để sẵn sàng cho bộ phận BI/Analytics sử dụng.

#### 3.3. Kiểm tra chất lượng (Data Quality) và Hợp nhất (Merge)
Toàn bộ quá trình chạy trên Dagster (Data Orchestration). Sau khi xử lý xong Bronze to Gold, hệ thống chạy Data Quality Checks bằng Great Expectations trên nhánh `etl_run_...`.
- **Nếu FAIL (Có lỗi dữ liệu):** Pipeline dừng. Dữ liệu lỗi được cô lập trên nhánh phụ, không làm rác nhánh `main`.
- **Nếu PASS (Đạt chuẩn):** Dagster gọi script `merge_branch.py` thực hiện lệnh Spark SQL `ASSIGN BRANCH main TO etl_run_...`. Dữ liệu sạch chính thức được phát hành (publish) lên nhánh `main` và sẵn sàng cung cấp cho người dùng cuối.

## III. Kết quả

Sau khi chạy lệnh `docker compose up -d`, ta có thể truy cập vào các giao diện chính như sau:

1. **Jupyter Notebook (Khám phá dữ liệu):**
   - Truy cập: `http://localhost:8888`
   - Data Engineer có thể trực tiếp code PySpark phân tích dữ liệu trên notebook kết nối thẳng vào cluster Spark.

2. **MinIO (Kho lưu trữ - Data Lake):**
   - Truy cập: `http://localhost:9001` (Tài khoản: admin / admin123)
   - Xác nhận file Parquet dữ liệu gốc và cả dữ liệu phân mảnh của Iceberg (data files, metadata, manifest files) đều đã được lưu trữ thành công.

3. **Apache NiFi (Thu thập dữ liệu):**
   - Truy cập: `https://localhost:8082/nifi` (Tài khoản: admin / admin12345678)
   - Theo dõi pipeline đẩy data từ nguồn bên ngoài vào hệ thống.

4. **Dagster (Điều phối Pipeline):**
   - Truy cập: `http://localhost:3000`
   - Theo dõi tình trạng thực thi các job (success/fail), sự kiện rẽ nhánh và merge của Nessie, và kiểm tra báo cáo Data Quality. 

5. **Spark Master UI & History Server:**
   - Spark Master: `http://localhost:8080`
   - History Server: `http://localhost:18080`
   - Kiểm tra tài nguyên cấp phát cho ETL Jobs, logs thực thi chi tiết.

Việc tích hợp này mang lại cho tổ chức một Lakehouse hoàn thiện với khả năng Audit, Revert, và CI/CD cho dữ liệu (Data-as-Code) ngay trên môi trường nội bộ.
