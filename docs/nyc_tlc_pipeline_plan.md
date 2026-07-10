# Kế hoạch Chuẩn hoá Dữ liệu NYC TLC (Bronze -> Silver -> Gold)

Tài liệu này phác thảo kế hoạch chuẩn hoá, làm sạch và tổng hợp dữ liệu cho các bộ dataset của NYC TLC (Yellow, Green, FHV, High Volume FHV, và Zone Lookup) dựa trên kiến trúc Medallion (Lakehouse).

**Phạm vi hiện tại:** Partition `2024-01` (theo `infra/nifi/config/ingestion_sources.json`). Schema trong tài liệu này đã được **xác minh trực tiếp từ footer của file Parquet gốc trên CDN chính thức của TLC** (`d37ci6vzurychx.cloudfront.net`), nên phản ánh đúng dữ liệu thực tế của tháng 2024-01, không phải data dictionary tổng quát.

> **Lưu ý quan trọng về sai lệch so với Data Dictionary tổng quát của TLC:**
> - File `fhvhv_tripdata_2024-01` **KHÔNG có cột `bcf`** (Black Car Fund) — trước đây data dictionary có liệt kê, nhưng file thực tế 2024-01 không chứa.
> - **KHÔNG file nào** của 2024-01 có cột `cbd_congestion_fee` (phí này chỉ áp dụng từ 2025+).
> - `green` có thêm cột `ehail_fee` và cột gốc `trip_type` (1=street-hail, 2=dispatch) — khác với `trip_type` mà pipeline tự gán (yellow/green/fhv/hvfhv).
> - Casing không đồng nhất giữa các nguồn: `yellow` dùng `Airport_fee` (hoa), `fhvhv` dùng `airport_fee` (thường); `fhv` dùng `dropOff_datetime`, `PUlocationID`, `DOlocationID`.

---

## 0. Tổng quan luồng xử lý (Orchestration)

Pipeline được điều phối bởi **Dagster** (`infra/dagster/pipeline.py`), partition theo tháng (`MonthlyPartitionsDefinition`, bắt đầu 2024-01). Mỗi `dataset_type` chạy một chuỗi asset độc lập trên một **nhánh Nessie riêng** theo mẫu **Write-Audit-Publish (WAP)**:

```
bronze_to_silver  ->  dq_check_silver  ->  silver_to_gold  ->  dq_check_gold  ->  merge_branch
   (Spark)             (Great Exp.)         (Spark)            (Great Exp.)       (MERGE -> main)
```

- **Write:** Mỗi run tạo nhánh `etl_run_{type}_{year}_{month}` từ `main`, ghi toàn bộ vào nhánh này.
- **Audit:** DQ check bằng Great Expectations sau cả Silver và Gold. Fail -> dừng, không publish.
- **Publish:** `MERGE BRANCH {branch} INTO main` — tạo merge commit, giữ đầy đủ lịch sử Nessie.
- 5 job **chạy tuần tự trong từng run riêng** (không có run song song): thường `zone_pipeline` chạy **đầu tiên** (setup DAG), rồi lần lượt `yellow → green → fhv → hvfhv`.
- **Setup DAG** (`zone_pipeline`): `silver_zone → dq_check_silver_zone → gold_dimensions → merge`. Bước `gold_dimensions` build các **conformed dimension** dùng chung (`dim_location`, `dim_payment_type`). Tách riêng vì dimension **không phụ thuộc `trip_type`** — nếu để mỗi trip pipeline tự build sẽ lặp lại và ghi đè vô ích. Bước này dùng profile Spark nhẹ (`executor_memory=2g`) do các bảng dim rất nhỏ.
- **Trip DAG** (`{type}_pipeline`): `silver → dq_silver → gold_facts → dq_gold → merge`. Build 4 bảng `fact_*`.

Định dạng bảng: **Apache Iceberg** trên MinIO (S3), catalog quản lý bởi **Nessie**.

---

## 1. Lớp Bronze (Raw Data)
**Mục đích:** Tạo ra "Nguồn Sự Thật Duy Nhất" (Single Source of Truth) bất biến trực tiếp từ nguồn dữ liệu.
**Cách lấy dữ liệu:** NiFi tải trực tiếp file Parquet (trip data) và CSV (zone lookup) từ CDN chính thức của TLC và ghi thô vào `s3a://lakehouse/bronze/`.
**Đặc điểm:** Giữ nguyên dữ liệu gốc (raw). Không áp dụng bất kỳ biến đổi nào ở bước này.

### 1.1 Bảng `taxi_zone_lookup.csv` (Dữ liệu bản đồ khu vực)
*Nguồn:* `misc/taxi_zone_lookup.csv` — 265 bản ghi.

| Tên Cột | Kiểu Dữ liệu | Giải thích |
| :--- | :--- | :--- |
| `LocationID` | String | Mã định danh duy nhất cho taxi zone (khớp với PULocationID/DOLocationID). |
| `Borough` | String | Tên Quận ở NYC (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR, hoặc Unknown). |
| `Zone` | String | Tên cụ thể của khu vực/vùng lân cận (VD: "Newark Airport", "Jamaica Bay"). |
| `service_zone` | String | Danh mục dịch vụ (Yellow Zone, Boro Zone, Airports, EWR). |

### 1.2 Bảng `yellow_tripdata_2024-01.parquet`
*Các cột thực tế trong file 2024-01:*

| Tên Cột | Kiểu Dữ liệu | Giải thích |
| :--- | :--- | :--- |
| `VendorID` | Long | Mã nhà cung cấp thiết bị TPEP (1 = Creative Mobile Technologies, 2 = VeriFone Inc). |
| `tpep_pickup_datetime` | Timestamp | Thời điểm đồng hồ bắt đầu chạy (đón khách). |
| `tpep_dropoff_datetime` | Timestamp | Thời điểm đồng hồ dừng (trả khách). |
| `passenger_count` | Double | Số lượng hành khách (do tài xế nhập). |
| `trip_distance` | Double | Quãng đường chuyến đi (miles). |
| `RatecodeID` | Double | Mã loại cước (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group). |
| `store_and_fwd_flag` | String | Cờ "lưu và chuyển tiếp" ('Y'/'N'). |
| `PULocationID` | Long | ID khu vực đón khách. |
| `DOLocationID` | Long | ID khu vực trả khách. |
| `payment_type` | Long | Hình thức thanh toán (1=Credit, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided). |
| `fare_amount` | Double | Cước phí theo thời gian & khoảng cách. |
| `extra` | Double | Phụ phí (giờ cao điểm, qua đêm). |
| `mta_tax` | Double | Thuế MTA $0.50. |
| `tip_amount` | Double | Tiền tip (chỉ thẻ; KHÔNG gồm tip tiền mặt). |
| `tolls_amount` | Double | Tổng phí cầu đường. |
| `improvement_surcharge` | Double | Phí cải thiện dịch vụ $0.30. |
| `total_amount` | Double | Tổng tiền khách phải trả (không gồm tip tiền mặt). |
| `congestion_surcharge` | Double | Phí ùn tắc NYS. |
| `Airport_fee` | Double | Phí sân bay (LGA/JFK). **Chú ý: viết hoa chữ `A`.** |

### 1.3 Bảng `green_tripdata_2024-01.parquet`

| Tên Cột | Kiểu Dữ liệu | Giải thích |
| :--- | :--- | :--- |
| `VendorID` | Long | Mã nhà cung cấp (1 = CMT, 2 = VeriFone). |
| `lpep_pickup_datetime` | Timestamp | Thời gian đón khách. |
| `lpep_dropoff_datetime` | Timestamp | Thời gian trả khách. |
| `store_and_fwd_flag` | String | Cờ "lưu và chuyển tiếp" ('Y'/'N'). |
| `RatecodeID` | Double | Mã loại cước (1-6). |
| `PULocationID` | Long | ID khu vực đón khách. |
| `DOLocationID` | Long | ID khu vực trả khách. |
| `passenger_count` | Double | Số lượng hành khách. |
| `trip_distance` | Double | Quãng đường (miles). |
| `fare_amount` | Double | Cước phí cơ bản. |
| `extra` | Double | Phụ phí. |
| `mta_tax` | Double | Thuế MTA $0.50. |
| `tip_amount` | Double | Tiền tip qua thẻ. |
| `tolls_amount` | Double | Phí cầu đường. |
| `ehail_fee` | Double | Phí gọi xe điện tử (thường null). **Chỉ có ở green.** |
| `improvement_surcharge` | Double | Phí cải thiện dịch vụ $0.30. |
| `total_amount` | Double | Tổng tiền khách phải trả. |
| `payment_type` | Long | Hình thức thanh toán (1=Credit, 2=Cash...). |
| `trip_type` | Long | **Cột gốc của TLC:** 1=street-hail, 2=dispatch. (Khác với `trip_type` do pipeline gán). |
| `congestion_surcharge` | Double | Phí ùn tắc. |

> `green` **không** có cột `airport_fee` và `cbd_congestion_fee` trong file 2024-01.

### 1.4 Bảng `fhv_tripdata_2024-01.parquet` (For-Hire Vehicles thường)
*File này rất "mỏng" — không có cột tiền, quãng đường hay hành khách.*

| Tên Cột | Kiểu Dữ liệu | Giải thích |
| :--- | :--- | :--- |
| `dispatching_base_num` | String | Mã giấy phép (TLC Base License) của cơ sở điều phối. |
| `pickup_datetime` | Timestamp | Ngày giờ đón khách. |
| `dropOff_datetime` | Timestamp | Ngày giờ trả khách. **Chú ý casing: `dropOff`.** |
| `PUlocationID` | Double | ID khu vực đón. **Casing: `PUlocationID`.** |
| `DOlocationID` | Double | ID khu vực trả. **Casing: `DOlocationID`.** |
| `SR_Flag` | String | Cờ chuyến đi chung (1 = Shared, Null = Non-shared). |
| `Affiliated_base_number` | String | Mã giấy phép cơ sở trực thuộc. |

### 1.5 Bảng `fhvhv_tripdata_2024-01.parquet` (High Volume FHV - Uber, Lyft)
*Dataset lớn nhất (~19-20 triệu dòng/tháng).*

| Tên Cột | Kiểu Dữ liệu | Giải thích |
| :--- | :--- | :--- |
| `hvfhs_license_num` | String | Mã giấy phép HVFHS (HV0003 = Uber, HV0005 = Lyft). |
| `dispatching_base_num` | String | Mã giấy phép cơ sở điều phối. |
| `originating_base_num` | String | Mã cơ sở tiếp nhận yêu cầu ban đầu. |
| `request_datetime` | Timestamp | Thời điểm hành khách yêu cầu chuyến. |
| `on_scene_datetime` | Timestamp | Thời điểm tài xế đến điểm đón. |
| `pickup_datetime` | Timestamp | Thời điểm đón khách. |
| `dropoff_datetime` | Timestamp | Thời điểm trả khách. |
| `PULocationID` | Long | ID khu vực đón. |
| `DOLocationID` | Long | ID khu vực trả. |
| `trip_miles` | Double | Tổng số miles của chuyến đi. |
| `trip_time` | Long | Tổng thời gian chuyến đi (giây). |
| `base_passenger_fare` | Double | Cước phí hành khách cơ sở (trước thuế/phí/tip). |
| `tolls` | Double | Tổng phí cầu đường. |
| `sales_tax` | Double | Thuế bán hàng NYS. |
| `congestion_surcharge` | Double | Phí ùn tắc NYS. |
| `airport_fee` | Double | Phí sân bay. **Casing chữ thường.** |
| `tips` | Double | Tiền tip. |
| `driver_pay` | Double | Tổng tiền trả cho tài xế. |
| `shared_request_flag` | String | Khách có yêu cầu đi chung không (Y/N). |
| `shared_match_flag` | String | Khách có được ghép đi chung không (Y/N). |
| `access_a_ride_flag` | String | Phương tiện Access-A-Ride. |
| `wav_request_flag` | String | Yêu cầu xe cho xe lăn. |
| `wav_match_flag` | String | Khớp được xe cho xe lăn. |

> **KHÔNG có `bcf`** và **KHÔNG có `cbd_congestion_fee`** trong file 2024-01.

---

## 2. Lớp Silver (Cleaned, Enriched & Standardized Data)
**Mục đích:** Ép kiểu chặt chẽ, đổi tên cột về chuẩn chung, lọc bỏ dòng lỗi, dedup, làm giàu cột thời gian. **Không** aggregate.
**Thực thi:** `infra/dagster/spark_scripts/bronze_to_silver.py` (mẫu Template Method + Factory theo từng `dataset_type`).

### 2.1 Chuẩn hoá schema (Standardize)
Mỗi loại trip có bước `standardize_schema` đổi tên cột nguồn về tên chung, sau đó **ép về đúng bộ `STANDARD_TYPES`** (cột thiếu tự điền Null). Bảng ánh xạ:

| Cột chuẩn (silver) | Yellow | Green | FHV | HVFHV |
| :--- | :--- | :--- | :--- | :--- |
| `vendor_id` (string) | VendorID | VendorID | dispatching_base_num | hvfhs_license_num |
| `pickup_datetime` (timestamp) | tpep_pickup_datetime | lpep_pickup_datetime | pickup_datetime | pickup_datetime |
| `dropoff_datetime` (timestamp) | tpep_dropoff_datetime | lpep_dropoff_datetime | dropOff_datetime | dropoff_datetime |
| `pulocation_id` (integer) | PULocationID | PULocationID | PUlocationID | PULocationID |
| `dolocation_id` (integer) | DOLocationID | DOLocationID | DOlocationID | DOLocationID |
| `passenger_count` (integer) | passenger_count | passenger_count | *(null)* | *(null)* |
| `trip_distance` (double) | trip_distance | trip_distance | *(null)* | trip_miles |
| `fare_amount` (double) | fare_amount | fare_amount | *(null)* | base_passenger_fare |
| `tip_amount` (double) | tip_amount | tip_amount | *(null)* | tips |
| `total_amount` (double) | total_amount | total_amount | *(null)* | *(tính toán, xem 2.2)* |
| `payment_type` (integer) | payment_type | payment_type | *(null)* | *(null)* |
| `tolls_amount` (double) | tolls_amount | tolls_amount | *(null)* | *(null — nguồn là `tolls`)* |
| `sales_tax` (double) | *(null)* | *(null)* | *(null)* | sales_tax |
| `congestion_surcharge` (double) | congestion_surcharge | congestion_surcharge | *(null)* | congestion_surcharge |
| `airport_fee` (double) | *(null — nguồn là `Airport_fee`)* | *(null)* | *(null)* | airport_fee |
| `bcf` (double) | *(null)* | *(null)* | *(null)* | *(null — không tồn tại trong 2024-01)* |
| `cbd_congestion_fee` (double) | *(null)* | *(null)* | *(null)* | *(null)* |
| `trip_type` (string) | "yellow" | "green" | "fhv" | "hvfhv" |

> **Điểm còn tồn đọng ở tầng code (chưa thuộc phạm vi refactor Gold):** mapping hiện tại của yellow **không** rename `Airport_fee` -> `airport_fee`, nên cột `airport_fee` của yellow bị Null. Tương tự hvfhv có `tolls` nhưng không map sang `tolls_amount`. Không ảnh hưởng doanh thu (`total_amount` đã đúng), chỉ khiến 2 cột phí lẻ này rỗng ở Silver.

### 2.2 Công thức `total_amount` cho HVFHV
FHVHV không có sẵn `total_amount`, phải cộng dồn từ các thành phần phí. Công thức **đúng theo dữ liệu 2024-01** (loại `bcf` vì không tồn tại, bổ sung `airport_fee`):

```
total_amount = base_passenger_fare + tolls + sales_tax
             + congestion_surcharge + airport_fee + tips
```

> Công thức này đã được áp dụng đúng trong `bronze_to_silver.py` (`HVFHVTripProcessor.standardize_schema`): không tham chiếu `bcf` (cột không tồn tại trong 2024-01, sẽ gây `AnalysisException`), có cộng `airport_fee`.

### 2.3 Quy tắc làm sạch (đúng theo code)
Áp dụng lần lượt cho tất cả trip:
- **Null bắt buộc:** loại dòng thiếu `pickup_datetime`, `dropoff_datetime`, `pulocation_id`, `dolocation_id`.
- **Toàn vẹn thời gian:** `dropoff_datetime > pickup_datetime`.
- **Tài chính:** `fare_amount >= 0` (hoặc null); `total_amount >= 0` (hoặc null). *(Chỉ chặn số âm; chưa cap trần outlier — trần được kiểm ở tầng DQ, max 99999.)*
- **Hành khách:** `0 < passenger_count <= 9` (hoặc null).
- **Khoảng cách:** `0 < trip_distance < 150` miles (hoặc null).
- **Imputation:** `payment_type` null -> `5` (Unknown); `cbd_congestion_fee` null -> `0.0`.
- **Dedup:** `dropDuplicates()` toàn dòng (không key).

### 2.4 Làm giàu thời gian (Temporal Enrichment)
Thêm 4 cột dẫn xuất từ `pickup_datetime`:
- `Year` = year(pickup_datetime)
- `Month` = month(pickup_datetime)
- `trip_date` = to_date(pickup_datetime)
- `trip_duration_seconds` = unix(dropoff) − unix(pickup)

### 2.5 Lọc theo partition đích
- `Year == target_year` **AND** `Month == target_month` — cách này đồng thời loại bỏ "date infiltration" (bản ghi ghi nhầm năm 2001/2088...) vì chúng rơi ngoài partition đích.
- `trip_duration_seconds < 86400` (loại chuyến > 24 giờ).

### 2.6 Bảng đầu ra Silver

| Bảng | Ghi bởi | Partition | Ghi chú |
| :--- | :--- | :--- | :--- |
| `nessie.silver.trips` | 4 trip processor | `Year, Month, trip_type` | `overwritePartitions()` khi bảng đã tồn tại. |
| `nessie.silver.dim_location` | ZoneLookupProcessor | *(không partition)* | `createOrReplace()`. Chỉ giữ `LocationID, Borough, Zone, service_zone`, dedup theo `LocationID`. |

> Join giữa trips và `dim_location` **KHÔNG** thực hiện ở Silver — join được đẩy xuống Gold (query `fact_revenue_by_zone` × `dim_location`).

### 2.7 DQ Check Silver (`dq_check_silver.py`)
- `dim_location`: `LocationID` not null & unique; `Zone` not null.
- `trips` (chung): các cột `trip_type`, `pickup/dropoff_datetime`, `pu/dolocation_id` not null; `0 <= trip_duration_seconds <= 86400`.
- `trips` (chỉ yellow/green/hvfhv — có tiền): `total_amount` not null & trong [0, 99999]; `passenger_count` trong [1,9] (mostly 0.9); `trip_distance` trong [0,150] (mostly 0.9).

---

## 3. Lớp Gold (Star Schema: Conformed Dimensions + Aggregated Facts)
**Mục đích:** Xây dựng aggregated metrics phục vụ báo cáo & phân tích thị phần, tổ chức theo mô hình **star schema** (dimension dùng chung + fact đã tổng hợp).
**Thực thi:** `infra/dagster/spark_scripts/silver_to_gold.py`. Toàn bộ bảng nằm ở namespace `nessie.gold`.

> **Nguyên tắc grain:** Fact ở Gold vẫn **giữ nguyên mức đã tổng hợp** (theo ngày/tháng), KHÔNG hạ xuống grain từng chuyến đi — tránh nhân ~20M dòng hvfhv vào Gold và giữ đúng định nghĩa "Gold = đã aggregate".

### 3.0 Phân công build theo pipeline (luồng tuần tự)
5 job chạy **tuần tự, mỗi lần một type** (không có gì chạy song song): **setup DAG chạy đầu tiên** (`zone_pipeline`), rồi lần lượt `yellow → green → fhv → hvfhv`. Mỗi run tạo nhánh Nessie riêng rồi `MERGE BRANCH ... INTO main`. Phân công:
- **Dimension** (`dim_*`) build trong **setup DAG** (job `zone_pipeline`, asset `gold_dimensions` chạy `gold_dimensions.py`) — vì: (1) dimension là dữ liệu **dùng chung, không phụ thuộc `trip_type`**, nếu để mỗi trip pipeline tự build thì 4 run sẽ build lại và ghi đè lặp; (2) `gold.dim_location` nguồn từ `silver.dim_location` do chính bước `silver_zone` tạo, đặt cùng chỗ là đúng luồng dữ liệu. Setup DAG chạy đầu → dimension sẵn trên `main` trước khi các run trip cần.
- **Fact** (`fact_*`) build trong **trip pipeline** (`silver_to_gold.py` cho từng dataset_type), partition theo `Year, Month, trip_type` để mỗi type merge vào `main` không đè partition của type khác.
- Fact chỉ lưu **khoá số** (`pulocation_id`, `payment_type_id`) tính trực tiếp từ dữ liệu → build fact không phụ thuộc dim; join xảy ra lúc query.
- **Tài nguyên:** job `gold_dimensions` build các bảng tí hon (location 265 dòng, payment 7 dòng) nên dùng profile nhẹ (`executor_memory=2g`) thay vì 4g như các job trip.

### 3.1 Dimensions (`nessie.gold.dim_*`)

**`dim_location`** — conform từ `nessie.silver.dim_location`, `createOrReplace`.

| Tên Cột | Kiểu | Ý nghĩa |
| :--- | :--- | :--- |
| `location_id` | Integer | PK. Khớp `pulocation_id`/`dolocation_id`. |
| `borough` | String | Tên quận. |
| `zone_name` | String | Tên khu vực (từ `Zone`). |
| `service_zone` | String | Danh mục dịch vụ. |

**`dim_payment_type`** — bảng tĩnh 7 dòng, `createOrReplace`.

| Tên Cột | Kiểu | Ý nghĩa |
| :--- | :--- | :--- |
| `payment_type_id` | Integer | PK (0–6). |
| `description` | String | 0=Flex Fare trip, 1=Credit Card, 2=Cash, 3=No Charge, 4=Dispute, 5=Unknown, 6=Voided Trip. |

### 3.2 `fact_daily_trips` (Thống kê theo ngày)
*Group by:* `Year, Month, trip_date, trip_type`.

| Tên Cột | Kiểu | Ý nghĩa |
| :--- | :--- | :--- |
| `Year` | Integer | Năm (partition). |
| `Month` | Integer | Tháng (partition). |
| `trip_type` | String | Loại xe (partition). |
| `trip_date` | Date | Ngày đón khách. |
| `total_trips` | Long | Tổng số chuyến trong ngày. |
| `total_distance` | Double | Tổng quãng đường. |
| `total_revenue` | Double | Tổng doanh thu (sum total_amount). |
| `avg_trip_duration_seconds` | Double | Thời lượng chuyến trung bình (giây). |

### 3.3 `fact_monthly_summary` (Thống kê theo tháng)
*Group by:* `Year, Month, trip_type` (grain tháng).

| Tên Cột | Kiểu | Ý nghĩa |
| :--- | :--- | :--- |
| `Year` | Integer | Năm. |
| `Month` | Integer | Tháng. |
| `trip_type` | String | Loại xe. |
| `total_trips` | Long | Tổng số chuyến trong tháng. |
| `total_distance` | Double | Tổng quãng đường. |
| `total_revenue` | Double | Tổng doanh thu. |
| `avg_trip_duration_seconds` | Double | Thời lượng chuyến trung bình (giây). |

### 3.4 `fact_revenue_by_zone` (Doanh thu theo khu vực đón)
*Group by:* `Year, Month, trip_type, pulocation_id`. **Không** denormalize tên zone — join `dim_location` lúc query.

| Tên Cột | Kiểu | Ý nghĩa |
| :--- | :--- | :--- |
| `Year` | Integer | Năm. |
| `Month` | Integer | Tháng. |
| `trip_type` | String | Loại xe. |
| `pulocation_id` | Integer | FK → `dim_location.location_id`. |
| `total_trips` | Long | Tổng số chuyến tại khu vực. |
| `total_revenue` | Double | Tổng doanh thu tại khu vực. |
| `total_tip` | Double | Tổng tiền tip. |
| `total_fare` | Double | Tổng cước phí. |
| `avg_fare` | Double | Cước phí trung bình. |
| `avg_tip` | Double | Tiền tip trung bình. |
| `tip_percentage` | Double | Tỷ lệ tip / fare (%). = 0 nếu `total_fare` = 0. |

### 3.5 `fact_payment_summary` (Thống kê theo hình thức thanh toán)
*Group by:* `Year, Month, trip_type, payment_type_id`. Giữ **mã số** — join `dim_payment_type` để lấy mô tả.

| Tên Cột | Kiểu | Ý nghĩa |
| :--- | :--- | :--- |
| `Year` | Integer | Năm. |
| `Month` | Integer | Tháng. |
| `trip_type` | String | Loại xe. |
| `payment_type_id` | Integer | FK → `dim_payment_type.payment_type_id`. |
| `total_trips` | Long | Tổng số chuyến bằng hình thức này. |
| `total_revenue` | Double | Tổng doanh thu qua hình thức này. |
| `total_tips` | Double | Tổng tiền tip qua hình thức này. |

> Cả 4 bảng `fact_*` partition theo `Year, Month, trip_type` và dùng `overwritePartitions()` khi đã tồn tại.

### 3.6 DQ Check Gold (`dq_check_gold.py`)
- `fact_monthly_summary`: `total_trips` not null & trong [1, 99999999]; `trip_type` not null.
- `fact_revenue_by_zone`: `pulocation_id` not null; `total_revenue` not null (**bỏ qua với fhv** vì fhv không có dữ liệu tiền).
