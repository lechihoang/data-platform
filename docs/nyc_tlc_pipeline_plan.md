# Kế hoạch Chuẩn hoá Dữ liệu NYC TLC (Bronze -> Silver -> Gold)

Tài liệu này phác thảo kế hoạch chuẩn hoá, làm sạch và tổng hợp dữ liệu cho các bộ dataset của NYC TLC (Yellow, Green, FHV, High Volume FHV, và Zone Lookup) dựa trên kiến trúc Medallion (Lakehouse) và Tài liệu Schema chính thức (Data Dictionary) của NYC TLC do Research Agent tổng hợp.

---

## 1. Lớp Bronze (Raw Data)
**Mục đích:** Tạo ra "Nguồn Sự Thật Duy Nhất" (Single Source of Truth) bất biến trực tiếp từ nguồn dữ liệu.
**Cách lấy dữ liệu:** Tải trực tiếp các file Parquet (trip data) và CSV (zone lookup) từ bucket S3 (`s3://nyc-tlc/`) hoặc trang web chính thức của NYC TLC.
**Đặc điểm:** Giữ nguyên dữ liệu gốc (raw). Không áp dụng bất kỳ biến đổi nào ở bước này.

### 1.1 Bảng `bronze_zone_lookup` (Dữ liệu bản đồ khu vực)
*Nguồn:* Tải file `taxi_zone_lookup.csv` từ trang TLC.

| Tên Cột | Kiểu Dữ liệu | Giải thích chính thức từ TLC |
| :--- | :--- | :--- |
| `LocationID` | String | Mã định danh duy nhất cho taxi zone (khớp với PULocationID/DOLocationID). |
| `Borough` | String | Tên Quận ở NYC (Manhattan, Brooklyn, Queens, Bronx, Staten Island, EWR, hoặc Unknown). |
| `Zone` | String | Tên cụ thể của khu vực/vùng lân cận (VD: "Midtown Center", "JFK Airport"). |
| `service_zone` | String | Danh mục dịch vụ rộng hơn (Yellow Zone, Boro Zone, Airports, EWR). |

### 1.2 Bảng `bronze_yellow_tripdata`
*Nguồn:* File `yellow_tripdata_YYYY-MM.parquet`

| Tên Cột | Kiểu Dữ liệu | Giải thích chính thức từ TLC |
| :--- | :--- | :--- |
| `VendorID` | String | Mã nhà cung cấp thiết bị TPEP (1 = Creative Mobile Technologies, 2 = VeriFone Inc). |
| `tpep_pickup_datetime` | String | Ngày và giờ khi đồng hồ tính tiền bắt đầu chạy (đón khách). |
| `tpep_dropoff_datetime` | String | Ngày và giờ khi đồng hồ tính tiền dừng lại (trả khách). |
| `passenger_count` | String | Số lượng hành khách trên xe (do tài xế nhập). |
| `trip_distance` | String | Quãng đường chuyến đi (tính bằng dặm/miles) do đồng hồ báo cáo. |
| `PULocationID` | String | ID khu vực TLC (TLC Taxi Zone) nơi chuyến đi bắt đầu. |
| `DOLocationID` | String | ID khu vực TLC nơi chuyến đi kết thúc. |
| `RatecodeID` | String | Mã loại cước phí áp dụng (1=Standard, 2=JFK, 3=Newark, 4=Nassau/Westchester, 5=Negotiated, 6=Group ride). |
| `store_and_fwd_flag` | String | Cờ cho biết dữ liệu có bị lưu tạm trên bộ nhớ xe trước khi gửi đi không ('Y'=Yes, 'N'=No). |
| `payment_type` | String | Hình thức thanh toán (1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided). |
| `fare_amount` | String | Cước phí tính theo thời gian và khoảng cách bởi đồng hồ. |
| `extra` | String | Các khoản phụ phí khác (như giờ cao điểm, qua đêm). |
| `mta_tax` | String | $0.50 Thuế MTA được kích hoạt dựa trên cước phí. |
| `improvement_surcharge` | String | $0.30 Phí cải thiện dịch vụ áp dụng cho các chuyến xe vẫy. |
| `tip_amount` | String | Tiền tip (tự động điền cho thanh toán thẻ; KHÔNG bao gồm tip tiền mặt). |
| `tolls_amount` | String | Tổng số phí cầu đường đã thanh toán. |
| `total_amount` | String | Tổng số tiền khách phải trả (không bao gồm tip tiền mặt). |
| `congestion_surcharge` | String | Phí ùn tắc giao thông của bang New York (NYS). |
| `airport_fee` | String | Phí sân bay áp dụng cho các chuyến đi đến/từ LGA và JFK. |
| `cbd_congestion_fee` | String | Phí áp dụng khi đi vào Khu Thương mại Trung tâm (Central Business District) - áp dụng từ 2025+. |

### 1.3 Bảng `bronze_green_tripdata`
*Nguồn:* File `green_tripdata_YYYY-MM.parquet`

| Tên Cột | Kiểu Dữ liệu | Giải thích chính thức từ TLC |
| :--- | :--- | :--- |
| `VendorID` | String | Mã nhà cung cấp (1 = CMT, 2 = VeriFone). |
| `lpep_pickup_datetime` | String | Thời gian đón khách (bắt đầu chạy đồng hồ). |
| `lpep_dropoff_datetime` | String | Thời gian trả khách (dừng đồng hồ). |
| `store_and_fwd_flag` | String | Cờ "lưu và chuyển tiếp" ('Y'/'N'). |
| `RatecodeID` | String | Mã loại cước phí (1-6). |
| `PULocationID` | String | ID khu vực đón khách. |
| `DOLocationID` | String | ID khu vực trả khách. |
| `passenger_count` | String | Số lượng hành khách do tài xế nhập. |
| `trip_distance` | String | Quãng đường chuyến đi (miles). |
| `fare_amount` | String | Cước phí cơ bản. |
| `extra` | String | Phụ phí. |
| `mta_tax` | String | Thuế MTA ($0.50). |
| `improvement_surcharge` | String | Phí cải thiện dịch vụ ($0.30). |
| `tip_amount` | String | Tiền tip qua thẻ. |
| `tolls_amount` | String | Phí cầu đường. |
| `total_amount` | String | Tổng số tiền khách phải trả. |
| `payment_type` | String | Hình thức thanh toán (1=Credit, 2=Cash...). |
| `trip_type` | String | Loại chuyến đi (1= Gọi vẫy trên đường/street-hail, 2= Đặt trước/dispatch). |
| `congestion_surcharge` | String | Phí ùn tắc. |
| `airport_fee` | String | Phí sân bay. |
| `cbd_congestion_fee` | String | Phí Central Business District (CBD). |

### 1.4 Bảng `bronze_fhv_tripdata` (For-Hire Vehicles thường)
*Nguồn:* File `fhv_tripdata_YYYY-MM.parquet`

| Tên Cột | Kiểu Dữ liệu | Giải thích chính thức từ TLC |
| :--- | :--- | :--- |
| `dispatching_base_num` | String | Mã số giấy phép (TLC Base License Number) của cơ sở đã điều phối chuyến đi. |
| `pickup_datetime` | String | Ngày và giờ đón khách. |
| `dropoff_datetime` | String | Ngày và giờ trả khách. |
| `PULocationID` | String | ID khu vực đón khách (TLC Taxi Zone). |
| `DOLocationID` | String | ID khu vực trả khách (TLC Taxi Zone). |
| `SR_Flag` | String | Cờ cho biết chuyến đi có thuộc chuỗi đi chung hay không (1 = Shared, Null = Non-shared). |
| `Affiliated_base_number` | String | Mã số giấy phép của cơ sở trực thuộc của phương tiện. |

### 1.5 Bảng `bronze_hvfhv_tripdata` (High Volume FHV - Uber, Lyft)
*Nguồn:* File `fhvhv_tripdata_YYYY-MM.parquet`

| Tên Cột | Kiểu Dữ liệu | Giải thích chính thức từ TLC |
| :--- | :--- | :--- |
| `hvfhs_license_num` | String | Mã số giấy phép TLC của ứng dụng HVFHS (VD: HV0003 = Uber, HV0005 = Lyft). |
| `dispatching_base_num` | String | Mã số giấy phép cơ sở điều phối. |
| `originating_base_num` | String | Mã số cơ sở tiếp nhận yêu cầu chuyến đi ban đầu. |
| `request_datetime` | String | Ngày và giờ khi hành khách yêu cầu chuyến đi. |
| `on_scene_datetime` | String | Ngày và giờ khi tài xế đến điểm đón. |
| `pickup_datetime` | String | Ngày và giờ đón khách. |
| `dropoff_datetime` | String | Ngày và giờ trả khách. |
| `PULocationID` | String | ID khu vực đón khách. |
| `DOLocationID` | String | ID khu vực trả khách. |
| `trip_miles` | String | Tổng số dặm (miles) của chuyến đi. |
| `trip_time` | String | Tổng thời gian của chuyến đi tính bằng giây. |
| `base_passenger_fare` | String | Cước phí hành khách cơ sở (trước thuế, phí, tip). |
| `tolls` | String | Tổng tiền phí cầu đường đã trả. |
| `bcf` | String | Tổng tiền thu cho Black Car Fund. |
| `sales_tax` | String | Tổng thuế bán hàng NYS thu được. |
| `congestion_surcharge` | String | Phí ùn tắc giao thông NYS. |
| `airport_fee` | String | Phí sân bay. |
| `cbd_congestion_fee` | String | Phí Central Business District. |
| `tips` | String | Tiền tip. |
| `driver_pay` | String | Tổng số tiền chi trả cho tài xế cho chuyến đi. |
| `shared_request_flag` | String | Khách hàng có yêu cầu đi chung xe không? (Y/N). |
| `shared_match_flag` | String | Khách hàng có đi chung xe với khách khác không? (Y/N). |
| `access_a_ride_flag` | String | Phương tiện dành cho người khuyết tật. |
| `wav_request_flag` | String | Yêu cầu xe thân thiện với xe lăn. |
| `wav_match_flag` | String | Khớp được xe chở xe lăn. |

---

## 2. Lớp Silver (Cleaned, Enriched & Standardized Data)
**Mục đích:** Ép kiểu chặt chẽ, đổi tên cột về chuẩn chung. Lọc bỏ các dòng lỗi (Date Infiltration, Null Location, Invalid Amounts) theo ĐÚNG CÁC QUY CHUẨN NGÀNH do Research Agent đề xuất.

**Quy tắc Cleansing & Chuẩn hoá (Theo Best Practices Data Engineering cho TLC):**
- **Date Infiltration (Temporal Integrity):** Loại bỏ các bản ghi mà `pickup_datetime` nằm ngoài khoảng thời gian hợp lý (ví dụ: máy tính trên xe bị lỗi ghi năm 2001, 2088 hoặc 2099 thay vì 2024). Bỏ chuyến đi có `dropoff_datetime <= pickup_datetime`.
- **Logic Null & Imputation:** Loại bỏ các dòng mà `PULocationID` hoặc `DOLocationID` bị rỗng/null. Xử lý an toàn các giá trị null: map `payment_type` không xác định thành "Unknown". Xử lý `cbd_congestion_fee` null (trước 2025) thành `0.0`.
- **Tài chính (Cleanse Số Âm):** **LỌC BỎ toàn bộ các chuyến đi có `total_amount` < 0 hoặc `fare_amount` < 0.** Theo chuẩn phân tích, dữ liệu âm (void/dispute) làm sai lệch tính toán doanh thu thực tế và cần bị loại bỏ ở lớp Silver. Đồng thời loại bỏ outliers vô lý (VD: > $10,000).
- **Hành khách & Khoảng cách:** Bỏ `passenger_count <= 0` hoặc lớn vô lý (ví dụ: > 9). Loại bỏ `trip_distance <= 0` hoặc khoảng cách bất khả thi về mặt địa lý (> 150 miles trong khu vực NYC). 
- **Deduplication:** Xoá bỏ các dòng trùng lặp hoàn toàn (row-level duplicates), thường xuyên xảy ra do lỗi ingest dữ liệu.
- **Enrichment:** Join tất cả các bảng trip với `silver_zone_lookup` theo ID để lấy Tên Quận (Borough) và Tên Khu vực (Zone).

*(Các bảng ở Silver giữ nguyên 100% logic schema đã thống nhất, bổ sung thêm cột `cbd_congestion_fee` kiểu DoubleType).*

---

## 3. Lớp Gold (Aggregated Analytics)
**Mục đích:** Xây dựng các aggregated metrics phục vụ cho báo cáo chiến lược, phân tích thị phần. Áp dụng chuẩn công thức tính doanh thu đầy đủ nhất do Research Agent đề xuất.

### 3.1 Bảng `daily_trips` (Thống kê chuyến đi theo ngày)
*Aggregation Level:* Gom nhóm theo Ngày và Loại xe (`trip_type`).

| Tên Cột | Kiểu Dữ liệu | Ý nghĩa |
| :--- | :--- | :--- |
| `Year` | IntegerType | Năm của chuyến đi (dùng để partition) |
| `Month` | IntegerType | Tháng của chuyến đi (dùng để partition) |
| `trip_type` | StringType | Loại dữ liệu (Yellow, Green, FHV, HVFHV) (dùng để partition) |
| `pickup_date` | DateType | Ngày đón khách |
| `total_trips` | LongType | Tổng số chuyến đi trong ngày |
| `avg_trip_distance` | DoubleType | Quãng đường trung bình |
| `total_passenger_count` | LongType | Tổng số hành khách |

### 3.2 Bảng `monthly_summary` (Thống kê tổng hợp theo tháng)
*Aggregation Level:* Gom nhóm theo Năm, Tháng và Loại xe (`trip_type`).

| Tên Cột | Kiểu Dữ liệu | Ý nghĩa |
| :--- | :--- | :--- |
| `Year` | IntegerType | Năm |
| `Month` | IntegerType | Tháng |
| `trip_type` | StringType | Loại dữ liệu (Yellow, Green, FHV, HVFHV) |
| `total_trips` | LongType | Tổng số chuyến đi trong tháng |
| `total_revenue` | DoubleType | Tổng doanh thu trong tháng |
| `avg_tip_amount` | DoubleType | Tiền tip trung bình |
| `avg_tolls_amount` | DoubleType | Phí cầu đường trung bình |

### 3.3 Bảng `revenue_by_zone` (Báo cáo doanh thu theo khu vực)
*Aggregation Level:* Gom nhóm theo ID khu vực đón, Năm, Tháng, Loại xe.

| Tên Cột | Kiểu Dữ liệu | Ý nghĩa |
| :--- | :--- | :--- |
| `Year` | IntegerType | Năm |
| `Month` | IntegerType | Tháng |
| `trip_type` | StringType | Loại dữ liệu |
| `pulocation_id` | IntegerType | ID khu vực đón khách |
| `total_trips` | LongType | Tổng số chuyến |
| `total_revenue` | DoubleType | Tổng doanh thu tại khu vực này |

### 3.4 Bảng `payment_type_summary` (Thống kê theo hình thức thanh toán)
*Aggregation Level:* Gom nhóm theo Hình thức thanh toán, Năm, Tháng, Loại xe.

| Tên Cột | Kiểu Dữ liệu | Ý nghĩa |
| :--- | :--- | :--- |
| `Year` | IntegerType | Năm |
| `Month` | IntegerType | Tháng |
| `trip_type` | StringType | Loại dữ liệu |
| `payment_type` | IntegerType | Hình thức thanh toán (1=Credit, 2=Cash...) |
| `total_trips` | LongType | Tổng số chuyến bằng hình thức này |
| `total_revenue` | DoubleType | Tổng doanh thu qua hình thức này |
