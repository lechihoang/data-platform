# BÁO CÁO CÔNG VIỆC TUẦN

## 1. Kết quả công việc

### Các công việc phụ trách
| STT | Đầu công việc | Mô tả ngắn gọn | Trạng thái | Tiến độ trong tuần | Khó khăn |
|---|---|---|---|---|---|
| 1 | Truy cập vào máy ảo | Thiết lập kết nối và truy cập vào máy ảo của công ty | Hoàn thành | Đã vào được máy ảo thành công | Không |
| 2 | Tìm hiểu vai trò của các công cụ Big Data | Phân tích và xác định vai trò các công cụ: NiFi, MinIO, Spark, Iceberg, Nessie | Hoàn thành | Đã tìm hiểu xong vai trò của các công cụ | Không |
| 3 | Vẽ sơ đồ luồng dữ liệu | Vẽ sơ đồ đường đi của data từ lúc thu thập đến khi lưu trữ | Hoàn thành | Đã hiểu rõ luồng đi của data và vai trò của các công cụ thông qua việc vẽ sơ đồ | Không |
| 4 | Practice ETL với data mẫu | Tìm kiếm dữ liệu mẫu, tiến hành thực hành luồng ETL với các tech stack đã học | Đang hoàn thành | Đã xây dựng pipeline ETL cơ bản tích hợp đầy đủ tech stack (NiFi, MinIO, Spark, Iceberg, Nessie, Dagster) với tập dữ liệu mẫu NYC Yellow Taxi (2 triệu dòng) | Không |

### 1.1 Công việc đã hoàn thành
| STT | Đầu công việc | Mô tả ngắn gọn | Trạng thái | Tiến độ trong tuần | Khó khăn |
|---|---|---|---|---|---|
| 1 | Truy cập vào máy ảo | Thiết lập kết nối và truy cập vào máy ảo của công ty | Hoàn thành | Đã vào được máy ảo thành công | Không |
| 2 | Tìm hiểu vai trò của các công cụ Big Data | Phân tích và xác định vai trò các công cụ: NiFi, MinIO, Spark, Iceberg, Nessie | Hoàn thành | Đã nắm vững vai trò, cách thức sử dụng và quy trình triển khai thực tế của từng công cụ | Không |
| 3 | Vẽ sơ đồ luồng dữ liệu | Vẽ sơ đồ đường đi của data từ lúc thu thập đến khi lưu trữ | Hoàn thành | Đã hiểu rõ luồng đi của data và vai trò của các công cụ, vẽ lại thành sơ đồ trực quan (ảnh PNG) và chèn vào tài liệu | Không |

### 1.2 Công việc tồn đọng/chưa hoàn thành
| STT | Tên công việc | Lý do chưa hoàn thành | Ảnh hưởng | Kế hoạch xử lý |
|---|---|---|---|---|
| 1 | Practice ETL với data mẫu | Mới test với data nhỏ, chưa thể hoàn thiện thêm với data to hơn, cần thêm tech stack | Chưa đánh giá được hiệu năng thực tế trên Big Data | Thử nghiệm thêm tech stack (ví dụ Trino) và tăng lượng data sử dụng |

---

## 2. Kế hoạch công việc tuần tới
| STT | Tên công việc | Mục tiêu/Mô tả ngắn gọn | Dự kiến hoàn thành | Dự đoán khó khăn & Đề xuất hỗ trợ |
|---|---|---|---|---|
| 1 | Thử nghiệm thêm tech stack | Tích hợp thêm công cụ (ví dụ Trino) vào kiến trúc hiện tại để hoàn thiện hệ thống | Cuối tuần tới | |
| 2 | Tăng lượng data sử dụng | Mở rộng scale dữ liệu đầu vào để đánh giá hiệu suất của pipeline ETL | Cuối tuần tới | |

---

## 3. Các vấn đề phát sinh & giải pháp
| STT | Vấn đề phát sinh | Ảnh hưởng | Giải pháp đã thực hiện | Kết quả/Đề xuất tiếp theo |
|---|---|---|---|---|
| 1 | Một số công cụ trên máy ảo chưa được cấu hình xong | Làm chậm tiến độ test các chức năng của công cụ | Nhờ sự trợ giúp của người hướng dẫn để xử lý | Đã được người hướng dẫn hỗ trợ cấu hình lại thành công |

---

## 4. Khó khăn
- Hiện tại chưa gặp khó khăn lớn nào trong giai đoạn thiết kế. Quá trình chọn công nghệ và dữ liệu mẫu diễn ra suôn sẻ

---

## 5. Đề xuất
- Đề xuất dự trù thêm tài nguyên (RAM/CPU) cho máy local/server test, vì cụm Data Lakehouse sử dụng nhiều container (NiFi, Spark, MinIO) tốn khá nhiều bộ nhớ khi khởi động đồng loạt
