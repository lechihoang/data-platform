.PHONY: help up down rebuild format ingest process notebook

help:
	@echo "Danh sách các lệnh Makefile:"
	@echo "  make up        - Bật toàn bộ hệ thống Docker"
	@echo "  make down      - Tắt toàn bộ hệ thống Docker"
	@echo "  make rebuild   - Build lại và bật toàn bộ hệ thống"
	@echo "  make format    - Chạy Black, isort, flake8 để format toàn bộ code"
	@echo "  make ingest    - Bật các dịch vụ phục vụ quá trình thu thập (NiFi, MinIO)"
	@echo "  make process   - Bật các dịch vụ phục vụ xử lý dữ liệu (Dagster, Spark, Nessie, Trino, MinIO)"
	@echo "  make notebook  - Bật các dịch vụ phục vụ phân tích (Jupyter, Spark, Nessie, MinIO)"

up:
	docker-compose up -d

down:
	docker-compose down

rebuild:
	docker-compose up -d --build

format:
	pre-commit run --all-files

ingest:
	@echo "Đang khởi động cụm dịch vụ Ingest (NiFi, MinIO)..."
	docker-compose up -d minio nifi

process:
	@echo "Đang khởi động cụm dịch vụ Process (Dagster, Spark, Nessie, Trino)..."
	docker-compose up -d minio nessie spark-master spark-worker dagster trino

notebook:
	@echo "Đang khởi động cụm dịch vụ Notebook (Jupyter, Spark, Nessie)..."
	docker-compose up -d minio nessie spark-master jupyter
