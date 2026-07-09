FROM python:3.10-slim

# Cài đặt Java (JRE) và procps để có thể chạy spark-submit trực tiếp từ Dagster container
RUN apt-get update && \
    apt-get install -y default-jre procps curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PYTHONDONTWRITEBYTECODE=1

# Copy và cài đặt thư viện Python từ requirements.txt
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Thư mục chứa code (sẽ mount volume từ host)
WORKDIR /opt/dagster/app

# Thiết lập permissions cho dagster_home để tránh lỗi ghi file SQLite khi chạy non-root
RUN mkdir -p /opt/dagster/dagster_home/storage && \
    chown -R 185:185 /opt/dagster/dagster_home

# Khai báo định danh user vào /etc/passwd để Hadoop/Spark không bị lỗi NullPointerException
RUN groupadd -g 185 dagster && \
    useradd -u 185 -g 185 -m -s /bin/bash dagster

# Ép Dagster chạy dưới user 185 (giống UID của Spark) để chung volume permission
USER 185

# Mặc định chạy Dagster dev mode (Web UI)
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-w", "/opt/dagster/dagster_home/workspace.yaml"]
