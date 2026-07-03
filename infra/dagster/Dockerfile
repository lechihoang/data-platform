FROM python:3.10-slim

# Cài đặt Java (JRE) và procps để có thể chạy spark-submit trực tiếp từ Dagster container
RUN apt-get update && \
    apt-get install -y default-jre procps curl && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

# Copy và cài đặt thư viện Python từ requirements.txt
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Thư mục chứa code (sẽ mount volume từ host)
WORKDIR /opt/dagster/app

# Mặc định chạy Dagster dev mode (Web UI)
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-w", "/opt/dagster/dagster_home/workspace.yaml"]
