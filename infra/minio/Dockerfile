FROM minio/minio:latest

# Create data directory
RUN mkdir -p /data

# Copy and setup entrypoint
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Working directory
WORKDIR /data

ENTRYPOINT ["/entrypoint.sh"]
