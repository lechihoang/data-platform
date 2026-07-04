#!/bin/sh
# Create lakehouse bucket by creating a directory
mkdir -p /data/lakehouse

# Start MinIO server with arguments passed from docker-compose
exec minio "$@"
