#!/bin/sh
# Create lakehouse bucket 
mkdir -p /data/lakehouse

# Start MinIO server with arguments passed from docker-compose
exec minio "$@"
