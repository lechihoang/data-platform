#!/bin/bash

# Khởi động NiFi ở background
/opt/nifi/scripts/start.sh &
NIFI_PID=$!

# Đảm bảo tắt NiFi đúng cách khi Stop Docker
trap 'kill -TERM $NIFI_PID; wait $NIFI_PID' TERM INT

echo "==> [NiFi-Init] Đang chờ NiFi khởi động..."
# Chờ cho đến khi NiFi API trả về 200 OK
until curl -s -k https://localhost:8443/nifi-api/access/config > /dev/null; do
    sleep 5
done

echo "==> [NiFi-Init] NiFi đã khởi động! Tiến hành kiểm tra và Import Flow..."

# Cấu hình biến môi trường cho NiFi CLI
NIFI_CLI="/opt/nifi/nifi-toolkit-current/bin/cli.sh"
CLI_PROPS="-u https://localhost:8443 -bau ${SINGLE_USER_CREDENTIALS_USERNAME:-admin} -bap ${SINGLE_USER_CREDENTIALS_PASSWORD}"

# Lệnh pg-list sẽ lấy danh sách các Process Group ở root
PG_LIST=$($NIFI_CLI nifi pg-list $CLI_PROPS 2>/dev/null)

# Quét qua tất cả các file JSON trong thư mục flows
for flow_file in /opt/nifi/nifi-current/flows/*.json; do
    [ -e "$flow_file" ] || continue
    
    # Lấy tên file không có đuôi .json (vd: Ingest_Nyc_taxi_data)
    flow_name=$(basename "$flow_file" .json)
    
    if echo "$PG_LIST" | grep -q "$flow_name"; then
        echo "==> [NiFi-Init] Luồng '$flow_name' đã tồn tại. Bỏ qua Import."
    else
        echo "==> [NiFi-Init] Chưa có luồng '$flow_name'. Đang tự động Import từ JSON..."
        $NIFI_CLI nifi pg-import -f "$flow_file" $CLI_PROPS
        echo "==> [NiFi-Init] Import thành công luồng: $flow_name"
    fi
done

# Chờ process NiFi chính
wait $NIFI_PID
