#!/bin/bash

# Khởi động NiFi ở background
/opt/nifi/scripts/start.sh &
NIFI_PID=$!

# Đảm bảo tắt NiFi đúng cách khi Stop Docker
trap 'kill -TERM $NIFI_PID; wait $NIFI_PID' TERM INT

echo "==> [NiFi-Init] Đang chờ NiFi khởi động..."
# Chờ cho đến khi NiFi API mở port (mã trả về 200 hoặc 401 đều ok vì có auth)
until [ "$(curl -s -k --resolve localhost:8443:$(hostname -i) -o /dev/null -w "%{http_code}" https://localhost:8443/nifi-api/access/config)" != "000" ]; do
    sleep 5
done

echo "==> [NiFi-Init] NiFi đã khởi động! Tiến hành kiểm tra và Import Flow..."

# Lấy token để dùng REST API
TOKEN=$(curl -s -k --resolve localhost:8443:$(hostname -i) -X POST https://localhost:8443/nifi-api/access/token -H "Content-Type: application/x-www-form-urlencoded" --data "username=${SINGLE_USER_CREDENTIALS_USERNAME:-admin}&password=${SINGLE_USER_CREDENTIALS_PASSWORD}")

# Lấy Root Process Group ID
ROOT_PG=$(curl -s -k --resolve localhost:8443:$(hostname -i) -X GET https://localhost:8443/nifi-api/flow/process-groups/root -H "Authorization: Bearer $TOKEN" | grep -o '"id":"[^"]*' | head -1 | awk -F'"' '{print $4}')

# Lấy danh sách Process Group con
PG_LIST=$(curl -s -k --resolve localhost:8443:$(hostname -i) -X GET https://localhost:8443/nifi-api/process-groups/$ROOT_PG/process-groups -H "Authorization: Bearer $TOKEN")

# Quét qua tất cả các file JSON trong thư mục flows
for flow_file in /opt/nifi/nifi-current/flows/*.json; do
    [ -e "$flow_file" ] || continue
    
    # Lấy tên file không có đuôi .json (vd: Ingest_Nyc_taxi_data)
    flow_name=$(basename "$flow_file" .json)
    
    if echo "$PG_LIST" | grep -q "\"name\":\"$flow_name\""; then
        echo "==> [NiFi-Init] Luồng '$flow_name' đã tồn tại. Bỏ qua Import."
    else
        echo "==> [NiFi-Init] Chưa có luồng '$flow_name'. Đang tự động Import từ JSON..."
        curl -s -k --resolve localhost:8443:$(hostname -i) -X POST -H "Authorization: Bearer $TOKEN" https://localhost:8443/nifi-api/process-groups/$ROOT_PG/process-groups/upload -F "id=$ROOT_PG" -F "clientId=$(cat /proc/sys/kernel/random/uuid)" -F "groupName=$flow_name" -F "positionX=0" -F "positionY=0" -F "file=@$flow_file" > /dev/null
        echo "==> [NiFi-Init] Import thành công luồng: $flow_name"
    fi
done

# Chờ process NiFi chính
wait $NIFI_PID
