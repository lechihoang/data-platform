#!/bin/bash

# Kiểm tra xem người dùng đã nhập tháng chưa
if [ -z "$1" ]; then
    echo "Lỗi: Vui lòng nhập ngày của tháng cần chạy (Định dạng: YYYY-MM-DD)"
    echo "Ví dụ: ./run_pipeline_month.sh 2024-05-01"
    exit 1
fi

PARTITION=$1
echo "=========================================================="
echo "🚀 BẮT ĐẦU CHẠY PIPELINE CHO THÁNG: $PARTITION"
echo "=========================================================="

# Chạy tuần tự 4 loại xe để không quá tải RAM
for job in yellow green fhv hvfhv; do
  echo "--------------------------------------------------------"
  echo "▶️ ĐANG CHẠY ${job}_pipeline ..."
  echo "--------------------------------------------------------"
  
  docker exec dagster dagster job execute -f /opt/dagster/app/pipeline.py -j ${job}_pipeline --tags "{\"dagster/partition\": \"$PARTITION\"}"
  
  if [ $? -eq 0 ]; then
    echo "✅ Xong ${job}_pipeline."
  else
    echo "❌ LỖI: ${job}_pipeline chạy thất bại! Đang dừng tiến trình..."
    exit 1
  fi
done

# echo "--------------------------------------------------------"
# echo "🧹 ĐANG DỌN DẸP RÁC Ổ CỨNG (spark-work)..."
# echo "--------------------------------------------------------"
# docker exec spark-worker bash -c "rm -rf /opt/spark/work/*"
# docker exec spark-master bash -c "rm -rf /opt/spark/work/*"
# echo "✅ Đã dọn xong hàng chục GB file rác!"

echo "=========================================================="
echo "🎉 HOÀN TẤT TOÀN BỘ DATA CHO THÁNG $PARTITION CHỈ VỚI 1 LỆNH!"
echo "=========================================================="
