#!/bin/bash

echo "🚀 BẮT ĐẦU CHUỖI TỰ ĐỘNG CHẠY DATA CÁC THÁNG CÒN LẠI CỦA NĂM 2024"

for month in 05 06 07 08 09 10 11 12; do
  echo "=========================================================="
  echo "🕒 ĐANG TIẾN HÀNH THÁNG 2024-$month..."
  echo "=========================================================="
  
  ./run_pipeline_month.sh 2024-${month}-01
  
  if [ $? -ne 0 ]; then
    echo "⚠️ Tiến trình tự động đã dừng lại ở tháng 2024-$month."
    echo "Nguyên nhân có thể là do TLC chưa công bố file dữ liệu của tháng này."
    break
  fi
done

echo "🎉 HOÀN TẤT CHUỖI TỰ ĐỘNG!"
