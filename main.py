# main.py
import subprocess

# Khởi động producer (luồng đẩy dữ liệu vào Kafka)
producer_process = subprocess.Popen(['python', 'producer.py'])

# Khởi động demo ETL consumer (luồng xử lý dữ liệu)
consumer_process = subprocess.Popen(['python', 'demo3.py'])

# (Tùy chọn) chờ cả hai process hoàn tất
producer_process.wait()
consumer_process.wait()
