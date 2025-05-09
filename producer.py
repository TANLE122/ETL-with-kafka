from kafka import KafkaProducer
import csv
import json
import time

KAFKA_TOPIC = "Orders"
KAFKA_SERVER = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
with open("SalesDatasetN.csv" ,"r") as file:
    reader = csv.DictReader(file)
    for row in reader:
        producer.send(KAFKA_TOPIC, row)
        time.sleep(1)  # Giả lập gửi dữ liệu theo thời gian thực
        print(f"Sent: {row}")
producer.flush()
producer.close()
