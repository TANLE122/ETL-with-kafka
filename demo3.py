#!/usr/bin/env python3

import json
import time
import uuid
import random
from datetime import datetime

from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


# ——————————————————————————————
# 1. KẾT NỐI SQL SERVER
server   = 'DESKTOP-OFO2COK'  # Thay bằng tên server thật nếu khác
database = 'DWSALES'
CONN_STR = (
    f'mssql+pyodbc://{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server'
)
engine = create_engine(CONN_STR, fast_executemany=True, echo=True)


# ——————————————————————————————
# 2. KẾT NỐI KAFKA
TOPIC = 'Orders'
BOOTSTRAP = 'localhost:9092'

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BOOTSTRAP,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='python-sink-group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
print(f"[+] Sink connector running — listening on topic '{TOPIC}'...")


# ——————————————————————————————
# 3. CHUYỂN ĐỊNH DẠNG NGÀY
def parse_order_date(dt_str):
    try:
        return datetime.strptime(dt_str, "%m/%d/%Y").date()
    except:
        return None

def write_record_to_cus(rec: dict, conn):
    sql = text("""
        INSERT INTO dbo.Dim_Customer (
            CustomerName ,Phone,Gender
            ) VALUES (
               :CustomerName, :Phone,:Gender
               )
    """)
    value ={
        "Gender" : rec.get("Gender"),
        "CustomerName" : rec.get("CustomerName"),
        "Phone": rec.get("Phone")
    }
    conn.execute(sql, value)
# ——————————————————————————————
# 4. GHI DỮ LIỆU VÀO BẢNG dbo.Orders
def write_record_to_db(rec: dict, conn):
    sql = text("""
        INSERT INTO dbo.Fact_Sales(
            OrderID, CustomerName, Amount, Profit,
            Quantity, Payment, OrderDate,
            SubCategory
        ) VALUES (
            :OrderID, :CustomerName, :Amount, :Profit,
            :Quantity, :PaymentMode, :OrderDate,
            :SubCategory
        )
    """)
    params = {
        "OrderID":      rec.get("Order ID"),
        "CustomerName": rec.get("CustomerName"),
        "Amount":       float(rec.get("Amount", 0)),
        "Profit":       float(rec.get("Profit", 0)),
        "Quantity":     int(rec.get("Quantity", 0)),
        "PaymentMode":  rec.get("PaymentMode"),
        "OrderDate":    parse_order_date(rec.get("Order Date", "")),
        "SubCategory":  rec.get("Sub-Category"),
        
    }
    conn.execute(sql, params)


# ——————————————————————————————
# 5. CONSUME & LOAD VÀO DB
for msg in consumer:
    try:
        rec = msg.value
        print("[>] Consumed:", rec)

        with engine.begin() as conn:
            try:
                write_record_to_db(rec, conn)
                write_record_to_cus(rec, conn)
            except SQLAlchemyError as e:
                print("[!] DB Error:", e)

    except json.JSONDecodeError:
        print("[!] Invalid JSON message from Kafka:", msg.value)
        continue

    time.sleep(1)  # Tránh overload nếu cần
