import csv
import json
import time

from kafka import KafkaProducer


# =========================
# 1. Kafka 配置
# =========================
KAFKA_SERVERS = [
    "192.168.56.101:9092",
    "192.168.56.102:9092",
    "192.168.56.103:9092"
]

TOPIC = "transaction_topic"

CSV_FILE = r"D:\GitHub\data-engineering-learning\IEEE-CIS\data\train_transaction.csv"


# =========================
# 2. 创建 Kafka Producer
# =========================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


# =========================
# 3. 读取 CSV
# =========================
with open(CSV_FILE, "r", encoding="utf-8") as f:

    reader = csv.DictReader(f)

    for i, row in enumerate(reader):

        # 先只测试 10 条
        if i >= 10:
            break

        message = {
            "TransactionID": int(row["TransactionID"]),
            "TransactionAmt": float(row["TransactionAmt"]),
            "ProductCD": row["ProductCD"],
            "card1": int(row["card1"]),
            "card4": row["card4"],
            "card6": row["card6"],
            "TransactionDT": int(row["TransactionDT"]),
            "isFraud": int(row["isFraud"])
        }

        producer.send(TOPIC, value=message)

        print("发送交易：", message["TransactionID"])

        # 模拟实时交易间隔
        time.sleep(1)


producer.flush()
producer.close()

print("10 条交易发送完成")