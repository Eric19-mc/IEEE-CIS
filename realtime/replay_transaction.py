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

# 官方 IEEE-CIS 测试数据
TRANSACTION_FILE = r"D:\GitHub\IEEE-CIS\data\test_transaction.csv"
IDENTITY_FILE = r"D:\GitHub\IEEE-CIS\data\test_identity.csv"

# 每条交易发送间隔
SEND_INTERVAL = 1

# 测试发送数量
# None = 全部发送
MAX_RECORDS = 10


# =========================
# 2. Kafka Producer
# =========================

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVERS,
    value_serializer=lambda v: json.dumps(
        v,
        ensure_ascii=False
    ).encode("utf-8")
)


# =========================
# 3. 读取 Identity 数据
# =========================

print("正在读取 identity 数据...")

identity_data = {}

with open(
        IDENTITY_FILE,
        "r",
        encoding="utf-8",
        newline=""
) as f:

    reader = csv.DictReader(f)

    for row in reader:
        transaction_id = row["TransactionID"]

        identity_data[transaction_id] = {
            key: value
            for key, value in row.items()
            if key != "TransactionID"
        }

print(f"Identity 数据读取完成：{len(identity_data)} 条")


# =========================
# 4. 数据类型转换
# =========================

def convert_value(value):
    """
    将 CSV 中的字符串转换成适合 JSON 的类型。
    空值保留为 None。
    """

    if value == "":
        return None

    try:
        if "." in value:
            return float(value)

        return int(value)

    except ValueError:
        return value


# =========================
# 5. 读取 Test Transaction
# =========================

print("开始读取 test_transaction.csv...")

with open(
        TRANSACTION_FILE,
        "r",
        encoding="utf-8",
        newline=""
) as f:

    reader = csv.DictReader(f)

    for i, row in enumerate(reader):

        # 达到测试数量后停止
        if MAX_RECORDS is not None and i >= MAX_RECORDS:
            break

        transaction_id = row["TransactionID"]

        # =========================
        # Transaction 特征
        # =========================

        message = {}

        for key, value in row.items():

            # test_transaction 本身没有 isFraud
            if key == "isFraud":
                continue

            message[key] = convert_value(value)

        # =========================
        # 合并 Identity 特征
        # =========================

        identity = identity_data.get(transaction_id)

        if identity is not None:

            for key, value in identity.items():
                message[key] = convert_value(value)

        else:

            # 没有 identity 的交易
            # 后面的模型特征工程会处理缺失值
            pass

        # =========================
        # 发送 Kafka
        # =========================

        producer.send(
            TOPIC,
            value=message
        )

        print(
            f"发送交易: {transaction_id} "
            f"| 金额: {message.get('TransactionAmt')} "
            f"| Identity: {'有' if identity else '无'}"
        )

        time.sleep(SEND_INTERVAL)


# =========================
# 6. 等待 Kafka 发送完成
# =========================

producer.flush()
producer.close()

print("实时交易回放完成")