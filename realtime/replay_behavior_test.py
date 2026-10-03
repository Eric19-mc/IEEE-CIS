import json
import os
import time

import pandas as pd
from kafka import KafkaProducer


# =========================================================
# 基础路径
# =========================================================

BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

TEST_CASE_FILE = os.path.join(
    BASE_DIR,
    "realtime",
    "behavior_test_case.csv",
)


# =========================================================
# Kafka
# =========================================================

KAFKA_SERVERS = [
    "192.168.56.101:9092",
    "192.168.56.102:9092",
    "192.168.56.103:9092",
]

TOPIC = "transaction_topic"


# =========================================================
# Kafka Producer
# =========================================================

def create_producer():

    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8"),
    )


# =========================================================
# 主程序
# =========================================================

def main():

    print("=" * 60)
    print("FinGuard 行为异常测试数据 Replay")
    print("=" * 60)

    print(
        f"测试数据：{TEST_CASE_FILE}"
    )

    # -----------------------------------------------------
    # 读取测试样本
    # -----------------------------------------------------

    df = pd.read_csv(
        TEST_CASE_FILE
    )

    print(
        f"测试交易数量：{len(df)}"
    )

    print()

    print(
        "TransactionID："
    )

    print(
        df["TransactionID"].tolist()
    )

    print()

    # -----------------------------------------------------
    # Producer
    # -----------------------------------------------------

    producer = create_producer()

    print(
        "Kafka 连接成功"
    )

    print(
        f"发送 Topic：{TOPIC}"
    )

    print()

    # -----------------------------------------------------
    # 逐笔发送
    # -----------------------------------------------------

    for index, row in df.iterrows():

        transaction = row.to_dict()

        # -------------------------------------------------
        # NaN → None
        # -------------------------------------------------

        for key, value in transaction.items():

            if pd.isna(value):

                transaction[key] = None

        transaction_id = transaction.get(
            "TransactionID"
        )

        amount = transaction.get(
            "TransactionAmt"
        )

        transaction_dt = transaction.get(
            "TransactionDT"
        )

        # -------------------------------------------------
        # Kafka
        # -------------------------------------------------

        future = producer.send(
            TOPIC,
            transaction,
        )

        future.get(
            timeout=10
        )

        print(
            f"[行为测试 Replay] "
            f"{index + 1}/{len(df)} | "
            f"TransactionID={transaction_id} | "
            f"Amount={amount} | "
            f"TransactionDT={transaction_dt}"
        )

        # -------------------------------------------------
        # 故意稍微间隔
        #
        # 保证 Kafka/Flink/模型/行为服务
        # 有足够时间处理
        # -------------------------------------------------

        time.sleep(2)

    producer.flush()

    print()

    print(
        "=" * 60
    )

    print(
        "行为测试数据发送完成"
    )

    print(
        "=" * 60
    )


if __name__ == "__main__":
    main()