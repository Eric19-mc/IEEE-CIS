import json
import math

from kafka import KafkaProducer


KAFKA_SERVERS = [
    "192.168.56.101:9092",
    "192.168.56.102:9092",
    "192.168.56.103:9092",
]

TOPIC = "transaction_topic"


def clean_nan(value):
    """
    将 NaN / Infinity 转换为 JSON 标准的 null。
    """

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None

        return value

    if isinstance(value, dict):
        return {
            key: clean_nan(val)
            for key, val in value.items()
        }

    if isinstance(value, list):
        return [
            clean_nan(item)
            for item in value
        ]

    return value


def create_producer():

    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,

        value_serializer=lambda value:
        json.dumps(
            clean_nan(value),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8"),
    )


def send_transaction(producer, transaction):

    producer.send(
        TOPIC,
        value=transaction,
    )


def close_producer(producer):

    producer.flush()
    producer.close()