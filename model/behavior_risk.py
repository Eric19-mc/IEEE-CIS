import json
import time
from collections import defaultdict, deque

from kafka import KafkaConsumer, KafkaProducer


# =========================================================
# Kafka 配置
# =========================================================

KAFKA_SERVERS = [
    "192.168.56.101:9092",
    "192.168.56.102:9092",
    "192.168.56.103:9092",
]

INPUT_TOPIC = "model_output"
OUTPUT_TOPIC = "behavior_output"

GROUP_ID = "finguard_behavior_risk"


# =========================================================
# 行为窗口
# =========================================================

WINDOW_SECONDS = 10 * 60


# =========================================================
# Kafka Consumer
# =========================================================

def create_consumer():

    return KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )


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
# 行为窗口
#
# subject_key -> deque
#
# 每条记录：
# {
#     "transaction_id": ...,
#     "event_time": ...,
#     "amount": ...,
#     "fraud_probability": ...
# }
# =========================================================

windows = defaultdict(deque)


# =========================================================
# TransactionID 幂等去重
#
# TransactionID 是单笔交易的唯一标识。
#
# 注意：
# 同一个 subject_key 下，不同 TransactionID 的交易
# 仍然会正常进入行为窗口并参与统计。
# =========================================================

processed_transactions = set()


# =========================================================
# 清理过期交易
# =========================================================

def clean_window(subject_key, current_time):

    window = windows[subject_key]

    while window:

        oldest = window[0]

        if current_time - oldest["event_time"] <= WINDOW_SECONDS:
            break

        window.popleft()


# =========================================================
# 行为风险规则
# =========================================================

def calculate_behavior_risk(records):

    if not records:
        return {
            "rule_score": 0,
            "behavior_level": "LOW",
            "behavior_reason": "窗口内无历史交易",
        }

    transaction_count = len(records)

    amounts = [
        float(record["amount"])
        for record in records
    ]

    total_amount = sum(amounts)

    max_amount = max(amounts)

    avg_amount = total_amount / transaction_count

    score = 0

    reasons = []

    # -----------------------------------------------------
    # 交易频率
    # -----------------------------------------------------

    if transaction_count >= 7:

        score += 40

        reasons.append(
            f"10分钟内交易{transaction_count}笔"
        )

    elif transaction_count >= 4:

        score += 25

        reasons.append(
            f"10分钟内交易{transaction_count}笔"
        )

    elif transaction_count >= 3:

        score += 10

        reasons.append(
            f"10分钟内交易{transaction_count}笔"
        )

    # -----------------------------------------------------
    # 累计金额
    # -----------------------------------------------------

    if total_amount >= 6000:

        score += 40

        reasons.append(
            f"10分钟累计金额{total_amount:.2f}"
        )

    elif total_amount >= 3000:

        score += 25

        reasons.append(
            f"10分钟累计金额{total_amount:.2f}"
        )

    elif total_amount >= 1500:

        score += 10

        reasons.append(
            f"10分钟累计金额{total_amount:.2f}"
        )

    # -----------------------------------------------------
    # 单笔最大金额
    # -----------------------------------------------------

    if max_amount >= 3000:

        score += 20

        reasons.append(
            f"单笔最大金额{max_amount:.2f}"
        )

    elif max_amount >= 1500:

        score += 10

        reasons.append(
            f"单笔最大金额{max_amount:.2f}"
        )

    score = min(score, 100)

    # -----------------------------------------------------
    # 行为风险等级
    # -----------------------------------------------------

    if score >= 70:

        level = "HIGH"

    elif score >= 40:

        level = "MEDIUM"

    else:

        level = "LOW"

    if not reasons:

        reason = "10分钟行为窗口未发现明显异常"

    else:

        reason = "；".join(reasons)

    return {
        "rule_score": score,
        "behavior_level": level,
        "behavior_reason": reason,
        "transaction_count": transaction_count,
        "total_amount": round(total_amount, 3),
        "max_amount": round(max_amount, 3),
        "avg_amount": round(avg_amount, 3),
    }


# =========================================================
# 处理单笔模型结果
# =========================================================

def process_transaction(transaction):

    subject_key = transaction.get("subject_key")

    transaction_id = transaction.get("TransactionID")

    amount = transaction.get(
        "TransactionAmt",
        0,
    )

    transaction_dt = transaction.get(
        "TransactionDT"
    )

    if subject_key is None:

        raise ValueError(
            "model_output 缺少 subject_key"
        )

    if transaction_id is None:

        raise ValueError(
            "model_output 缺少 TransactionID"
        )

    if transaction_dt is None:

        raise ValueError(
            "model_output 缺少 TransactionDT"
        )

    # -----------------------------------------------------
    # TransactionID 幂等检查
    # -----------------------------------------------------
    #
    # 如果同一笔交易已经处理过：
    # 不再加入行为窗口
    # 不再重复计算行为风险
    # 不再输出重复结果
    #
    # 不影响同一个 subject_key 下的其他交易。
    # -----------------------------------------------------

    if transaction_id in processed_transactions:

        return None

    processed_transactions.add(
        transaction_id
    )

    # -----------------------------------------------------
    # 金额转换
    # -----------------------------------------------------

    try:

        amount = float(amount)

    except (TypeError, ValueError):

        amount = 0.0

    # -----------------------------------------------------
    # TransactionDT 转换
    # -----------------------------------------------------

    try:

        event_time = int(transaction_dt)

    except (TypeError, ValueError):

        raise ValueError(
            "TransactionDT 无法转换为整数"
        )

    # -----------------------------------------------------
    # 加入当前交易
    # -----------------------------------------------------

    windows[subject_key].append(
        {
            "transaction_id": transaction_id,
            "event_time": event_time,
            "amount": amount,
            "fraud_probability": transaction.get(
                "fraud_probability",
                0,
            ),
        }
    )

    # -----------------------------------------------------
    # 删除10分钟以前的交易
    # -----------------------------------------------------

    clean_window(
        subject_key,
        event_time,
    )

    records = windows[subject_key]

    # -----------------------------------------------------
    # 计算行为风险
    # -----------------------------------------------------

    behavior = calculate_behavior_risk(
        records
    )

    # -----------------------------------------------------
    # 输出
    # -----------------------------------------------------

    result = {

        "TransactionID": transaction_id,

        "subject_key": subject_key,

        "TransactionAmt": amount,

        "TransactionDT": event_time,

        "fraud_probability": float(
            transaction.get(
                "fraud_probability",
                0,
            )
        ),

        "rule_score": behavior["rule_score"],

        "behavior_level": behavior[
            "behavior_level"
        ],

        "behavior_reason": behavior[
            "behavior_reason"
        ],

        "transaction_count": behavior.get(
            "transaction_count",
            len(records),
        ),

        "total_amount": behavior.get(
            "total_amount",
            0,
        ),

        "max_amount": behavior.get(
            "max_amount",
            0,
        ),

        "avg_amount": behavior.get(
            "avg_amount",
            0,
        ),

        "process_time": time.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    }

    return result


# =========================================================
# 主程序
# =========================================================

def main():

    print("=" * 60)
    print("FinGuard 实时行为风险分析服务")
    print("=" * 60)

    print(
        f"行为窗口：{WINDOW_SECONDS // 60} 分钟"
    )

    print(
        f"输入 Topic：{INPUT_TOPIC}"
    )

    print(
        f"输出 Topic：{OUTPUT_TOPIC}"
    )

    consumer = create_consumer()

    producer = create_producer()

    print("Kafka 连接成功")

    print("等待模型推理结果...")

    # -----------------------------------------------------
    # 消费
    # -----------------------------------------------------

    for message in consumer:

        transaction = message.value

        transaction_id = transaction.get(
            "TransactionID"
        )

        try:

            result = process_transaction(
                transaction
            )

            # -------------------------------------------------
            # 重复交易直接跳过
            # -------------------------------------------------

            if result is None:

                print(
                    "[跳过重复交易] "
                    f"TransactionID={transaction_id}"
                )

                continue

            # -------------------------------------------------
            # 输出 Kafka
            # -------------------------------------------------

            future = producer.send(
                OUTPUT_TOPIC,
                result,
            )

            future.get(
                timeout=10
            )

            # -------------------------------------------------
            # 控制台
            # -------------------------------------------------

            print(
                "[行为分析] "
                f"TransactionID={transaction_id} | "
                f"subject={result['subject_key']} | "
                f"count={result['transaction_count']} | "
                f"total="
                f"{result['total_amount']:.2f} | "
                f"score={result['rule_score']} | "
                f"level={result['behavior_level']}"
            )

        except Exception as e:

            print(
                "[行为分析失败] "
                f"TransactionID={transaction_id} | "
                f"{type(e).__name__}: {e}"
            )


# =========================================================
# 程序入口
# =========================================================

if __name__ == "__main__":
    main()