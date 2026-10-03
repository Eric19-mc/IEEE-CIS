import json
import time

from kafka import KafkaConsumer, KafkaProducer


# =========================================================
# Kafka 配置
# =========================================================

KAFKA_SERVERS = [
    "192.168.56.101:9092",
    "192.168.56.102:9092",
    "192.168.56.103:9092",
]

MODEL_TOPIC = "model_output"
BEHAVIOR_TOPIC = "behavior_output"
OUTPUT_TOPIC = "risk_output"

MODEL_GROUP_ID = "finguard_risk_fusion_model"
BEHAVIOR_GROUP_ID = "finguard_risk_fusion_behavior"


# =========================================================
# Kafka Consumer
# =========================================================

def create_model_consumer():

    return KafkaConsumer(
        MODEL_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id=MODEL_GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
    )


def create_behavior_consumer():

    return KafkaConsumer(
        BEHAVIOR_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        group_id=BEHAVIOR_GROUP_ID,
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
# 结果缓存
#
# model_cache:
#     TransactionID -> {
#         "result": model_output,
#         "cached_at": 时间戳
#     }
#
# behavior_cache:
#     TransactionID -> {
#         "result": behavior_output,
#         "cached_at": 时间戳
#     }
#
# 只有两边都有同一个 TransactionID 时，
# 才允许进行风险融合。
# =========================================================

model_cache = {}
behavior_cache = {}


# =========================================================
# 缓存过期配置
# =========================================================

# 单个 TransactionID 最长保留 5 分钟
#
# 正常情况下：
# model_output 和 behavior_output
# 应该在较短时间内完成匹配。
#
# 如果超过这个时间仍然没有匹配成功，
# 说明这条数据可能因为异常导致另一边永远没有到达。
#
# 自动删除可以防止缓存长期增长。
CACHE_EXPIRE_SECONDS = 300


# 每隔 30 秒检查一次缓存
CACHE_CLEAN_INTERVAL_SECONDS = 30


# 上一次执行缓存清理的时间
last_cache_clean_time = time.time()


# =========================================================
# 缓存清理
# =========================================================

def clean_expired_cache():
    """
    清理超过 CACHE_EXPIRE_SECONDS
    仍然没有完成匹配的 TransactionID。

    防止 model_cache / behavior_cache
    长时间运行后无限增长。
    """

    global last_cache_clean_time

    current_time = time.time()

    # -----------------------------------------------------
    # 没到清理周期，不执行
    # -----------------------------------------------------

    if (
            current_time - last_cache_clean_time
            < CACHE_CLEAN_INTERVAL_SECONDS
    ):
        return

    last_cache_clean_time = current_time

    # =====================================================
    # 清理 model_cache
    # =====================================================

    expired_model_ids = []

    for transaction_id, cache_item in list(
            model_cache.items()
    ):

        cached_at = cache_item.get(
            "cached_at",
            current_time,
        )

        if (
                current_time - cached_at
                > CACHE_EXPIRE_SECONDS
        ):

            expired_model_ids.append(
                transaction_id
            )

    for transaction_id in expired_model_ids:

        del model_cache[
            transaction_id
        ]

    # =====================================================
    # 清理 behavior_cache
    # =====================================================

    expired_behavior_ids = []

    for transaction_id, cache_item in list(
            behavior_cache.items()
    ):

        cached_at = cache_item.get(
            "cached_at",
            current_time,
        )

        if (
                current_time - cached_at
                > CACHE_EXPIRE_SECONDS
        ):

            expired_behavior_ids.append(
                transaction_id
            )

    for transaction_id in expired_behavior_ids:

        del behavior_cache[
            transaction_id
        ]

    # =====================================================
    # 输出清理信息
    # =====================================================

    if (
            expired_model_ids
            or expired_behavior_ids
    ):

        print(
            "[缓存清理] "
            f"model_expired="
            f"{len(expired_model_ids)} | "
            f"behavior_expired="
            f"{len(expired_behavior_ids)} | "
            f"model_cache="
            f"{len(model_cache)} | "
            f"behavior_cache="
            f"{len(behavior_cache)}"
        )


# =========================================================
# 综合风险计算
# =========================================================

def calculate_risk(
        fraud_probability,
        behavior_score,
):
    """
    综合风险分数：

    模型风险：70%
    行为风险：30%

    最终范围：0 ~ 100
    """

    # -----------------------------------------------------
    # 模型风险
    #
    # fraud_probability:
    # 0 ~ 1
    #
    # model_score:
    # 0 ~ 100
    # -----------------------------------------------------

    model_score = fraud_probability * 100

    # -----------------------------------------------------
    # 风险融合
    # -----------------------------------------------------

    risk_score = (
            model_score * 0.7
            + behavior_score * 0.3
    )

    # -----------------------------------------------------
    # 限制范围
    # -----------------------------------------------------

    risk_score = max(
        0,
        min(
            risk_score,
            100,
        ),
    )

    return round(
        risk_score
    )


# =========================================================
# 风险等级
# =========================================================

def get_risk_level(
        risk_score
):

    # -----------------------------------------------------
    # 高风险
    # -----------------------------------------------------

    if risk_score >= 25:

        return "HIGH"

    # -----------------------------------------------------
    # 中风险
    # -----------------------------------------------------

    if risk_score >= 15:

        return "MEDIUM"

    # -----------------------------------------------------
    # 低风险
    # -----------------------------------------------------

    return "LOW"


# =========================================================
# 风险原因
# =========================================================

def get_risk_reason(
        fraud_probability,
        behavior_score,
        behavior_reason,
):

    reasons = []

    # -----------------------------------------------------
    # 模型风险
    # -----------------------------------------------------

    if fraud_probability >= 0.70:

        reasons.append(
            "LightGBM模型预测风险较高"
        )

    elif fraud_probability >= 0.30:

        reasons.append(
            "LightGBM模型预测存在一定风险"
        )

    # -----------------------------------------------------
    # 行为风险
    # -----------------------------------------------------

    if behavior_score >= 70:

        reasons.append(
            f"行为异常：{behavior_reason}"
        )

    elif behavior_score >= 30:

        reasons.append(
            f"存在行为异常：{behavior_reason}"
        )

    # -----------------------------------------------------
    # 没有明显风险
    # -----------------------------------------------------

    if not reasons:

        return (
            "模型和行为窗口均未发现明显异常"
        )

    return "；".join(
        reasons
    )


# =========================================================
# 合并模型结果 + 行为结果
# =========================================================

def build_risk_result(
        model_result,
        behavior_result,
):

    # =====================================================
    # 基础字段
    # =====================================================

    transaction_id = model_result.get(
        "TransactionID"
    )

    subject_key = model_result.get(
        "subject_key"
    )

    # =====================================================
    # 模型概率
    # =====================================================

    fraud_probability = float(
        model_result.get(
            "fraud_probability",
            0,
        )
    )

    # =====================================================
    # 行为风险分数
    #
    # behavior_output 中：
    # rule_score 就是 behavior_score
    # =====================================================

    behavior_score = float(
        behavior_result.get(
            "rule_score",
            0,
        )
    )

    # =====================================================
    # 综合风险
    # =====================================================

    risk_score = calculate_risk(
        fraud_probability,
        behavior_score,
    )

    risk_level = get_risk_level(
        risk_score
    )

    risk_reason = get_risk_reason(
        fraud_probability,
        behavior_score,
        behavior_result.get(
            "behavior_reason",
            "",
        ),
    )

    # =====================================================
    # 最终结果
    # =====================================================

    return {

        # =============================
        # 交易信息
        # =============================

        "TransactionID": transaction_id,

        "TransactionAmt": model_result.get(
            "TransactionAmt"
        ),

        "ProductCD": model_result.get(
            "ProductCD"
        ),

        "card4": model_result.get(
            "card4"
        ),

        "card6": model_result.get(
            "card6"
        ),

        "TransactionDT": model_result.get(
            "TransactionDT"
        ),

        # =============================
        # 行为主体
        # =============================

        "subject_key": subject_key,

        # =============================
        # 模型风险
        # =============================

        "fraud_probability": (
            fraud_probability
        ),

        # =============================
        # 行为风险
        # =============================

        "behavior_score": behavior_score,

        "behavior_level": behavior_result.get(
            "behavior_level",
            "LOW",
        ),

        "transaction_count": behavior_result.get(
            "transaction_count",
            1,
        ),

        "total_amount": behavior_result.get(
            "total_amount",
            0,
        ),

        "max_amount": behavior_result.get(
            "max_amount",
            0,
        ),

        "avg_amount": behavior_result.get(
            "avg_amount",
            0,
        ),

        # =============================
        # 综合风险
        # =============================

        "risk_score": risk_score,

        "risk_level": risk_level,

        "risk_reason": risk_reason,

        # =============================
        # 时间
        # =============================

        "process_time": time.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    }


# =========================================================
# 尝试进行风险融合
# =========================================================

def try_fuse(
        transaction_id,
        producer,
):
    """
    只有 model_output 和 behavior_output
    都存在同一个 TransactionID 时，
    才执行综合风险计算。
    """

    # -----------------------------------------------------
    # TransactionID 无效
    # -----------------------------------------------------

    if transaction_id is None:

        return

    # =====================================================
    # 获取缓存
    # =====================================================

    model_cache_item = model_cache.get(
        transaction_id
    )

    behavior_cache_item = behavior_cache.get(
        transaction_id
    )

    # -----------------------------------------------------
    # 任意一边还没到
    # -----------------------------------------------------

    if (
            model_cache_item is None
            or behavior_cache_item is None
    ):

        return

    # =====================================================
    # 提取真正的数据结果
    # =====================================================

    model_result = model_cache_item[
        "result"
    ]

    behavior_result = behavior_cache_item[
        "result"
    ]

    # =====================================================
    # 构建最终风险结果
    # =====================================================

    result = build_risk_result(
        model_result,
        behavior_result,
    )

    # =====================================================
    # 写入 risk_output
    # =====================================================

    future = producer.send(
        OUTPUT_TOPIC,
        result,
    )

    # 等待 Kafka 确认发送成功
    future.get(
        timeout=10
    )

    # =====================================================
    # 控制台输出
    # =====================================================

    print(
        "[综合风险] "
        f"TransactionID="
        f"{transaction_id} | "
        f"model="
        f"{result['fraud_probability']:.6f} | "
        f"behavior="
        f"{result['behavior_score']} | "
        f"risk="
        f"{result['risk_score']} | "
        f"level="
        f"{result['risk_level']}"
    )

    # =====================================================
    # 融合成功
    #
    # 删除两边缓存
    # 防止同一个 TransactionID 重复融合
    # =====================================================

    del model_cache[
        transaction_id
    ]

    del behavior_cache[
        transaction_id
    ]


# =========================================================
# 主程序
# =========================================================

def main():

    print("=" * 60)
    print("FinGuard 实时综合风险服务")
    print("=" * 60)

    print(
        f"模型输入：{MODEL_TOPIC}"
    )

    print(
        f"行为输入：{BEHAVIOR_TOPIC}"
    )

    print(
        f"综合输出：{OUTPUT_TOPIC}"
    )

    print(
        "风险权重：模型 70% + 行为 30%"
    )

    print(
        "风险分层："
        "HIGH >= 25，"
        "MEDIUM >= 15，"
        "LOW < 15"
    )

    print(
        "关联方式：TransactionID 精确匹配"
    )

    print(
        f"缓存过期时间："
        f"{CACHE_EXPIRE_SECONDS} 秒"
    )

    print(
        f"缓存清理周期："
        f"{CACHE_CLEAN_INTERVAL_SECONDS} 秒"
    )

    # =====================================================
    # Kafka
    # =====================================================

    model_consumer = create_model_consumer()

    behavior_consumer = create_behavior_consumer()

    producer = create_producer()

    print("Kafka 连接成功")

    print(
        "等待模型风险和行为风险..."
    )

    # =====================================================
    # 主循环
    # =====================================================

    while True:

        # -------------------------------------------------
        # 定期清理过期缓存
        # -------------------------------------------------

        clean_expired_cache()

        # =================================================
        # 读取模型结果
        # =================================================

        model_records = model_consumer.poll(
            timeout_ms=200
        )

        for _, messages in model_records.items():

            for message in messages:

                model_result = message.value

                transaction_id = model_result.get(
                    "TransactionID"
                )

                # -------------------------------------------------
                # 检查 TransactionID
                # -------------------------------------------------

                if transaction_id is None:

                    print(
                        "[警告] "
                        "model_output 缺少 TransactionID"
                    )

                    continue

                # -------------------------------------------------
                # 保存模型结果
                # -------------------------------------------------

                model_cache[
                    transaction_id
                ] = {
                    "result": model_result,
                    "cached_at": time.time(),
                }

                # -------------------------------------------------
                # 尝试融合
                # -------------------------------------------------

                try:

                    try_fuse(
                        transaction_id,
                        producer,
                    )

                except Exception as e:

                    print(
                        "[综合风险失败] "
                        f"TransactionID="
                        f"{transaction_id} | "
                        f"{type(e).__name__}: "
                        f"{e}"
                    )

        # =================================================
        # 读取行为结果
        # =================================================

        behavior_records = behavior_consumer.poll(
            timeout_ms=200
        )

        for _, messages in behavior_records.items():

            for message in messages:

                behavior_result = message.value

                transaction_id = behavior_result.get(
                    "TransactionID"
                )

                # -------------------------------------------------
                # 检查 TransactionID
                # -------------------------------------------------

                if transaction_id is None:

                    print(
                        "[警告] "
                        "behavior_output 缺少 TransactionID"
                    )

                    continue

                # -------------------------------------------------
                # 保存行为结果
                # -------------------------------------------------

                behavior_cache[
                    transaction_id
                ] = {
                    "result": behavior_result,
                    "cached_at": time.time(),
                }

                # -------------------------------------------------
                # 尝试融合
                # -------------------------------------------------

                try:

                    try_fuse(
                        transaction_id,
                        producer,
                    )

                except Exception as e:

                    print(
                        "[综合风险失败] "
                        f"TransactionID="
                        f"{transaction_id} | "
                        f"{type(e).__name__}: "
                        f"{e}"
                    )


# =========================================================
# 程序入口
# =========================================================

if __name__ == "__main__":

    main()