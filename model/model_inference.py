import json
import os
import time

import joblib
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer


# =========================================================
# 基础配置
# =========================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

MODEL_PATH = os.path.join(
    BASE_DIR,
    "model",
    "model.pkl",
)


# =========================================================
# Kafka 配置
# =========================================================

KAFKA_SERVERS = [
    "192.168.56.101:9092",
    "192.168.56.102:9092",
    "192.168.56.103:9092",
]

INPUT_TOPIC = "model_input"
OUTPUT_TOPIC = "model_output"

GROUP_ID = "finguard_lightgbm_inference"


# =========================================================
# 模型特征
# =========================================================

MODEL_FEATURES = [
    "TransactionDT",
    "TransactionAmt",
    "ProductCD",
    "card1",
    "card2",
    "card3",
    "card4",
    "card5",
    "card6",
    "addr1",
    "addr2",
    "dist1",
    "P_emaildomain",
    "id_01",
    "id_02",
    "id_03",
    "id_04",
    "id_05",
    "id_06",
    "id_09",
    "id_10",
    "id_11",
    "id_12",
    "id_13",
    "id_14",
    "id_15",
    "id_16",
    "id_17",
    "id_19",
    "id_20",
    "id_28",
    "id_29",
    "id_30",
    "id_31",
    "id_32",
    "id_33",
    "id_34",
    "id_35",
    "id_36",
    "id_37",
    "id_38",
    "DeviceType",
    "DeviceInfo",
    "transaction_hour",
    "transaction_day",
    "transaction_weekday",
]


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
# 构造行为主体 Key
# =========================================================

def build_subject_key(transaction):
    """
    构造实时行为分析使用的主体代理 Key。

    与离线分析保持一致：

    card1 + card2 + card3 + card5 + card6
    + addr1 + addr2

    注意：
    该 Key 只是 IEEE-CIS 数据集中的行为主体代理标识，
    不代表真实用户 ID。
    """

    fields = [
        transaction.get("card1"),
        transaction.get("card2"),
        transaction.get("card3"),
        transaction.get("card5"),
        transaction.get("card6"),
        transaction.get("addr1"),
        transaction.get("addr2"),
    ]

    values = []

    for value in fields:

        if value is None:
            values.append("NA")
            continue

        if pd.isna(value):
            values.append("NA")
            continue

        values.append(str(value))

    return "_".join(values)


# =========================================================
# 加载模型
# =========================================================

def load_model():

    print("=" * 60)
    print("FinGuard LightGBM 实时推理服务")
    print("=" * 60)

    print(f"模型路径：{MODEL_PATH}")

    model = joblib.load(MODEL_PATH)

    print("模型加载成功")
    print(f"模型特征数量：{len(MODEL_FEATURES)}")

    # -----------------------------------------------------
    # 检查模型特征
    # -----------------------------------------------------

    if hasattr(model, "feature_name_"):

        model_features = list(model.feature_name_)

        if model_features != MODEL_FEATURES:

            print("\n模型特征检查失败")

            print("当前模型特征：")
            print(model_features)

            print("\n程序要求特征：")
            print(MODEL_FEATURES)

            raise ValueError(
                "LightGBM 模型特征顺序或数量不一致"
            )

    print("模型特征检查通过")

    return model


# =========================================================
# 构造模型输入
# =========================================================

def prepare_features(transaction, model):
    """
    构造与训练阶段完全一致的 LightGBM 输入。

    关键：
    使用模型内部保存的 pandas_categorical，
    避免实时数据重新创建 category 后
    与训练数据 categorical_feature 不一致。
    """

    features = {}

    for feature in MODEL_FEATURES:
        features[feature] = transaction.get(feature)

    df = pd.DataFrame(
        [features],
        columns=MODEL_FEATURES,
    )

    # =====================================================
    # 数值字段
    # =====================================================

    numeric_features = [
        "TransactionDT",
        "TransactionAmt",
        "card1",
        "card2",
        "card3",
        "card5",
        "addr1",
        "addr2",
        "dist1",
        "id_01",
        "id_02",
        "id_03",
        "id_04",
        "id_05",
        "id_06",
        "id_09",
        "id_10",
        "id_11",
        "id_13",
        "id_14",
        "id_17",
        "id_19",
        "id_20",
        "transaction_hour",
        "transaction_day",
        "transaction_weekday",
    ]

    for column in numeric_features:

        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce",
            )

    # =====================================================
    # 分类字段
    # =====================================================

    categorical_features = [
        "ProductCD",
        "card4",
        "card6",
        "P_emaildomain",
        "id_12",
        "id_15",
        "id_16",
        "id_28",
        "id_29",
        "id_30",
        "id_31",
        "id_32",
        "id_33",
        "id_34",
        "id_35",
        "id_36",
        "id_37",
        "id_38",
        "DeviceType",
        "DeviceInfo",
    ]

    # -----------------------------------------------------
    # 获取训练阶段保存的类别信息
    # -----------------------------------------------------

    booster = model.booster_

    pandas_categorical = getattr(
        booster,
        "pandas_categorical",
        None,
    )

    if pandas_categorical is None:
        raise ValueError(
            "模型中没有找到 pandas_categorical 信息"
        )

    if len(pandas_categorical) != len(
            categorical_features
    ):
        raise ValueError(
            "模型分类特征数量与实时分类特征数量不一致："
            f"{len(pandas_categorical)} != "
            f"{len(categorical_features)}"
        )

    # -----------------------------------------------------
    # 严格按照训练阶段的类别集合恢复 category
    # -----------------------------------------------------

    for column, categories in zip(
            categorical_features,
            pandas_categorical,
    ):

        value = df[column]

        # 训练阶段没有出现过的值统一视为缺失
        value = value.where(
            value.notna(),
            None,
        )

        df[column] = pd.Categorical(
            value,
            categories=categories,
        )

    return df


# =========================================================
# 风险等级
# =========================================================

def get_risk_level(probability):

    if probability >= 0.70:
        return "HIGH"

    if probability >= 0.30:
        return "MEDIUM"

    return "LOW"


# =========================================================
# 风险原因
# =========================================================

def get_risk_reason(probability):

    if probability >= 0.70:
        return "LightGBM模型预测风险较高"

    if probability >= 0.30:
        return "LightGBM模型预测存在一定风险"

    return "LightGBM模型预测风险较低"


# =========================================================
# 单笔交易预测
# =========================================================

def predict_transaction(model, transaction):

    # -----------------------------------------------------
    # 构造模型特征
    # -----------------------------------------------------

    features = prepare_features(
        transaction,
        model,
    )

    # -----------------------------------------------------
    # 模型预测
    # -----------------------------------------------------

    probability = float(
        model.predict_proba(features)[0][1]
    )

    probability = max(
        0.0,
        min(
            probability,
            1.0,
        ),
    )

    # -----------------------------------------------------
    # 风险等级
    # -----------------------------------------------------

    risk_level = get_risk_level(
        probability
    )

    risk_reason = get_risk_reason(
        probability
    )

    # -----------------------------------------------------
    # 行为主体代理 Key
    # -----------------------------------------------------

    subject_key = build_subject_key(
        transaction
    )

    # -----------------------------------------------------
    # 风险分数
    # -----------------------------------------------------

    risk_score = int(
        probability * 100
    )

    # -----------------------------------------------------
    # 输出结果
    # -----------------------------------------------------

    result = {

        # =============================
        # 交易基础信息
        # =============================

        "TransactionID": transaction.get(
            "TransactionID"
        ),

        "TransactionAmt": transaction.get(
            "TransactionAmt"
        ),

        "ProductCD": transaction.get(
            "ProductCD"
        ),

        "card4": transaction.get(
            "card4"
        ),

        "card6": transaction.get(
            "card6"
        ),

        "TransactionDT": transaction.get(
            "TransactionDT"
        ),

        # =============================
        # 行为主体代理 Key
        # =============================

        "subject_key": subject_key,

        # =============================
        # LightGBM
        # =============================

        "fraud_probability": probability,

        # =============================
        # 行为规则
        # =============================

        "rule_score": 0,

        # =============================
        # 当前风险
        # =============================

        "risk_score": risk_score,

        "risk_level": risk_level,

        "risk_reason": risk_reason,

        # =============================
        # 处理时间
        # =============================

        "process_time": time.strftime(
            "%Y-%m-%d %H:%M:%S"
        ),
    }

    return result


# =========================================================
# 主程序
# =========================================================

def main():

    model = load_model()

    # -----------------------------------------------------
    # Kafka
    # -----------------------------------------------------

    consumer = create_consumer()

    producer = create_producer()

    print("Kafka 连接成功")

    print(f"监听 Topic：{INPUT_TOPIC}")

    print(f"输出 Topic：{OUTPUT_TOPIC}")

    print("等待实时交易...")

    # -----------------------------------------------------
    # 实时消费
    # -----------------------------------------------------

    for message in consumer:

        transaction = message.value

        transaction_id = transaction.get(
            "TransactionID"
        )

        try:

            # =============================================
            # 模型推理
            # =============================================

            result = predict_transaction(
                model,
                transaction,
            )

            # =============================================
            # 写入 model_output
            # =============================================

            future = producer.send(
                OUTPUT_TOPIC,
                result,
            )

            # 等待 Kafka 确认
            future.get(
                timeout=10
            )

            # =============================================
            # 控制台输出
            # =============================================

            print(
                "[模型推理] "
                f"TransactionID={transaction_id} | "
                f"probability="
                f"{result['fraud_probability']:.6f} | "
                f"risk="
                f"{result['risk_level']} | "
                f"subject="
                f"{result['subject_key']}"
            )

        except Exception as e:

            print(
                "[推理失败] "
                f"TransactionID={transaction_id} | "
                f"{type(e).__name__}: {e}"
            )


# =========================================================
# 程序入口
# =========================================================

if __name__ == "__main__":
    main()