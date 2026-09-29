import math
from typing import Any

import lightgbm as lgb
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel


# =========================
# 1. 模型
# =========================

MODEL_FILE = r"D:\GitHub\IEEE-CIS\model\output\finguard_lgbm_v2.txt"

model = lgb.Booster(model_file=MODEL_FILE)

FEATURES = model.feature_name()

print(f"模型加载成功")
print(f"模型特征数: {len(FEATURES)}")


# =========================
# 2. FastAPI
# =========================

app = FastAPI(
    title="FinGuard LightGBM Model Service",
    version="1.0.0"
)


# =========================
# 3. 请求格式
# =========================

class TransactionRequest(BaseModel):
    data: dict[str, Any]


# =========================
# 4. 特征工程
# =========================

def build_features(data: dict[str, Any]) -> pd.DataFrame:

    df = pd.DataFrame([data])

    # -------------------------
    # 数值类型转换
    # -------------------------

    numeric_features = [
        feature
        for feature in FEATURES
        if feature not in [
            "ProductCD",
            "card4",
            "card6",
            "P_emaildomain",
            "R_emaildomain",
            "amount_level",
            "DeviceType",
            "DeviceInfo",
            "id_12",
            "id_15",
            "id_16",
            "id_23",
            "id_27",
            "id_28",
            "id_29",
            "id_30",
            "id_31",
            "id_33",
            "id_34",
            "id_35",
            "id_36",
            "id_37",
            "id_38",
        ]
    ]

    for column in numeric_features:
        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce"
            )

    # -------------------------
    # 字符串类型
    # -------------------------

    categorical_features = [
        "ProductCD",
        "card4",
        "card6",
        "P_emaildomain",
        "R_emaildomain",
        "amount_level",
        "DeviceType",
        "DeviceInfo",
        "id_12",
        "id_15",
        "id_16",
        "id_23",
        "id_27",
        "id_28",
        "id_29",
        "id_30",
        "id_31",
        "id_33",
        "id_34",
        "id_35",
        "id_36",
        "id_37",
        "id_38",
    ]

    for column in categorical_features:
        if column in df.columns:
            df[column] = df[column].fillna("unknown").astype("category")

    # -------------------------
    # 缺失值
    # -------------------------

    for column in numeric_features:
        if column in df.columns:
            df[column] = df[column].fillna(-1)

    # -------------------------
    # 特征工程
    # -------------------------

    if "TransactionAmt" in df.columns:

        df["TransactionAmt_log"] = (
            df["TransactionAmt"]
            .fillna(0)
            .clip(lower=0)
            .apply(math.log1p)
        )

        def get_amount_level(amount):

            if amount < 20:
                return "very_low"

            if amount < 50:
                return "low"

            if amount < 200:
                return "medium"

            if amount < 500:
                return "high"

            if amount < 2000:
                return "very_high"

            return "extreme"

        df["amount_level"] = (
            df["TransactionAmt"]
            .fillna(0)
            .apply(get_amount_level)
            .astype("category")
        )

    # -------------------------
    # 时间特征
    # -------------------------

    if "TransactionDT" in df.columns:

        transaction_dt = (
            pd.to_numeric(
                df["TransactionDT"],
                errors="coerce"
            )
            .fillna(0)
        )

        df["transaction_hour"] = (
                                         transaction_dt // 3600
                                 ) % 24

        df["transaction_day"] = (
                transaction_dt // 86400
        )

        df["transaction_weekday"] = (
                df["transaction_day"] % 7
        )

        df["is_late_night"] = (
                (df["transaction_hour"] < 6)
                | (df["transaction_hour"] >= 23)
        ).astype(int)

        df["is_work_hour"] = (
                (df["transaction_hour"] >= 9)
                & (df["transaction_hour"] < 18)
        ).astype(int)

        df["is_weekend"] = (
                df["transaction_weekday"] >= 5
        ).astype(int)

    # -------------------------
    # Identity 是否存在
    # -------------------------

    identity_columns = [
        column for column in FEATURES
        if column.startswith("id_")
           or column in ["DeviceType", "DeviceInfo"]
    ]

    # 先确保 Identity 字段存在
    for column in identity_columns:
        if column not in df.columns:
            df[column] = "unknown"

    df["has_identity"] = (
        df[identity_columns]
        .replace("unknown", pd.NA)
        .notna()
        .any(axis=1)
        .astype(int)
    )

    # -------------------------
    # card1 与训练保持一致
    # -------------------------

    if "card1" in df.columns:
        df["card1"] = (
            pd.to_numeric(
                df["card1"],
                errors="coerce"
            )
            .fillna(-1)
            .astype(str)
            .astype("category")
        )

    # -------------------------
    # 最终特征
    # -------------------------

    for feature in FEATURES:

        if feature not in df.columns:

            if feature in categorical_features:
                df[feature] = pd.Series(
                    ["unknown"],
                    dtype="category"
                )

            else:
                df[feature] = -1

    # 保证顺序与模型完全一致
    df = df[FEATURES]

    # categorical 类型统一
    for feature in FEATURES:

        if feature in categorical_features:
            df[feature] = (
                df[feature]
                .fillna("unknown")
                .astype("category")
            )

        else:
            df[feature] = pd.to_numeric(
                df[feature],
                errors="coerce"
            ).fillna(-1)

    return df


# =========================
# 5. 预测接口
# =========================

@app.post("/predict")
def predict(request: TransactionRequest):

    df = build_features(request.data)

    probability = float(
        model.predict(df)[0]
    )

    return {
        "fraud_probability": round(
            probability,
            6
        )
    }


# =========================
# 6. 健康检查
# =========================

@app.get("/health")
def health():

    return {
        "status": "ok",
        "model": "finguard_lgbm_v2",
        "features": len(FEATURES)
    }