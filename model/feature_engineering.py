import pandas as pd
import numpy as np
from pathlib import Path


# =========================
# 路径
# =========================

BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_FILE = BASE_DIR / "model" / "output" / "train_clean.csv"
OUTPUT_FILE = BASE_DIR / "model" / "output" / "train_features.csv"


print("========== 读取清洗数据 ==========")

df = pd.read_csv(INPUT_FILE)

print("原始数据:", df.shape)


# =========================
# 1. 金额特征
# =========================

print("\n========== 金额特征 ==========")

# 金额对数，减少极端大金额影响
df["TransactionAmt_log"] = np.log1p(df["TransactionAmt"])

# 金额分档
df["amount_level"] = pd.cut(
    df["TransactionAmt"],
    bins=[-np.inf, 20, 50, 100, 500, 2000, np.inf],
    labels=[
        "very_low",
        "low",
        "medium",
        "high",
        "very_high",
        "extreme"
    ]
).astype(str)


# =========================
# 2. 时间特征
# =========================

print("\n========== 时间特征 ==========")

# 是否深夜交易
df["is_late_night"] = (
    (df["transaction_hour"] < 6)
).astype("int8")

# 是否工作时间
df["is_work_hour"] = (
        (df["transaction_hour"] >= 9) &
        (df["transaction_hour"] < 18)
).astype("int8")

# 周末
df["is_weekend"] = (
        df["transaction_weekday"] >= 5
).astype("int8")


# =========================
# 3. Identity 特征
# =========================

print("\n========== Identity 特征 ==========")

df["has_identity"] = (
        df["DeviceType"] != "unknown"
).astype("int8")


# =========================
# 4. 卡片特征
# =========================

print("\n========== 卡片特征 ==========")

# card1 是一个重要的卡片标识特征
# 转成字符串，让模型按类别处理
df["card1"] = df["card1"].astype(str)

# card2 / card3 / card5 保持数值


# =========================
# 5. 输出检查
# =========================

print("\n========== 特征检查 ==========")

print("特征工程后:", df.shape)

print("\n新增特征:")

new_features = [
    "TransactionAmt_log",
    "amount_level",
    "is_late_night",
    "is_work_hour",
    "is_weekend",
    "has_identity"
]

print(df[new_features].head())


# =========================
# 保存
# =========================

print("\n========== 保存特征数据 ==========")

df.to_csv(
    OUTPUT_FILE,
    index=False
)

print("保存完成:")
print(OUTPUT_FILE)