import pandas as pd
from pathlib import Path


# =========================
# 路径
# =========================

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
OUTPUT_DIR = BASE_DIR / "model" / "output"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


TRANSACTION_FILE = DATA_DIR / "train_transaction.csv"
IDENTITY_FILE = DATA_DIR / "train_identity.csv"
OUTPUT_FILE = OUTPUT_DIR / "train_clean.csv"


# =========================
# 第一版特征
# =========================

TRANSACTION_FEATURES = [
    "TransactionID",
    "isFraud",
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
    "dist2",

    "P_emaildomain",
    "R_emaildomain",
]

# C1 ~ C14
TRANSACTION_FEATURES += [f"C{i}" for i in range(1, 15)]

# D1 ~ D15
TRANSACTION_FEATURES += [f"D{i}" for i in range(1, 16)]


IDENTITY_FEATURES = [
    "TransactionID",
    "DeviceType",
    "DeviceInfo",
]

IDENTITY_FEATURES += [f"id_{i:02d}" for i in range(1, 39)]


# =========================
# 读取交易数据
# =========================

print("========== 读取交易数据 ==========")

df_transaction = pd.read_csv(
    TRANSACTION_FILE,
    usecols=TRANSACTION_FEATURES
)

print("交易数据:", df_transaction.shape)


# =========================
# 读取 Identity
# =========================

print("\n========== 读取 Identity 数据 ==========")

df_identity = pd.read_csv(
    IDENTITY_FILE,
    usecols=IDENTITY_FEATURES
)

print("Identity 数据:", df_identity.shape)


# =========================
# 合并
# =========================

print("\n========== 合并数据 ==========")

df = df_transaction.merge(
    df_identity,
    on="TransactionID",
    how="left"
)

print("合并后:", df.shape)


# =========================
# 时间特征
# =========================

print("\n========== 生成时间特征 ==========")

# IEEE-CIS 的 TransactionDT 是相对时间秒数
df["transaction_hour"] = (
        (df["TransactionDT"] // 3600) % 24
).astype("int8")

df["transaction_day"] = (
        df["TransactionDT"] // 86400
).astype("int16")

df["transaction_weekday"] = (
        (df["TransactionDT"] // 86400) % 7
).astype("int8")


# =========================
# 缺失值处理
# =========================

print("\n========== 缺失值处理 ==========")

# 数值型字段
numeric_cols = df.select_dtypes(
    include=["number"]
).columns.tolist()

# 排除标签
numeric_feature_cols = [
    col for col in numeric_cols
    if col != "isFraud"
]

df[numeric_feature_cols] = df[numeric_feature_cols].fillna(-1)


# 字符串字段
categorical_cols = df.select_dtypes(
    include=["object"]
).columns.tolist()

df[categorical_cols] = df[categorical_cols].fillna("unknown")


# =========================
# 基本检查
# =========================

print("\n========== 数据检查 ==========")

print("总记录数:", len(df))
print("总字段数:", len(df.columns))

print("\n欺诈分布:")
print(df["isFraud"].value_counts())

print("\n欺诈率:")
print(round(df["isFraud"].mean() * 100, 4), "%")

print("\nTransactionID 重复:")
print(df["TransactionID"].duplicated().sum())

print("\n剩余缺失值:")
print(df.isna().sum().sum())


# =========================
# 保存
# =========================

print("\n========== 保存清洗数据 ==========")

df.to_csv(
    OUTPUT_FILE,
    index=False
)

print("保存完成:")
print(OUTPUT_FILE)