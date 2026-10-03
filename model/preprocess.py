import os

import pandas as pd


# =========================================================
# 路径
# =========================================================

BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

TRANSACTION_FILE = os.path.join(
    BASE_DIR,
    "data",
    "train_transaction.csv",
)

IDENTITY_FILE = os.path.join(
    BASE_DIR,
    "data",
    "train_identity.csv",
)

OUTPUT_FILE = os.path.join(
    BASE_DIR,
    "model",
    "train_features.csv",
)


# =========================================================
# 特征定义
# =========================================================

TRANSACTION_FEATURES = [
    "TransactionID",
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
    "isFraud",
]


IDENTITY_FEATURES = [
    "TransactionID",
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
]


# =========================================================
# 读取数据
# =========================================================

print("=" * 70)
print("FinGuard 数据预处理")
print("=" * 70)

print("\n正在读取 train_transaction.csv...")

transaction = pd.read_csv(
    TRANSACTION_FILE,
    usecols=TRANSACTION_FEATURES,
    low_memory=False,
)

print(
    f"交易数据：{len(transaction):,} 条"
)


print("\n正在读取 train_identity.csv...")

identity = pd.read_csv(
    IDENTITY_FILE,
    usecols=IDENTITY_FEATURES,
    low_memory=False,
)

print(
    f"Identity 数据：{len(identity):,} 条"
)


# =========================================================
# 关联
# =========================================================

print("\n正在关联 transaction + identity...")

df = transaction.merge(
    identity,
    on="TransactionID",
    how="left",
)

print(
    f"关联后数据：{len(df):,} 条"
)


# =========================================================
# 时间特征
# =========================================================

print("\n正在构造时间特征...")

SECONDS_PER_DAY = 24 * 60 * 60

df["transaction_hour"] = (
        (df["TransactionDT"] % SECONDS_PER_DAY)
        // 3600
).astype("int8")

df["transaction_day"] = (
        df["TransactionDT"] // SECONDS_PER_DAY
).astype("int16")

df["transaction_weekday"] = (
        df["transaction_day"] % 7
).astype("int8")


# =========================================================
# 类别字段统一处理
# =========================================================

categorical_columns = [
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


print("\n正在处理类别字段...")

for column in categorical_columns:

    if column not in df.columns:
        continue

    df[column] = (
        df[column]
        .fillna("Missing")
        .astype("category")
    )


# =========================================================
# 缺失值统计
# =========================================================

print("\n" + "=" * 70)
print("缺失值统计")
print("=" * 70)

missing_rate = (
        df.isna()
        .mean()
        .sort_values(ascending=False)
        * 100
)

print(
    missing_rate[
        missing_rate > 0
        ].head(30).to_string(
        float_format=lambda x: f"{x:.2f}%"
    )
)


# =========================================================
# 数据检查
# =========================================================

print("\n" + "=" * 70)
print("数据检查")
print("=" * 70)

print(
    f"总记录数：{len(df):,}"
)

print(
    f"欺诈记录数：{int(df['isFraud'].sum()):,}"
)

print(
    f"欺诈比例：{df['isFraud'].mean():.4%}"
)

print(
    f"字段数量：{len(df.columns)}"
)


# =========================================================
# 保存
# =========================================================

print("\n正在保存处理后的数据...")

df.to_pickle(
    OUTPUT_FILE.replace(
        ".csv",
        ".pkl",
    )
)

print(
    f"输出文件："
    f"{OUTPUT_FILE.replace('.csv', '.pkl')}"
)

print("\n预处理完成。")