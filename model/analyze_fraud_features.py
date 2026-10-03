import os

import pandas as pd


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


print("=" * 70)
print("FinGuard 欺诈特征分析")
print("=" * 70)


# =========================================================
# 1. 读取交易数据
# =========================================================

print("\n正在读取交易数据...")

transaction = pd.read_csv(
    TRANSACTION_FILE,
    usecols=[
        "TransactionID",
        "TransactionDT",
        "TransactionAmt",
        "ProductCD",
        "card4",
        "card6",
        "P_emaildomain",
        "R_emaildomain",
        "addr1",
        "addr2",
        "card1",
        "card2",
        "card3",
        "card5",
        "isFraud",
    ],
    low_memory=False,
)

print(
    f"交易数量：{len(transaction):,}"
)


# =========================================================
# 2. 关联 Identity
# =========================================================

print("\n正在关联 Identity...")

identity_columns = [
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

identity = pd.read_csv(
    IDENTITY_FILE,
    usecols=identity_columns,
    low_memory=False,
)

df = transaction.merge(
    identity,
    on="TransactionID",
    how="left",
)

print(
    f"关联完成：{len(df):,} 条"
)


# =========================================================
# 3. 基础信息
# =========================================================

print("\n" + "=" * 70)
print("一、整体欺诈情况")
print("=" * 70)

print(
    f"总交易：{len(df):,}"
)

print(
    f"欺诈交易：{int(df['isFraud'].sum()):,}"
)

print(
    f"欺诈率：{df['isFraud'].mean():.4%}"
)


# =========================================================
# 4. 类别字段欺诈率
# =========================================================

category_columns = [
    "ProductCD",
    "card4",
    "card6",
    "DeviceType",
    "id_12",
    "id_15",
    "id_16",
    "id_23",
    "id_27",
    "id_28",
    "id_29",
    "id_32",
    "id_34",
    "id_35",
    "id_36",
    "id_37",
    "id_38",
]


print("\n" + "=" * 70)
print("二、类别字段欺诈率")
print("=" * 70)


for column in category_columns:

    if column not in df.columns:
        continue

    print("\n" + "-" * 60)
    print(column)

    result = (
        df.groupby(
            df[column].fillna("Missing"),
            dropna=False,
        )["isFraud"]
        .agg(
            count="count",
            fraud_count="sum",
            fraud_rate="mean",
        )
        .sort_values(
            "fraud_rate",
            ascending=False,
        )
    )

    result["fraud_rate"] *= 100

    print(
        result.head(15).to_string(
            float_format=lambda x: f"{x:.2f}"
        )
    )


# =========================================================
# 5. 金额分析
# =========================================================

print("\n" + "=" * 70)
print("三、交易金额分析")
print("=" * 70)

amount_stats = (
    df.groupby("isFraud")["TransactionAmt"]
    .agg(
        count="count",
        mean="mean",
        median="median",
        max="max",
    )
)

print(amount_stats)


# =========================================================
# 6. 时间分析
# =========================================================

print("\n" + "=" * 70)
print("四、交易时间分析")
print("=" * 70)

# IEEE-CIS TransactionDT 是相对时间
seconds_per_day = 24 * 60 * 60

df["transaction_hour"] = (
        (df["TransactionDT"] % seconds_per_day)
        // 3600
)

hour_result = (
    df.groupby("transaction_hour")["isFraud"]
    .agg(
        count="count",
        fraud_count="sum",
        fraud_rate="mean",
    )
)

hour_result["fraud_rate"] *= 100

print(
    hour_result.to_string(
        float_format=lambda x: f"{x:.2f}"
    )
)


# =========================================================
# 7. Identity 覆盖情况
# =========================================================

print("\n" + "=" * 70)
print("五、Identity 覆盖与欺诈率")
print("=" * 70)

has_identity = df["id_01"].notna()

identity_result = (
    df.groupby(has_identity)["isFraud"]
    .agg(
        count="count",
        fraud_count="sum",
        fraud_rate="mean",
    )
)

identity_result["fraud_rate"] *= 100

identity_result.index = [
    "无 Identity",
    "有 Identity",
]

print(
    identity_result.to_string(
        float_format=lambda x: f"{x:.2f}"
    )
)


print("\n" + "=" * 70)
print("分析完成")
print("=" * 70)