import os

import pandas as pd


BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

TRAIN_TRANSACTION = os.path.join(
    BASE_DIR,
    "data",
    "train_transaction.csv",
)

TRAIN_IDENTITY = os.path.join(
    BASE_DIR,
    "data",
    "train_identity.csv",
)


print("=" * 70)
print("FinGuard Identity 特征分析")
print("=" * 70)


# =========================================================
# 1. 读取数据
# =========================================================

print("\n正在读取 train_transaction.csv...")

transaction = pd.read_csv(
    TRAIN_TRANSACTION,
    usecols=[
        "TransactionID",
        "isFraud",
    ],
    low_memory=False,
)

print(
    f"train_transaction："
    f"{len(transaction):,} 条"
)


print("\n正在读取 train_identity.csv...")

identity = pd.read_csv(
    TRAIN_IDENTITY,
    low_memory=False,
)

print(
    f"train_identity："
    f"{len(identity):,} 条"
)


# =========================================================
# 2. 基础信息
# =========================================================

print("\n" + "=" * 70)
print("一、Identity 基础信息")
print("=" * 70)

print(f"字段数量：{len(identity.columns)}")

print("\n字段：")

for column in identity.columns:
    print(column)


# =========================================================
# 3. TransactionID 关联情况
# =========================================================

print("\n" + "=" * 70)
print("二、TransactionID 关联情况")
print("=" * 70)

transaction_ids = set(
    transaction["TransactionID"]
)

identity_ids = set(
    identity["TransactionID"]
)

matched_ids = transaction_ids & identity_ids

print(
    f"transaction TransactionID："
    f"{len(transaction_ids):,}"
)

print(
    f"identity TransactionID："
    f"{len(identity_ids):,}"
)

print(
    f"成功关联："
    f"{len(matched_ids):,}"
)

print(
    f"关联覆盖率："
    f"{len(matched_ids) / len(transaction_ids):.2%}"
)


# =========================================================
# 4. Identity 缺失率
# =========================================================

print("\n" + "=" * 70)
print("三、Identity 字段缺失率")
print("=" * 70)

missing_rate = (
        identity.isna()
        .mean()
        .sort_values(ascending=False)
        * 100
)

for column, rate in missing_rate.items():

    print(
        f"{column:<20}"
        f"{rate:>8.2f}%"
    )


# =========================================================
# 5. 唯一值数量
# =========================================================

print("\n" + "=" * 70)
print("四、Identity 字段唯一值数量")
print("=" * 70)

for column in identity.columns:

    unique_count = identity[column].nunique(
        dropna=True
    )

    print(
        f"{column:<20}"
        f"{unique_count:>10,}"
    )


# =========================================================
# 6. 关键字段
# =========================================================

print("\n" + "=" * 70)
print("五、关键 Identity 字段")
print("=" * 70)

key_columns = [
    "id_01",
    "id_02",
    "id_05",
    "id_06",
    "id_12",
    "id_13",
    "id_14",
    "id_17",
    "id_19",
    "id_20",
    "id_30",
    "id_31",
    "id_32",
    "DeviceType",
    "DeviceInfo",
]

for column in key_columns:

    if column not in identity.columns:
        continue

    print(
        f"\n{column}"
    )

    print(
        f"缺失率："
        f"{missing_rate[column]:.2f}%"
    )

    print(
        f"唯一值："
        f"{identity[column].nunique(dropna=True):,}"
    )

    values = (
        identity[column]
        .dropna()
        .astype(str)
        .value_counts()
        .head(10)
    )

    print("常见值：")

    for value, count in values.items():

        print(
            f"  {value}: {count:,}"
        )


# =========================================================
# 7. Identity 与欺诈关联
# =========================================================

print("\n" + "=" * 70)
print("六、Identity 覆盖数据中的欺诈情况")
print("=" * 70)

merged = identity[
    ["TransactionID"]
].merge(
    transaction[
        ["TransactionID", "isFraud"]
    ],
    on="TransactionID",
    how="left",
)

print(
    f"Identity 对应交易："
    f"{len(merged):,}"
)

print(
    f"其中欺诈交易："
    f"{int(merged['isFraud'].sum()):,}"
)

print(
    f"Identity 数据欺诈率："
    f"{merged['isFraud'].mean():.4%}"
)


# =========================================================
# 完成
# =========================================================

print("\n" + "=" * 70)
print("分析完成")
print("=" * 70)