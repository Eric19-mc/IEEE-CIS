import os

import pandas as pd


BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

FILE = os.path.join(
    BASE_DIR,
    "data",
    "train_transaction.csv",
)

CHUNK_SIZE = 100000


# =========================================================
# 统计数据
# =========================================================

print("=" * 70)
print("FinGuard 特征分析")
print("=" * 70)

total_rows = 0
fraud_count = 0

missing_count = None

numeric_stats = {}
category_values = {}


# =========================================================
# 分块读取
# =========================================================

for i, chunk in enumerate(
        pd.read_csv(
            FILE,
            chunksize=CHUNK_SIZE,
            low_memory=False,
        )
):
    print(f"正在分析第 {i + 1} 个数据块...")

    total_rows += len(chunk)
    fraud_count += int(chunk["isFraud"].sum())

    # 缺失值
    current_missing = chunk.isna().sum()

    if missing_count is None:
        missing_count = current_missing
    else:
        missing_count += current_missing

    # =====================================================
    # 类别字段
    # =====================================================

    for column in [
        "ProductCD",
        "card4",
        "card6",
        "P_emaildomain",
        "R_emaildomain",
    ]:
        if column not in chunk.columns:
            continue

        if column not in category_values:
            category_values[column] = set()

        values = (
            chunk[column]
            .dropna()
            .astype(str)
            .unique()
        )

        category_values[column].update(values)


# =========================================================
# 基础信息
# =========================================================

print("\n" + "=" * 70)
print("一、基础数据")
print("=" * 70)

print(f"交易数量：{total_rows:,}")
print(f"欺诈数量：{fraud_count:,}")
print(f"欺诈比例：{fraud_count / total_rows:.4%}")


# =========================================================
# 缺失率
# =========================================================

missing_rate = (
        missing_count / total_rows * 100
).sort_values(ascending=False)

print("\n" + "=" * 70)
print("二、缺失率分布")
print("=" * 70)

print(
    f"缺失率 > 90%："
    f"{(missing_rate > 90).sum()} 个字段"
)

print(
    f"缺失率 70%~90%："
    f"{((missing_rate > 70) & (missing_rate <= 90)).sum()} 个字段"
)

print(
    f"缺失率 30%~70%："
    f"{((missing_rate > 30) & (missing_rate <= 70)).sum()} 个字段"
)

print(
    f"缺失率 < 30%："
    f"{(missing_rate < 30).sum()} 个字段"
)


# =========================================================
# 类别字段
# =========================================================

print("\n" + "=" * 70)
print("三、类别字段")
print("=" * 70)

for column, values in category_values.items():
    print(f"\n{column}")
    print(f"唯一值数量：{len(values)}")
    print("示例：", list(sorted(values))[:20])


# =========================================================
# 核心字段信息
# =========================================================

print("\n" + "=" * 70)
print("四、核心交易字段")
print("=" * 70)

core_columns = [
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
    "R_emaildomain",
]

for column in core_columns:
    if column in missing_rate.index:
        print(
            f"{column:<20}"
            f"缺失率：{missing_rate[column]:>8.2f}%"
        )


print("\n分析完成。")