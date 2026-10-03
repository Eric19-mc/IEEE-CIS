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

CHUNK_SIZE = 50000

CHUNK_SIZE = 50000

total_rows = 0
fraud_count = 0
transaction_ids = set()

missing_count = None
dtypes = None


print("=" * 70)
print("FinGuard IEEE-CIS 数据质量分析")
print("=" * 70)

for i, chunk in enumerate(
        pd.read_csv(
            FILE,
            chunksize=CHUNK_SIZE,
            low_memory=False,
        )
):
    print(f"正在处理第 {i + 1} 个数据块...")

    total_rows += len(chunk)

    # 欺诈数量
    fraud_count += int(chunk["isFraud"].sum())

    # TransactionID 重复检查
    transaction_ids.update(chunk["TransactionID"].dropna().astype("int64"))

    # 缺失值统计
    current_missing = chunk.isna().sum()

    if missing_count is None:
        missing_count = current_missing
        dtypes = chunk.dtypes
    else:
        missing_count += current_missing


print("\n" + "=" * 70)
print("一、数据规模")
print("=" * 70)

print(f"总交易数：{total_rows:,}")
print(f"字段数量：{len(missing_count)}")


print("\n" + "=" * 70)
print("二、欺诈比例")
print("=" * 70)

normal_count = total_rows - fraud_count

print(f"正常交易：{normal_count:,}")
print(f"欺诈交易：{fraud_count:,}")
print(f"欺诈比例：{fraud_count / total_rows:.4%}")


print("\n" + "=" * 70)
print("三、TransactionID 唯一性")
print("=" * 70)

unique_count = len(transaction_ids)

print(f"唯一 TransactionID：{unique_count:,}")
print(f"重复数量：{total_rows - unique_count:,}")


print("\n" + "=" * 70)
print("四、缺失率最高的字段")
print("=" * 70)

missing_rate = (
        missing_count / total_rows * 100
).sort_values(ascending=False)

for field, rate in missing_rate.head(30).items():
    print(f"{field:<20} {rate:>8.2f}%")


print("\n" + "=" * 70)
print("五、缺失率较低的字段")
print("=" * 70)

for field, rate in missing_rate[missing_rate < 30].head(80).items():
    print(f"{field:<20} {rate:>8.2f}%")


print("\n" + "=" * 70)
print("六、字段类型")
print("=" * 70)

print(dtypes.value_counts())


print("\n分析完成。")