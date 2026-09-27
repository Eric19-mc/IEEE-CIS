import pandas as pd

# 只读关键字段 + 抽样，避免一次性读全表（650MB）
cols = ['TransactionID', 'isFraud', 'TransactionDT', 'TransactionAmt',
        'ProductCD', 'card1', 'card4', 'card6', 'addr1', 'addr2',
        'P_emaildomain', 'dist1']

df = pd.read_csv(r'D:\train_transaction.csv', usecols=cols)

print("=== 基础信息 ===")
print(f"总行数: {len(df)}")
print(f"欺诈样本数: {df['isFraud'].sum()}")
print(f"欺诈率: {df['isFraud'].mean():.2%}")

print("\n=== 时间跨度 ===")
print(f"TransactionDT 最小值: {df['TransactionDT'].min()}")
print(f"TransactionDT 最大值: {df['TransactionDT'].max()}")
days = (df['TransactionDT'].max() - df['TransactionDT'].min()) / 86400
print(f"时间跨度: {days:.1f} 天")

print("\n=== 关键字段分布 ===")
print("\nProductCD 分布:")
print(df['ProductCD'].value_counts())
print("\ncard4 (卡组织) 分布:")
print(df['card4'].value_counts())
print("\ncard6 (卡类型) 分布:")
print(df['card6'].value_counts())

print("\n=== 金额统计 ===")
print(df['TransactionAmt'].describe())

print("\n=== 缺失率 Top 10 ===")
missing = df.isnull().mean().sort_values(ascending=False)
print(missing.head(10))