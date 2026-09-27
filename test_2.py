import pandas as pd

cols = ['TransactionID', 'isFraud', 'TransactionDT', 'TransactionAmt',
        'ProductCD', 'card4', 'card6', 'addr1', 'addr2', 'P_emaildomain', 'dist1']
df = pd.read_csv(r'D:\train_transaction.csv', usecols=cols)

# 1. 每个产品码的欺诈率
print("=== ProductCD 欺诈率 ===")
print(df.groupby('ProductCD')['isFraud'].agg(['count', 'sum', 'mean']))

# 2. 卡组织 × 卡类型 的欺诈率
print("\n=== card4 × card6 欺诈率 ===")
print(df.groupby(['card4', 'card6'])['isFraud'].agg(['count', 'mean']))

# 3. 按天交易量和欺诈率
df['day'] = df['TransactionDT'] // 86400
print("\n=== 按天统计（前 10 天）===")
daily = df.groupby('day').agg(
    tx_count=('TransactionID', 'count'),
    fraud_count=('isFraud', 'sum'),
    fraud_rate=('isFraud', 'mean')
)
print(daily.head(10))

# 4. 金额分位数（用于告警阈值）
print("\n=== 金额分位数 ===")
for q in [0.5, 0.9, 0.95, 0.99, 0.999]:
    print(f"P{q*100:.1f}: ${df['TransactionAmt'].quantile(q):.2f}")

# 5. 按小时分布（看是不是有高峰，决定窗口大小）
df['hour'] = (df['TransactionDT'] % 86400) // 3600
print("\n=== 按小时分布 ===")
print(df.groupby('hour').agg(
    tx_count=('TransactionID', 'count'),
    fraud_rate=('isFraud', 'mean')
))

# 6. 高缺失字段的"缺失 vs 不缺失"欺诈率对比
print("\n=== dist1 缺失 vs 不缺失，欺诈率对比 ===")
print(df.groupby(df['dist1'].isnull())['isFraud'].agg(['count', 'mean']))