import pandas as pd
cols = ['TransactionID', 'isFraud', 'TransactionDT', 'TransactionAmt',
        'ProductCD', 'card4', 'card6', 'P_emaildomain']
df = pd.read_csv(r'D:\train_transaction.csv', usecols=cols)

# 1. 大额交易（> P99 = $1104）的欺诈率是多少？
print("=== 大额交易的欺诈率 ===")
df['is_large'] = df['TransactionAmt'] > df['TransactionAmt'].quantile(0.99)
print(df.groupby('is_large')['isFraud'].agg(['count', 'mean']))

# 2. P_emaildomain 的欺诈率（哪些邮箱域更危险？）
print("\n=== 邮箱域欺诈率 Top 10（交易量 > 500 的）===")
email_stats = df.groupby('P_emaildomain')['isFraud'].agg(['count', 'mean'])
email_stats = email_stats[email_stats['count'] > 500]
print(email_stats.sort_values('mean', ascending=False).head(10))