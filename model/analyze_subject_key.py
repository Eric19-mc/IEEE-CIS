import pandas as pd

TRANSACTION_FILE = "../data/train_transaction.csv"

print("=" * 60)
print("IEEE-CIS 行为主体时间窗口分析")
print("=" * 60)

df = pd.read_csv(
    TRANSACTION_FILE,
    usecols=[
        "TransactionID",
        "TransactionDT",
        "TransactionAmt",
        "card1",
        "card2",
        "card3",
        "card5",
        "card6",
        "addr1",
        "addr2",
    ],
)

# =========================================================
# 1. 构造候选主体 Key
# =========================================================

df["subject_key"] = (
        df["card1"].astype("string").fillna("NA")
        + "_"
        + df["card2"].astype("string").fillna("NA")
        + "_"
        + df["card3"].astype("string").fillna("NA")
        + "_"
        + df["card5"].astype("string").fillna("NA")
        + "_"
        + df["card6"].astype("string").fillna("NA")
        + "_"
        + df["addr1"].astype("string").fillna("NA")
        + "_"
        + df["addr2"].astype("string").fillna("NA")
)

# TransactionDT 单位是秒
df["event_time"] = pd.to_timedelta(
    df["TransactionDT"],
    unit="s",
)

df = df.sort_values(
    ["subject_key", "TransactionDT"]
).reset_index(drop=True)

print(f"\n交易数量：{len(df):,}")
print(f"主体 Key 数量：{df['subject_key'].nunique():,}")

# =========================================================
# 2. 计算 10 分钟 / 30 分钟窗口
# =========================================================

def analyze_window(window_seconds, name):
    print(f"\n{'=' * 60}")
    print(f"{name} 行为窗口")
    print(f"{'=' * 60}")

    results = []

    for key, group in df.groupby("subject_key", sort=False):
        times = group["TransactionDT"].to_numpy()
        amounts = group["TransactionAmt"].to_numpy()

        left = 0

        max_count = 0
        max_amount = 0.0
        max_avg = 0.0

        for right in range(len(times)):
            while times[right] - times[left] > window_seconds:
                left += 1

            count = right - left + 1

            if count > max_count:
                max_count = count

            window_amount = amounts[left:right + 1].sum()

            if window_amount > max_amount:
                max_amount = window_amount

            avg_amount = window_amount / count

            if avg_amount > max_avg:
                max_avg = avg_amount

        results.append(
            (
                key,
                max_count,
                max_amount,
                max_avg,
            )
        )

    result_df = pd.DataFrame(
        results,
        columns=[
            "subject_key",
            "max_transaction_count",
            "max_total_amount",
            "max_avg_amount",
        ],
    )

    print("\n交易次数窗口统计：")
    print(
        result_df["max_transaction_count"].describe(
            percentiles=[
                0.50,
                0.90,
                0.95,
                0.99,
                0.999,
            ]
        )
    )

    print("\n累计金额窗口统计：")
    print(
        result_df["max_total_amount"].describe(
            percentiles=[
                0.50,
                0.90,
                0.95,
                0.99,
                0.999,
            ]
        )
    )

    print("\n窗口交易次数 Top 20：")
    print(
        result_df.sort_values(
            "max_transaction_count",
            ascending=False,
        ).head(20)
    )

    return result_df


# =========================================================
# 3. 10 分钟
# =========================================================

result_10m = analyze_window(
    10 * 60,
    "10 分钟",
    )

# =========================================================
# 4. 30 分钟
# =========================================================

result_30m = analyze_window(
    30 * 60,
    "30 分钟",
    )