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

DATA_DIR = os.path.join(
    BASE_DIR,
    "data",
)

TRANSACTION_FILE = os.path.join(
    DATA_DIR,
    "test_transaction.csv",
)

IDENTITY_FILE = os.path.join(
    DATA_DIR,
    "test_identity.csv",
)


# =========================================================
# 参数
# =========================================================

WINDOW_SECONDS = 10 * 60


# =========================================================
# 字段
# =========================================================

TRANSACTION_FIELDS = [
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
]

IDENTITY_FIELDS = [
    "TransactionID",
    "id-01",
    "id-02",
    "id-03",
    "id-04",
    "id-05",
    "id-06",
    "id-09",
    "id-10",
    "id-11",
    "id-12",
    "id-13",
    "id-14",
    "id-15",
    "id-16",
    "id-17",
    "id-19",
    "id-20",
    "id-28",
    "id-29",
    "id-30",
    "id-31",
    "id-32",
    "id-33",
    "id-34",
    "id-35",
    "id-36",
    "id-37",
    "id-38",
    "DeviceType",
    "DeviceInfo",
]


# =========================================================
# 构造 subject_key
# =========================================================

def build_subject_key(df):

    fields = [
        "card1",
        "card2",
        "card3",
        "card5",
        "card6",
        "addr1",
        "addr2",
    ]

    for column in fields:

        if column not in df.columns:
            df[column] = "NA"

        df[column] = (
            df[column]
            .fillna("NA")
            .astype(str)
        )

    df["subject_key"] = (
            df["card1"] + "_"
            + df["card2"] + "_"
            + df["card3"] + "_"
            + df["card5"] + "_"
            + df["card6"] + "_"
            + df["addr1"] + "_"
            + df["addr2"]
    )

    return df


# =========================================================
# 主程序
# =========================================================

def main():

    print("=" * 60)
    print("FinGuard 行为异常真实样本分析")
    print("=" * 60)

    # =====================================================
    # 读取交易数据
    # =====================================================

    print()
    print("读取 test_transaction.csv...")

    transaction = pd.read_csv(
        TRANSACTION_FILE,
        usecols=TRANSACTION_FIELDS,
    )

    print(
        f"交易数量：{len(transaction):,}"
    )

    # =====================================================
    # 读取 Identity
    # =====================================================

    print()
    print("读取 test_identity.csv...")

    identity = pd.read_csv(
        IDENTITY_FILE,
        usecols=IDENTITY_FIELDS,
    )

    print(
        f"Identity 数量：{len(identity):,}"
    )

    # =====================================================
    # identity 字段改成实时项目使用的 id_XX
    # =====================================================

    identity = identity.rename(
        columns={
            column: column.replace("-", "_")
            for column in identity.columns
            if column.startswith("id-")
        }
    )

    # =====================================================
    # 检查 Identity TransactionID 是否重复
    # =====================================================

    duplicate_identity = identity[
        identity["TransactionID"].duplicated(
            keep=False
        )
    ]

    if not duplicate_identity.empty:

        print()
        print(
            "警告：test_identity.csv 存在重复 TransactionID"
        )

        print(
            f"重复行数：{len(duplicate_identity)}"
        )

        identity = identity.drop_duplicates(
            subset=["TransactionID"],
            keep="first",
        )

    # =====================================================
    # 合并
    # =====================================================

    print()
    print("合并 transaction + identity...")

    data = transaction.merge(
        identity,
        on="TransactionID",
        how="left",
    )

    print(
        f"合并后数据：{len(data):,}"
    )

    # =====================================================
    # 构造 subject_key
    # =====================================================

    data = build_subject_key(data)

    # =====================================================
    # 排序
    # =====================================================

    data = data.sort_values(
        [
            "subject_key",
            "TransactionDT",
        ]
    ).reset_index(drop=True)

    # =====================================================
    # 寻找10分钟内至少3笔交易
    # =====================================================

    cases = []

    for subject_key, group in data.groupby(
            "subject_key",
            sort=False,
    ):

        if len(group) < 3:
            continue

        group = group.sort_values(
            "TransactionDT"
        ).reset_index(drop=True)

        rows = group.to_dict(
            "records"
        )

        left = 0

        for right in range(len(rows)):

            current_time = rows[right][
                "TransactionDT"
            ]

            while (
                    current_time
                    - rows[left]["TransactionDT"]
                    > WINDOW_SECONDS
            ):
                left += 1

            count = right - left + 1

            if count >= 3:

                window_rows = rows[
                    left:right + 1
                ]

                amounts = [
                    float(row["TransactionAmt"])
                    for row in window_rows
                ]

                total_amount = sum(
                    amounts
                )

                cases.append(
                    {
                        "subject_key": subject_key,
                        "transaction_count": count,
                        "total_amount": total_amount,
                        "max_amount": max(amounts),
                        "start_dt": rows[left][
                            "TransactionDT"
                        ],
                        "end_dt": current_time,
                        "transactions": window_rows,
                    }
                )

                # 每个主体只保存第一组符合条件的窗口
                break

    # =====================================================
    # 没找到
    # =====================================================

    if not cases:

        print()
        print(
            "没有找到10分钟内至少3笔交易的 subject_key。"
        )

        return

    # =====================================================
    # 排序
    # =====================================================

    cases.sort(
        key=lambda x: (
            x["transaction_count"],
            x["total_amount"],
        ),
        reverse=True,
    )

    # =====================================================
    # 输出候选
    # =====================================================

    print()
    print(
        f"找到 {len(cases)} 个可用于行为测试的主体"
    )

    print()
    print("=" * 60)
    print("Top 10 行为异常候选")
    print("=" * 60)

    for index, case in enumerate(
            cases[:10],
            start=1,
    ):

        print()
        print(
            f"[{index}] "
            f"交易数={case['transaction_count']} | "
            f"累计金额={case['total_amount']:.2f} | "
            f"最大金额={case['max_amount']:.2f}"
        )

        print(
            f"subject_key={case['subject_key']}"
        )

        print(
            f"TransactionDT="
            f"{case['start_dt']} -> "
            f"{case['end_dt']}"
        )

        print("TransactionID：")

        print(
            [
                row["TransactionID"]
                for row in case["transactions"]
            ]
        )

    # =====================================================
    # 保存最佳测试样本
    # =====================================================

    best = cases[0]

    output_file = os.path.join(
        BASE_DIR,
        "realtime",
        "behavior_test_case.csv",
    )

    output_rows = pd.DataFrame(
        best["transactions"]
    )

    # -----------------------------------------------------
    # 删除不应该进入实时链路的字段
    # -----------------------------------------------------

    if "isFraud" in output_rows.columns:

        output_rows = output_rows.drop(
            columns=["isFraud"]
        )

    # -----------------------------------------------------
    # subject_key 放最后，方便检查
    # -----------------------------------------------------

    output_rows.to_csv(
        output_file,
        index=False,
        encoding="utf-8-sig",
    )

    # =====================================================
    # 输出结果
    # =====================================================

    print()
    print("=" * 60)
    print("测试样本已保存")
    print("=" * 60)

    print(output_file)

    print()
    print(
        f"交易数量：{best['transaction_count']}"
    )

    print(
        f"累计金额：{best['total_amount']:.2f}"
    )

    print(
        f"最大金额：{best['max_amount']:.2f}"
    )

    print(
        f"subject_key：{best['subject_key']}"
    )

    print()
    print("测试 TransactionID：")

    print(
        [
            row["TransactionID"]
            for row in best["transactions"]
        ]
    )

    print()
    print("保存字段：")

    print(
        list(output_rows.columns)
    )


if __name__ == "__main__":
    main()