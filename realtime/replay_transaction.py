import os
import random
import time

import pandas as pd

from kafka_producer import (
    create_producer,
    send_transaction,
    close_producer,
)


# =========================================================
# 配置
# =========================================================

BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

TRANSACTION_FILE = os.path.join(
    BASE_DIR,
    "data",
    "test_transaction.csv",
)

IDENTITY_FILE = os.path.join(
    BASE_DIR,
    "data",
    "test_identity.csv",
)


# =========================================================
# 回放配置
# =========================================================

# None = 本次把全部数据跑完
MAX_RECORDS = None

# 是否循环回放
# False = 全部数据跑完后自动停止
LOOP = False

# 每条交易之间的随机间隔
#
# 为了最终项目收尾，加快回放速度。
# 仍然保留随机间隔，模拟实时交易流。
MIN_INTERVAL = 0.01
MAX_INTERVAL = 0.03


# =========================================================
# 模型需要的原始交易字段
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


# =========================================================
# 模型需要的身份/设备字段
#
# 注意：
# test_identity.csv 原始字段是 id-01、id-02...
# 后面统一转换成 id_01、id_02...
# =========================================================

IDENTITY_FIELDS = [
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
# 数值字段
#
# Kafka JSON 中尽量转换成正常的 int / float / None
# =========================================================

INTEGER_FIELDS = {
    "TransactionID",
    "TransactionDT",
}

FLOAT_FIELDS = {
    "TransactionAmt",
    "card1",
    "card2",
    "card3",
    "card5",
    "addr1",
    "addr2",
    "dist1",
    "id_01",
    "id_02",
    "id_03",
    "id_04",
    "id_05",
    "id_06",
    "id_09",
    "id_10",
    "id_11",
    "id_13",
    "id_14",
    "id_17",
    "id_19",
    "id_20",
    "id_32",
}


# =========================================================
# 数据读取
# =========================================================

def load_data():

    print("正在读取 test_transaction.csv...")

    transaction_df = pd.read_csv(
        TRANSACTION_FILE,
        usecols=TRANSACTION_FIELDS,
        low_memory=False,
    )

    print(
        f"交易数据读取完成："
        f"{len(transaction_df)} 条"
    )

    print(
        "正在读取 test_identity.csv..."
    )

    identity_df = pd.read_csv(
        IDENTITY_FILE,
        low_memory=False,
    )

    print(
        f"身份数据读取完成："
        f"{len(identity_df)} 条"
    )

    # =====================================================
    # 统一 identity 字段命名
    #
    # 原始：
    # id-01
    # id-02
    #
    # 统一：
    # id_01
    # id_02
    # =====================================================

    identity_df = identity_df.rename(
        columns={
            column: column.replace(
                "-",
                "_",
            )
            for column in identity_df.columns
            if column.startswith("id-")
        }
    )

    # =====================================================
    # 检查交易字段
    # =====================================================

    missing_transaction_fields = [
        field
        for field in TRANSACTION_FIELDS
        if field not in transaction_df.columns
    ]

    if missing_transaction_fields:

        raise ValueError(
            "test_transaction.csv 缺少字段："
            + ", ".join(
                missing_transaction_fields
            )
        )

    # =====================================================
    # 检查身份字段
    # =====================================================

    missing_identity_fields = [
        field
        for field in IDENTITY_FIELDS
        if field not in identity_df.columns
    ]

    if missing_identity_fields:

        raise ValueError(
            "test_identity.csv 缺少字段："
            + ", ".join(
                missing_identity_fields
            )
        )

    # =====================================================
    # 只保留模型需要的 identity 字段
    # =====================================================

    identity_df = identity_df[
        IDENTITY_FIELDS
    ]

    # =====================================================
    # TransactionID 类型统一
    # =====================================================

    transaction_df[
        "TransactionID"
    ] = pd.to_numeric(
        transaction_df[
            "TransactionID"
        ],
        errors="coerce",
    )

    identity_df[
        "TransactionID"
    ] = pd.to_numeric(
        identity_df[
            "TransactionID"
        ],
        errors="coerce",
    )

    # =====================================================
    # 删除无效 TransactionID
    # =====================================================

    transaction_df = transaction_df.dropna(
        subset=[
            "TransactionID"
        ]
    )

    identity_df = identity_df.dropna(
        subset=[
            "TransactionID"
        ]
    )

    transaction_df[
        "TransactionID"
    ] = transaction_df[
        "TransactionID"
    ].astype(
        "int64"
    )

    identity_df[
        "TransactionID"
    ] = identity_df[
        "TransactionID"
    ].astype(
        "int64"
    )

    # =====================================================
    # 检查 identity TransactionID 是否重复
    # =====================================================

    duplicate_identity = identity_df[
        identity_df[
            "TransactionID"
        ].duplicated(
            keep=False
        )
    ]

    if len(
            duplicate_identity
    ) > 0:

        raise ValueError(
            "test_identity.csv 存在重复 "
            f"TransactionID："
            f"{len(duplicate_identity)} 条"
        )

    # =====================================================
    # 关联 transaction + identity
    # =====================================================

    df = transaction_df.merge(
        identity_df,
        on="TransactionID",
        how="left",
    )

    # =====================================================
    # 检查关联结果
    # =====================================================

    if len(df) != len(
            transaction_df
    ):

        raise ValueError(
            "TransactionID 关联后交易数量发生变化，"
            "请检查 identity 数据。"
        )

    print(
        f"交易与身份数据关联完成："
        f"{len(df)} 条"
    )

    identity_matched = (
        df["DeviceType"].notna().sum()
    )

    print(
        "成功关联身份/设备信息："
        f"{identity_matched} 条"
    )

    print(
        "未关联身份/设备信息："
        f"{len(df) - identity_matched} 条"
    )

    return df


# =========================================================
# 清洗单条交易
# =========================================================

def build_transaction(row):
    """
    将 Pandas 行转换成 Kafka JSON。

    注意：
    1. 不发送 isFraud
    2. 不修改原始 CSV
    3. NaN -> None
    4. id-XX 已经统一为 id_XX
    5. 保留模型所需的全部原始字段
    """

    transaction = {}

    for field in row.index:

        value = row[field]

        # =================================================
        # Pandas NaN / NA -> None
        # =================================================

        if pd.isna(value):

            transaction[field] = None

            continue

        # =================================================
        # TransactionID / TransactionDT
        # =================================================

        if field in INTEGER_FIELDS:

            transaction[field] = int(
                value
            )

            continue

        # =================================================
        # 数值字段
        # =================================================

        if field in FLOAT_FIELDS:

            transaction[field] = float(
                value
            )

            continue

        # =================================================
        # 其他字段统一转字符串
        # =================================================

        transaction[field] = str(
            value
        )

    return transaction


# =========================================================
# 实时回放
# =========================================================

def replay(df):

    producer = create_producer()

    try:

        total = 0

        while True:

            print(
                "\n开始新的实时交易回放"
            )

            print(
                "=" * 60
            )

            # =================================================
            # 确定本轮回放范围
            # =================================================

            if MAX_RECORDS is None:

                end_index = len(df)

            else:

                end_index = min(
                    MAX_RECORDS,
                    len(df),
                )

            print(
                f"本轮计划回放："
                f"{end_index} 条"
            )

            # =================================================
            # 从第 0 条开始
            # 直到数据末尾
            # =================================================

            for index in range(
                    end_index
            ):

                row = df.iloc[
                    index
                ]

                transaction = build_transaction(
                    row
                )

                # =================================================
                # 发送 Kafka
                # =================================================

                send_transaction(
                    producer,
                    transaction,
                )

                total += 1

                # =================================================
                # 控制台
                # =================================================

                print(
                    f"[实时交易] "
                    f"Index={index} | "
                    f"TransactionID="
                    f"{transaction['TransactionID']} "
                    f"| Amount="
                    f"{transaction['TransactionAmt']} "
                    f"| ProductCD="
                    f"{transaction['ProductCD']} "
                    f"| Total={total}"
                )

                # =================================================
                # 随机模拟交易间隔
                # =================================================

                time.sleep(
                    random.uniform(
                        MIN_INTERVAL,
                        MAX_INTERVAL,
                    )
                )

            # =====================================================
            # 非循环模式
            # =====================================================

            if not LOOP:

                print(
                    "\n全部数据回放完成。"
                )

                print(
                    f"本次共发送："
                    f"{total} 条交易"
                )

                break

            # =====================================================
            # 循环模式
            # =====================================================

            total = 0

            print(
                "\n本轮回放完成，"
                "2 秒后重新开始..."
            )

            time.sleep(2)

    except KeyboardInterrupt:

        print(
            "\n检测到 Ctrl+C，"
            "停止实时交易回放..."
        )

    finally:

        close_producer(
            producer
        )

        print(
            "Kafka Producer 已关闭"
        )


# =========================================================
# Main
# =========================================================

if __name__ == "__main__":

    print("=" * 60)
    print("FinGuard 实时交易模拟器")
    print("=" * 60)

    print(
        f"交易数据文件："
        f"{TRANSACTION_FILE}"
    )

    print(
        f"身份数据文件："
        f"{IDENTITY_FILE}"
    )

    df = load_data()

    print(
        f"\n最终可回放交易数量："
        f"{len(df)}"
    )

    if MAX_RECORDS is None:

        print(
            "本轮最大回放数量：全部数据"
        )

    else:

        print(
            f"本轮最大回放数量："
            f"{MAX_RECORDS}"
        )

    print(
        f"循环回放："
        f"{'开启' if LOOP else '关闭'}"
    )

    print(
        f"交易间隔："
        f"{MIN_INTERVAL} ~ "
        f"{MAX_INTERVAL} 秒"
    )

    replay(df)