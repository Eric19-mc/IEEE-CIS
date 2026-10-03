import os
import pandas as pd


# =========================================================
# 基础路径
# =========================================================

BASE_DIR = r"D:\GitHub\IEEE-CIS"
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_PATH = os.path.join(
    BASE_DIR,
    "model",
    "test_features.pkl"
)

TRANSACTION_PATH = os.path.join(
    DATA_DIR,
    "test_transaction.csv"
)

IDENTITY_PATH = os.path.join(
    DATA_DIR,
    "test_identity.csv"
)


# =========================================================
# 交易数据字段
# =========================================================

TRANSACTION_FEATURES = [
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
# Identity 数据字段
#
# 注意：
# CSV 原始字段是 id-01、id-02...
# 读取后统一转换成 id_01、id_02...
# =========================================================

IDENTITY_FEATURES = [
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
# 模型最终使用的 46 个特征
# 必须与 model.pkl 保持一致
# =========================================================

MODEL_FEATURES = [
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
    "transaction_hour",
    "transaction_day",
    "transaction_weekday",
]


# =========================================================
# 类别特征
# 与 preprocess.py 保持一致
# =========================================================

CATEGORICAL_COLUMNS = [
    "ProductCD",
    "card4",
    "card6",
    "P_emaildomain",
    "id_12",
    "id_15",
    "id_16",
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
# Identity 数值特征
# =========================================================

IDENTITY_NUMERIC_COLUMNS = [
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
    "id_28",
    "id_29",
    "id_32",
    "id_34",
]


# =========================================================
# 开始
# =========================================================

print("=" * 70)
print("FinGuard 实时推理数据准备")
print("=" * 70)


# =========================================================
# 1. 读取交易数据
# =========================================================

print("\n正在读取 test_transaction.csv...")

transaction_df = pd.read_csv(
    TRANSACTION_PATH,
    usecols=TRANSACTION_FEATURES
)

print(f"交易数据：{len(transaction_df):,}")


# =========================================================
# 2. 读取 Identity 数据
# =========================================================

print("\n正在读取 test_identity.csv...")

identity_df = pd.read_csv(
    IDENTITY_PATH
)

print(f"Identity 数据：{len(identity_df):,}")


# =========================================================
# 3. 统一 Identity 字段名称
#
# IEEE-CIS 原始 test_identity.csv：
# id-01
# id-02
# ...
#
# 训练阶段：
# id_01
# id_02
# ...
# =========================================================

print("\n正在统一 Identity 字段名称...")

identity_df = identity_df.rename(
    columns={
        col: col.replace("id-", "id_")
        for col in identity_df.columns
        if col.startswith("id-")
    }
)


# =========================================================
# 4. 检查 Identity 字段
# =========================================================

missing_identity_features = [
    col
    for col in IDENTITY_FEATURES
    if col not in identity_df.columns
]

if missing_identity_features:

    print("\n错误：test_identity.csv 缺少模型需要的字段：")

    for col in missing_identity_features:
        print(f" - {col}")

    raise ValueError(
        "test_identity.csv 与训练阶段 Identity 特征不一致"
    )


identity_df = identity_df[
    IDENTITY_FEATURES
]


# =========================================================
# 5. Identity 数值字段类型统一
# =========================================================

print("正在处理 Identity 数值字段...")

for col in IDENTITY_NUMERIC_COLUMNS:

    identity_df[col] = pd.to_numeric(
        identity_df[col],
        errors="coerce"
    )


# =========================================================
# 6. 合并 Transaction + Identity
# =========================================================

print("\n正在进行 TransactionID 关联...")

df = transaction_df.merge(
    identity_df,
    on="TransactionID",
    how="left"
)

print(f"关联后数据：{len(df):,}")


# =========================================================
# 7. 检查 TransactionID 是否发生重复
# =========================================================

duplicate_count = df["TransactionID"].duplicated().sum()

print(f"TransactionID 重复记录：{duplicate_count:,}")

if duplicate_count > 0:

    raise ValueError(
        "TransactionID 关联后出现重复，禁止继续生成实时特征"
    )


# =========================================================
# 8. 构造时间特征
#
# 与 preprocess.py 保持完全一致
# =========================================================

print("\n正在构造时间特征...")

df["transaction_hour"] = (
                                 df["TransactionDT"] // 3600
                         ) % 24

df["transaction_day"] = (
        df["TransactionDT"] // 86400
)

df["transaction_weekday"] = (
                                    df["transaction_day"] + 4
                            ) % 7


# =========================================================
# 9. 类别特征处理
#
# 与 preprocess.py 保持一致：
# NaN → Missing → category
# =========================================================

print("正在处理类别特征...")

for col in CATEGORICAL_COLUMNS:

    if col not in df.columns:
        raise ValueError(
            f"缺少类别特征：{col}"
        )

    df[col] = (
        df[col]
        .fillna("Missing")
        .astype("category")
    )


# =========================================================
# 10. 检查模型特征是否完整
# =========================================================

print("\n正在检查模型特征...")

missing_model_features = [
    col
    for col in MODEL_FEATURES
    if col not in df.columns
]

if missing_model_features:

    print("\n错误：缺少模型特征：")

    for col in missing_model_features:
        print(f" - {col}")

    raise ValueError(
        "实时特征与 model.pkl 不一致"
    )


# =========================================================
# 11. 调整字段顺序
#
# TransactionID 只用于关联/追踪
# 不参与模型预测
# =========================================================

df = df[
    ["TransactionID"] + MODEL_FEATURES
    ]


# =========================================================
# 12. 最终数据类型检查
# =========================================================

print("\n正在检查数据类型...")

bad_dtypes = []

for col in MODEL_FEATURES:

    dtype = df[col].dtype

    if not (
            pd.api.types.is_numeric_dtype(dtype)
            or pd.api.types.is_bool_dtype(dtype)
            or isinstance(dtype, pd.CategoricalDtype)
    ):
        bad_dtypes.append(
            (col, str(dtype))
        )


if bad_dtypes:

    print("\n错误：发现不支持的数据类型：")

    for col, dtype in bad_dtypes:
        print(f" - {col}: {dtype}")

    raise ValueError(
        "实时特征存在不兼容的数据类型"
    )


# =========================================================
# 13. 最终检查
# =========================================================

print("\n" + "=" * 70)
print("实时特征检查")
print("=" * 70)

print(f"记录数：{len(df):,}")
print(f"字段数：{len(df.columns)}")
print(f"模型特征数：{len(MODEL_FEATURES)}")


# =========================================================
# 14. 输出模型特征
# =========================================================

print("\n模型特征：")

for i, col in enumerate(MODEL_FEATURES, 1):

    print(
        f"{i:02d}. {col:<25} "
        f"{str(df[col].dtype)}"
    )


# =========================================================
# 15. 保存
# =========================================================

print("\n正在保存实时特征...")

df.to_pickle(
    OUTPUT_PATH
)

print("\n输出文件：")
print(OUTPUT_PATH)

print("\n" + "=" * 70)
print("实时特征准备完成。")
print("=" * 70)