import os
import pickle

import lightgbm as lgb
import pandas as pd

from sklearn.metrics import (
    average_precision_score,
    classification_report,
    precision_score,
    recall_score,
    roc_auc_score,
)


# =========================================================
# 路径
# =========================================================

BASE_DIR = os.path.dirname(
    os.path.dirname(
        os.path.abspath(__file__)
    )
)

FEATURE_FILE = os.path.join(
    BASE_DIR,
    "model",
    "train_features.pkl",
)

MODEL_FILE = os.path.join(
    BASE_DIR,
    "model",
    "model.pkl",
)


print("=" * 70)
print("FinGuard LightGBM 模型训练")
print("=" * 70)


# =========================================================
# 1. 读取数据
# =========================================================

print("\n正在读取预处理数据...")

df = pd.read_pickle(
    FEATURE_FILE
)

print(
    f"数据量：{len(df):,}"
)

print(
    f"字段数量：{len(df.columns)}"
)


# =========================================================
# 2. 按时间排序
# =========================================================

print("\n正在按照 TransactionDT 排序...")

df = df.sort_values(
    "TransactionDT"
).reset_index(
    drop=True
)


# =========================================================
# 3. 构造 X / y
# =========================================================

TARGET = "isFraud"

DROP_COLUMNS = [
    "isFraud",
    "TransactionID",
]


X = df.drop(
    columns=DROP_COLUMNS
)

y = df[TARGET]


print(
    f"\n模型特征数量：{X.shape[1]}"
)

print(
    f"欺诈样本：{int(y.sum()):,}"
)

print(
    f"欺诈比例：{y.mean():.4%}"
)


# =========================================================
# 4. 时间划分
# =========================================================

split_index = int(
    len(df) * 0.8
)

X_train = X.iloc[
    :split_index
]

X_valid = X.iloc[
    split_index:
]

y_train = y.iloc[
    :split_index
]

y_valid = y.iloc[
    split_index:
]


print("\n" + "=" * 70)
print("数据集划分")
print("=" * 70)

print(
    f"训练集：{len(X_train):,}"
)

print(
    f"验证集：{len(X_valid):,}"
)

print(
    f"训练集欺诈率：{y_train.mean():.4%}"
)

print(
    f"验证集欺诈率：{y_valid.mean():.4%}"
)


# =========================================================
# 5. LightGBM
# =========================================================

print("\n" + "=" * 70)
print("开始训练 LightGBM")
print("=" * 70)


model = lgb.LGBMClassifier(
    objective="binary",
    n_estimators=1000,
    learning_rate=0.05,
    num_leaves=64,
    max_depth=-1,
    subsample=0.8,
    colsample_bytree=0.8,
    reg_alpha=0.1,
    reg_lambda=0.1,
    random_state=42,
    n_jobs=-1,
)


# =========================================================
# 6. 训练
# =========================================================

model.fit(
    X_train,
    y_train,

    eval_set=[
        (
            X_valid,
            y_valid,
        )
    ],

    callbacks=[
        lgb.early_stopping(
            50
        ),
        lgb.log_evaluation(
            50
        ),
    ],
)


# =========================================================
# 7. 预测
# =========================================================

print("\n正在进行验证集预测...")

probability = model.predict_proba(
    X_valid
)[:, 1]


# =========================================================
# 8. 指标
# =========================================================

auc = roc_auc_score(
    y_valid,
    probability,
)

pr_auc = average_precision_score(
    y_valid,
    probability,
)


# 使用 0.5 作为基础分类阈值
prediction = (
        probability >= 0.5
).astype(int)


precision = precision_score(
    y_valid,
    prediction,
    zero_division=0,
)

recall = recall_score(
    y_valid,
    prediction,
    zero_division=0,
)


print("\n" + "=" * 70)
print("模型评估")
print("=" * 70)

print(
    f"AUC：{auc:.6f}"
)

print(
    f"PR-AUC：{pr_auc:.6f}"
)

print(
    f"Precision：{precision:.6f}"
)

print(
    f"Recall：{recall:.6f}"
)


print("\n分类报告：")

print(
    classification_report(
        y_valid,
        prediction,
        digits=4,
        zero_division=0,
    )
)


# =========================================================
# 9. 特征重要性
# =========================================================

print("\n" + "=" * 70)
print("Top 30 特征重要性")
print("=" * 70)

importance = pd.DataFrame(
    {
        "feature": X.columns,
        "importance": model.feature_importances_,
    }
).sort_values(
    "importance",
    ascending=False,
)

print(
    importance.head(30).to_string(
        index=False
    )
)


# =========================================================
# 10. 保存模型
# =========================================================

print("\n正在保存模型...")

with open(
        MODEL_FILE,
        "wb",
) as f:

    pickle.dump(
        model,
        f,
    )


print(
    f"模型已保存："
    f"{MODEL_FILE}"
)

print("\n训练完成。")