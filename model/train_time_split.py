import pandas as pd
import lightgbm as lgb

from pathlib import Path
from sklearn.metrics import (
    roc_auc_score,
    average_precision_score,
    precision_score,
    recall_score,
    f1_score,
    confusion_matrix,
)


# =========================
# 路径
# =========================

BASE_DIR = Path(__file__).resolve().parent.parent

INPUT_FILE = BASE_DIR / "model" / "output" / "train_features.csv"
MODEL_FILE = BASE_DIR / "model" / "output" / "finguard_lgbm_v2.txt"


# =========================
# 读取数据
# =========================

print("========== 读取数据 ==========")

df = pd.read_csv(INPUT_FILE)

print("数据规模:", df.shape)


# =========================
# 按时间划分
# =========================

TRAIN_MAX_DAY = 145

train_df = df[
    df["transaction_day"] <= TRAIN_MAX_DAY
    ].copy()

valid_df = df[
    df["transaction_day"] > TRAIN_MAX_DAY
    ].copy()


print("\n========== 时间切分 ==========")

print(
    "训练集:",
    train_df.shape,
    "Day:",
    train_df["transaction_day"].min(),
    "~",
    train_df["transaction_day"].max()
)

print(
    "验证集:",
    valid_df.shape,
    "Day:",
    valid_df["transaction_day"].min(),
    "~",
    valid_df["transaction_day"].max()
)


# =========================
# 标签
# =========================

y_train = train_df["isFraud"]
y_valid = valid_df["isFraud"]


print(
    "\n训练集欺诈率:",
    round(y_train.mean() * 100, 4),
    "%"
)

print(
    "验证集欺诈率:",
    round(y_valid.mean() * 100, 4),
    "%"
)


# =========================
# 删除字段
# =========================

DROP_COLUMNS = [
    "isFraud",
    "TransactionID",
    "TransactionDT",
]


X_train = train_df.drop(
    columns=DROP_COLUMNS
)

X_valid = valid_df.drop(
    columns=DROP_COLUMNS
)


# =========================
# 类别字段
# =========================

categorical_columns = X_train.select_dtypes(
    include=["object", "str"]
).columns.tolist()


print(
    "\n类别字段数量:",
    len(categorical_columns)
)


for col in categorical_columns:
    X_train[col] = X_train[col].astype("category")
    X_valid[col] = X_valid[col].astype("category")

    # 确保训练和验证拥有一致的类别集合
    X_valid[col] = X_valid[col].cat.set_categories(
        X_train[col].cat.categories
    )


# =========================
# LightGBM
# =========================

print("\n========== 开始训练 V2 ==========")

model = lgb.LGBMClassifier(
    objective="binary",

    n_estimators=1000,
    learning_rate=0.05,

    num_leaves=31,

    subsample=0.8,
    colsample_bytree=0.8,

    reg_alpha=0.1,
    reg_lambda=0.1,

    random_state=42,

    n_jobs=-1,
)


model.fit(
    X_train,
    y_train,

    categorical_feature=categorical_columns,

    eval_X=X_valid,
    eval_y=y_valid,

    callbacks=[
        lgb.early_stopping(
            50,
            verbose=True
        )
    ],
)


# =========================
# 预测
# =========================

print("\n========== 模型评估 ==========")

y_prob = model.predict_proba(
    X_valid
)[:, 1]


threshold = 0.5

y_pred = (
        y_prob >= threshold
).astype(int)


# =========================
# 指标
# =========================

roc_auc = roc_auc_score(
    y_valid,
    y_prob
)

pr_auc = average_precision_score(
    y_valid,
    y_prob
)

precision = precision_score(
    y_valid,
    y_pred,
    zero_division=0
)

recall = recall_score(
    y_valid,
    y_pred,
    zero_division=0
)

f1 = f1_score(
    y_valid,
    y_pred,
    zero_division=0
)


print("\nROC-AUC :", round(roc_auc, 6))
print("PR-AUC  :", round(pr_auc, 6))
print("Precision:", round(precision, 6))
print("Recall   :", round(recall, 6))
print("F1       :", round(f1, 6))


# =========================
# 混淆矩阵
# =========================

print("\n========== 混淆矩阵 ==========")

print(
    confusion_matrix(
        y_valid,
        y_pred
    )
)


# =========================
# 特征重要性
# =========================

print("\n========== Top 20 特征 ==========")

importance = pd.DataFrame({
    "feature": X_train.columns,
    "importance": model.feature_importances_,
})

importance = importance.sort_values(
    "importance",
    ascending=False
)

print(
    importance.head(20).to_string(
        index=False
    )
)


# =========================
# 保存模型
# =========================

model.booster_.save_model(
    str(MODEL_FILE)
)

print("\n模型保存完成:")
print(MODEL_FILE)