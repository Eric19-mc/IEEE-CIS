import pandas as pd
import lightgbm as lgb

from pathlib import Path
from sklearn.model_selection import train_test_split
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
MODEL_DIR = BASE_DIR / "model" / "output"

MODEL_FILE = MODEL_DIR / "finguard_lgbm_v1.txt"


# =========================
# 读取数据
# =========================

print("========== 读取特征数据 ==========")

df = pd.read_csv(INPUT_FILE)

print("数据规模:", df.shape)


# =========================
# 标签
# =========================

y = df["isFraud"]


# =========================
# 删除不作为模型特征的字段
# =========================

DROP_COLUMNS = [
    "isFraud",
    "TransactionID",
    "TransactionDT",
]


X = df.drop(columns=DROP_COLUMNS)


# =========================
# 类别字段
# =========================

categorical_columns = X.select_dtypes(
    include=["object", "str"]
).columns.tolist()

print("\n类别字段数量:", len(categorical_columns))

print("类别字段:")
print(categorical_columns)


for col in categorical_columns:
    X[col] = X[col].astype("category")


# =========================
# 划分训练集 / 验证集
# =========================

print("\n========== 划分数据 ==========")

X_train, X_valid, y_train, y_valid = train_test_split(
    X,
    y,
    test_size=0.2,
    random_state=42,
    stratify=y,
)

print("训练集:", X_train.shape)
print("验证集:", X_valid.shape)

print(
    "训练集欺诈率:",
    round(y_train.mean() * 100, 4),
    "%"
)

print(
    "验证集欺诈率:",
    round(y_valid.mean() * 100, 4),
    "%"
)


# =========================
# LightGBM
# =========================

print("\n========== 开始训练 LightGBM ==========")

model = lgb.LGBMClassifier(
    objective="binary",

    n_estimators=1000,
    learning_rate=0.05,

    num_leaves=31,
    max_depth=-1,

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

    eval_set=[
        (X_valid, y_valid)
    ],

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


# 默认阈值
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

cm = confusion_matrix(
    y_valid,
    y_pred
)

print(cm)


# =========================
# 特征重要性
# =========================

print("\n========== Top 20 特征 ==========")

importance = pd.DataFrame({
    "feature": X.columns,
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

print("\n========== 保存模型 ==========")

model.booster_.save_model(
    str(MODEL_FILE)
)

print("模型保存完成:")
print(MODEL_FILE)