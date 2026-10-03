import os
import joblib
import pandas as pd


BASE_DIR = r"D:\GitHub\IEEE-CIS"

MODEL_PATH = os.path.join(
    BASE_DIR,
    "model",
    "model.pkl"
)

FEATURE_PATH = os.path.join(
    BASE_DIR,
    "model",
    "test_features.pkl"
)


print("=" * 70)
print("FinGuard LightGBM 实时推理测试")
print("=" * 70)


# =========================================================
# 1. 加载模型
# =========================================================

print("\n正在加载模型...")

model = joblib.load(MODEL_PATH)

print("模型加载成功")
print(f"模型特征数量：{len(model.feature_name_)}")


# =========================================================
# 2. 加载实时特征
# =========================================================

print("\n正在加载实时特征...")

df = pd.read_pickle(FEATURE_PATH)

print(f"测试数据：{len(df):,}")


# =========================================================
# 3. 获取模型需要的特征
# =========================================================

feature_names = model.feature_name_

X = df[feature_names]


print(f"推理特征数量：{X.shape[1]}")


# =========================================================
# 4. 随机抽取 10 笔进行测试
# =========================================================

sample = X.head(10)

print("\n正在进行模型推理...")

probabilities = model.predict_proba(sample)[:, 1]


# =========================================================
# 5. 输出结果
# =========================================================

print("\n" + "=" * 70)
print("实时推理结果")
print("=" * 70)

for i, probability in enumerate(probabilities, 1):

    if probability >= 0.8:
        risk_level = "HIGH"
    elif probability >= 0.5:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"

    print(
        f"交易 {i:02d} | "
        f"fraud_probability={probability:.6f} | "
        f"risk_level={risk_level}"
    )


print("\n推理测试完成。")