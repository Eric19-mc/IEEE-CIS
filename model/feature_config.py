import joblib

MODEL_PATH = r"D:\GitHub\IEEE-CIS\model\model.pkl"

model = joblib.load(MODEL_PATH)

print("=" * 70)
print("FinGuard 模型特征")
print("=" * 70)

print(f"特征数量：{len(model.feature_name_)}")

for i, feature in enumerate(model.feature_name_, 1):
    print(f"{i:02d}. {feature}")