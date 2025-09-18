import pandas as pd
import os

DATA_DIR = "data"
FILE_NAME = "experiment_sleep_summary.csv"
file_path = os.path.join(DATA_DIR, FILE_NAME)

df = pd.read_csv(file_path)

# === 1. pod_name 별로 첫 번째 검사 결과 제거 ===
df = df.sort_values(["pod_name", "timestamp"])  # 정렬
df = df.groupby("pod_name").apply(lambda g: g.iloc[1:]).reset_index(drop=True)

# 기대 라벨 (expected) = pod_name에 'active' or 'idle' 포함 여부
df["expected"] = df["pod_name"].apply(
    lambda x: "active" if "active" in x else ("idle" if "idle" in x else "unknown")
)

# 실제 분류 라벨 (predicted)
def classify(row):
    if row["active"] > 0:
        return "active"
    elif row["idle"] > 0:
        return "idle"
    else:
        return "other"

df["predicted"] = df.apply(classify, axis=1)

# 혼동 행렬 (Confusion Matrix) 생성
confusion = pd.crosstab(df["expected"], df["predicted"],
                        rownames=["Expected"], colnames=["Predicted"])

output_path = os.path.join("analyze/confusion_matrix.csv")
confusion.to_csv(output_path)