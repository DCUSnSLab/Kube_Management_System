import pandas as pd
import os

DATA_DIR = "data"
FILE_NAME = "process_classification_experiment10.csv"
file_path = os.path.join(DATA_DIR, FILE_NAME)

df = pd.read_csv(file_path)
df = df[df["pid"] != 1]

# state 컬럼에서 'ProcessStateClassification.' 문자열 제거
df["state"] = df["state"].str.replace("ProcessStateClassification.", "", regex=False)

# pod_name 별로 나열 (state, reason, cpu_activity만 표시)
result = df.groupby("pod_name")[["timestamp", "cpu_activity"]].apply(lambda x: x.reset_index(drop=True))

output_path = "analyze/analyze-process_classification_experiment10.csv"
result.to_csv(output_path)