import pandas as pd
import os

DATA_DIR = "data"
FILE_NAME = "1_experiment_sleep_classification.csv"
file_path = os.path.join(DATA_DIR, FILE_NAME)

df = pd.read_csv(file_path)

# "python" 프로세스이거나 pid != 1인 프로세스만 필터링
filtered_df = df[(df["comm"] == "python") | (df["pid"] != 1)]

# pod_name 별로 나열 (pid, comm, cpu_activity만 표시)
result = filtered_df.groupby("pod_name")[["pid", "comm", "cpu_activity"]] \
                    .apply(lambda x: x.reset_index(drop=True))

output_path = "analyze/1_analysis_classification.csv"
result.to_csv(output_path)