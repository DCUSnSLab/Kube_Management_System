import pandas as pd
import os

DATA_DIR = "data"
FILE_NAME = "experiment_sleep_classification.csv"
file_path = os.path.join(DATA_DIR, FILE_NAME)

df = pd.read_csv(file_path)

# pod_name 별 요약 통계 (평균, 최대, 최소)
summary_stats = df.groupby("pod_name")["cpu_activity"].agg(
    avg_activity="mean",
    max_activity="max",
    min_activity="min"
).reset_index()

summary_stats = summary_stats.round(3)

output_path = "analyze/cpu_activity_analysis.csv"
summary_stats.to_csv(output_path)