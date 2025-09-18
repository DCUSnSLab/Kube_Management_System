import pandas as pd
import os

# CSV 파일 불러오기
DATA_DIR = "data"
FILE_NAME = "1_experiment_sleep_classification.csv"  # 분석할 파일 이름
file_path = os.path.join(DATA_DIR, FILE_NAME)

df = pd.read_csv(file_path)

# "python" 프로세스이거나 pid != 1인 프로세스만 필터링
filtered_df = df[(df["comm"] == "python") | (df["pid"] != 1)]

# pod_name 별 요약 통계 (평균, 최대, 최소)
summary_stats = filtered_df.groupby("pod_name")["cpu_activity"].agg(
    avg_activity="mean",
    max_activity="max",
    min_activity="min"
).reset_index()

# 소수점 3자리로 반올림
summary_stats = summary_stats.round(3)

output_path = "analyze/1_analysis_cpu_activity.csv"
summary_stats.to_csv(output_path)