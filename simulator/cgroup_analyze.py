import pandas as pd

# # CSV 파일 불러오기
# df = pd.read_csv("data/cgroup_experiment1.csv")
#
# # timestamp 컬럼을 datetime 타입으로 변환
# df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
#
# # pod_name, timestamp 기준 정렬
# df_sorted = df.sort_values(by=["pod_name", "timestamp"])
#
# # 정렬된 CSV 다시 저장 (원하는 경우)
# df_sorted.to_csv("analyze/cgroup_experiment1_sorted.csv", index=False)



# CSV 불러오기
df = pd.read_csv("analyze/cgroup_experiment1_sorted.csv")

# timestamp datetime 변환
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

# 변화율 계산을 위한 정렬
df = df.sort_values(by=["pod_name", "timestamp"])

# 그룹별 이전 값과 차이 계산
for col in ["memory_current", "io_read_bytes", "io_write_bytes"]:
    df[f"{col}_diff"] = df.groupby("pod_name")[col].diff()

# 시간 차이 (초 단위)
df["time_diff_sec"] = df.groupby("pod_name")["timestamp"].diff().dt.total_seconds()

# 변화율 계산 (Δ값 / Δ시간)
for col in ["memory_current", "io_read_bytes", "io_write_bytes"]:
    df[f"{col}_rate"] = (df[f"{col}_diff"] / df["time_diff_sec"]).round(2)

# 결과 확인
print(df.head())

# # 분석 결과 CSV로 저장 가능
# df.to_csv("analyze/cgroup_experiment1_with_rates.csv", index=False)


import matplotlib.pyplot as plt
df = pd.read_csv("analyze/cgroup_experiment1_with_rates.csv")
df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

df_valid = df.dropna(subset=["memory_current_rate"])

# plt.figure(figsize=(12, 6))
#
# for pod_name, pod_data in df_valid.groupby("pod_name"):
#     plt.plot(pod_data["timestamp"], pod_data["memory_current_rate"], label=pod_name)


# plt.xlabel("Time")
# plt.ylabel("Memory Current Rate (bytes/sec)")
# plt.title("Pod별 Memory Current Rate 변화")
# plt.legend()
# plt.grid(True)
# plt.tight_layout()
# plt.show()


prefix = "active"   # 여기만 바꿔주면 idle, test 등 원하는 그룹만 가능
df_filtered = df_valid[df_valid["pod_name"].str.startswith(prefix)]

# 그래프 크기
plt.figure(figsize=(12, 6))

# 파드별로 그리기
for pod_name, pod_data in df_filtered.groupby("pod_name"):
    plt.plot(pod_data["timestamp"], pod_data["memory_current_rate"], label=pod_name)

plt.xlabel("Time")
plt.ylabel("Memory Current Rate (bytes/sec)")
plt.title(f"Pod Memory Current Rate - prefix: {prefix}")
plt.legend()
plt.grid(True)

ymin = df_filtered["memory_current_rate"].min()
ymax = df_filtered["memory_current_rate"].max()
margin = (ymax - ymin) * 0.2
plt.ylim(-2000, 3000)



plt.tight_layout()
plt.show()
