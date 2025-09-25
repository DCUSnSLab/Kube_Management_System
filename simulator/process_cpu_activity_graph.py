import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def callFile():
    # CSV 불러오기
    file_path = "analyze/analyze-process_classification_experiment10.csv"
    df = pd.read_csv(file_path)

    # 인덱스를 검사 순서로 사용하기 위해 reset_index
    df = df.reset_index(drop=True)

    # 검사 순서 번호 컬럼 추가 (0부터 시작 → 1부터 시작하려면 +1)
    df["check_num"] = df.groupby("pod_name").cumcount() + 1
    return df

def selectPodType(df, type):
    # 🔎 특정 prefix 선택 (예: active, idle 등)
    prefix = type
    df_filtered = df[df["pod_name"].str.startswith(prefix)]
    return df_filtered

def avg_downsampling(df_filtered):
    # ✅ 파드별로 3행씩 평균내서 40개로 축소
    df_downsampled = (
        df_filtered.groupby("pod_name")
        .apply(lambda g: g.groupby(np.arange(len(g)) // 3).agg({
            "check_num": "first",          # 구간 시작 index를 대표로 사용
            "cpu_activity": "mean"         # 3개 평균
        }))
        .reset_index(level=0)
        .reset_index(drop=True)
    )
    return df_downsampled

def showGraph_cpu_activity(df_filtered, prefix):
    # 그래프 크기
    plt.figure(figsize=(16, 10))

    # 파드별 CPU Activity 그래프
    for pod_name, pod_data in df_filtered.groupby("pod_name"):
        plt.plot(pod_data["check_num"], pod_data["cpu_activity"], label=pod_name)


    # min max y축
    plt.ylim(0, 1)

    plt.xlabel("Check Number")
    plt.ylabel("CPU Activity (avg per 3 checks)")
    plt.title(f"Pod CPU Activity Trends (prefix: {prefix})")

    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    df = callFile()
    df = selectPodType(df, 'idle')
    # df_downsampled = avg_downsampling(df)
    showGraph_cpu_activity(df, 'active')
