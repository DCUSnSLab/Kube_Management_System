import pandas as pd
import os
import glob

PROCESS_HEADERS = [
    "pod_name", "timestamp", "pid", "comm", "state", "ppid", "pgrp", "session", "tty_nr", "tpgid", "flags",
    "minflt", "cminflt", "majflt", "cmajflt", "utime", "stime", "cutime", "cstime", "priority", "nice",
    "num_threads", "itrealvalue", "starttime", "vsize", "rss", "rsslim", "startcode", "endcode",
    "startstack", "kstkesp", "kstkeip", "signal", "blocked", "sigignore", "sigcatch", "wchan", "nswap",
    "cnswap", "exit_signal", "processor", "rt_priority", "policy", "delayacct_blkio_ticks", "guest_time",
    "cguest_time", "start_data", "end_data", "start_brk", "arg_start", "arg_end", "env_start", "env_end",
    "exit_code", "voluntary_ctxt_switches", "nonvoluntary_ctxt_switches", "vm_rss_status",
    "read_bytes", "write_bytes"
]

CGROUP_HEADERS = [
    "pod_name", "timestamp", "memory_current", "memory_limit", "io_read_bytes", "io_write_bytes"
]

CLASSIFICATION_KEYS_ORDER = [
    "pod_name", "timestamp", "pid", "comm", "role", "state", "score", "reason"
]

SUMMARY_KEYS_ORDER = [
    "pod_name", "timestamp", "total", "active_cnt", "idle_cnt", "running_cnt", "bg_active_cnt", "note"
]

def load_data(file_path: str) -> pd.DataFrame:
    """Load CSV and filter out pid=1"""
    df = pd.read_csv(file_path)
    df = df[df["pid"] != 1]  # pid=1 제거
    return df

def add_process_headers(folder, pattern: str="process_metrics_experiment*.csv"):
    """프로세스 메트릭 파일에 헤더 추가"""
    file_pattern = os.path.join(folder, pattern)
    for file in glob.glob(file_pattern):
        try:
            df = pd.read_csv(file, header=None, names=PROCESS_HEADERS)
            df.to_csv(file, index=False)
            print(f"헤더 추가 완료: {file}")
        except Exception as e:
            print(f"파일 처리 실패: {file}, 에러: {e}")

def add_cgroup_headers(folder, pattern: str="cgroup_experiment*.csv"):
    """cgroup 메트릭 파일에 헤더 추가"""
    file_pattern = os.path.join(folder, pattern)
    for file in glob.glob(file_pattern):
        try:
            df = pd.read_csv(file, header=None, names=CGROUP_HEADERS)
            df.to_csv(file, index=False)
            print(f"헤더 추가 완료: {file}")
        except Exception as e:
            print(f"파일 처리 실패: {file}, 에러: {e}")



def add_classification_headers(folder, pattern: str="process_classification_experiment*.csv"):
    """프로세스 분류 파일에 헤더 추가"""
    file_pattern = os.path.join(folder, pattern)
    for file in glob.glob(file_pattern):
        try:
            df = pd.read_csv(file, header=None, names=CLASSIFICATION_KEYS_ORDER)
            df.to_csv(file, index=False)
            print(f"헤더 추가 완료: {file}")
        except Exception as e:
            print(f"파일 처리 실패: {file}, 에러: {e}")

def add_summary_headers(folder, pattern: str="process_summary_experiment*.csv"):
    """요약 파일에 헤더 추가"""
    file_pattern = os.path.join(folder, pattern)
    for file in glob.glob(file_pattern):
        try:
            df = pd.read_csv(file, header=None, names=SUMMARY_KEYS_ORDER)
            df.to_csv(file, index=False)
            print(f"헤더 추가 완료: {file}")
        except Exception as e:
            print(f"파일 처리 실패: {file}, 에러: {e}")

def convert_to_excel(df: pd.DataFrame, output_path: str):
    """Save dataframe to Excel file"""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_excel(output_path, index=False)
    print(f"엑셀 저장 완료: {output_path}")

def process_data(df: pd.DataFrame) -> pd.DataFrame:
    """Sort, select required columns, and simplify comm"""
    # timestamp 변환 + 정렬
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.sort_values(by=["pod_name", "timestamp"])

    # 필요한 컬럼만 남기기
    keep_cols = [
        "pod_name", "timestamp", "comm", "state",
        "minflt", "majflt", "utime", "stime", "rss",
        "voluntary_ctxt_switches", "nonvoluntary_ctxt_switches",
        "vm_rss_status", "read_bytes", "write_bytes"
    ]
    df = df[keep_cols]
    df["timestamp"] = df["timestamp"].dt.strftime("%H:%M:%S")

    # comm 단순화
    df["comm"] = df["comm"].apply(
        lambda x: os.path.splitext(
            os.path.basename(str(x).replace("python ./programs/", ""))
        )[0]
    )
    return df

def align_timestamps(df: pd.DataFrame, base_pod: str = "active-0") -> pd.DataFrame:
    """
    기준 pod_name의 timestamp로 모든 pod의 timestamp를 동일하게 맞춤
    """
    # 기준 파드 timestamp 추출
    base_timestamps = (
        df[df["pod_name"] == base_pod]
        .sort_values("timestamp")["timestamp"]
        .reset_index(drop=True)
    )

    # 각 파드 그룹에 대해 순서대로 timestamp 덮어쓰기
    def replace_ts(group):
        n = len(group)
        group = group.sort_values("timestamp").reset_index(drop=True)
        # 기준 파드 timestamp 길이에 맞게 잘라내거나 반복
        group["timestamp"] = base_timestamps.iloc[:n].values
        return group

    df_aligned = df.groupby("pod_name", group_keys=False).apply(replace_ts)
    return df_aligned

def save_data(df: pd.DataFrame, output_path: str):
    """Save dataframe to CSV"""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"저장 완료: {output_path}")

def add_deltas(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add delta (difference from previous row) columns for cumulative metrics.
    Grouped by pod_name to avoid mixing between different pods.
    """
    cumulative_cols = [
        "minflt", "majflt", "utime", "stime", "rss",
        "voluntary_ctxt_switches", "nonvoluntary_ctxt_switches",
        "vm_rss_status", "read_bytes", "write_bytes"
    ]

    for col in cumulative_cols:
        df[f"{col}_delta"] = df.groupby("pod_name")[col].diff()

    return df

def add_cpu_time_and_delta(df: pd.DataFrame) -> pd.DataFrame:
    """Add utime+stime and its delta"""
    df["cpu_time"] = df["utime"] + df["stime"]
    df["cpu_time_delta"] = df.groupby("pod_name")["cpu_time"].diff()
    return df

def add_cycle_number(df: pd.DataFrame) -> pd.DataFrame:
    """
    타임스탬프 다음 열에 cycle 번호(몇번째 수집인지)를 추가
    pod_name 기준으로 그룹별 순번을 매김
    """
    # pod_name 별로 1부터 시작하는 순번 부여
    df["cycle"] = df.groupby("pod_name").cumcount() + 1

    # cycle 열을 timestamp 바로 뒤로 이동
    cols = list(df.columns)
    if "timestamp" in cols and "cycle" in cols:
        ts_index = cols.index("timestamp")
        # timestamp 다음 위치에 cycle 넣기
        cols.insert(ts_index + 1, cols.pop(cols.index("cycle")))
        df = df[cols]

    return df

def preprocess_file(file_path: str) -> pd.DataFrame:
    """개별 파일에 대해 전처리 실행"""
    df = load_data(file_path)
    df = process_data(df)
    df = add_cycle_number(df)
    df = add_deltas(df)
    df = add_cpu_time_and_delta(df)
    df = align_timestamps(df, base_pod="active-0")
    return df

def merge_experiment_files(input_dir: str, output_path: str) -> pd.DataFrame:
    """
    Merge all experiment_data/process_metrics_experiment*.csv files
    → 파일마다 동일한 전처리 수행 후 병합
    → 첫 번째 열에 experiment_id 추가
    """
    files = glob.glob(os.path.join(input_dir, "process_metrics_experiment*.csv"))
    all_dfs = []

    for file in files:
        # experiment id 추출 (예: process_metrics_experiment10.csv → 10)
        filename = os.path.basename(file)
        experiment_id = "".join([c for c in filename if c.isdigit()])

        df = preprocess_file(file)
        df.insert(0, "experiment_id", int(experiment_id) if experiment_id else None)  # ✅ 첫 열에 추가
        all_dfs.append(df)

    merged_df = pd.concat(all_dfs, ignore_index=True)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    merged_df.to_csv(output_path, index=False)
    print(f"실험 파일 병합 완료: {output_path}")
    return merged_df


if __name__ == "__main__":
    DATA_DIR = "experiment_data/"
    FILE_NAME = "process_metrics_experiment1.csv"
    # add_process_headers(DATA_DIR)

    file_path = os.path.join(DATA_DIR, FILE_NAME)

    # 단일 파일 처리
    df = preprocess_file(file_path)

    # CSV 저장
    output_path = "analyze/statistics_process_metrics_experiment1.csv"
    save_data(df, output_path)

    # 엑셀로 저장
    excel_path = "analyze/statistics_process_metrics_experiment1.xlsx"
    convert_to_excel(df, excel_path)

    # 여러 실험 파일 병합
    merged_output_path = "analyze/statistics_process_metrics_merged.csv"
    merge_experiment_files(DATA_DIR, merged_output_path)
