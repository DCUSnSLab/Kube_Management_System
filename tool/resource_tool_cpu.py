# -*- coding: utf-8 -*-
"""
Batch CPU-usage summarizer
- 대상 파일: BASE_DIR 내 "CPU*.csv" (Excel에서 export한 'sep=,' 프롤로그 지원)
- 실험 분류: 파일명 중 "05m", "10m", "15m", "20m" 토큰으로 그룹 식별
- 두 가지 파일 포맷 자동 처리:
  A) 0번 컬럼이 실험번호(1..6) → 그 번호 기준으로 step×(exp1..exp6) pivot
  B) Time 컬럼 존재 → 6개 실험으로 분할(갭/60분 기준) 후 step×(exp1..exp6) pivot
- 산출:
  1) 개별 파일 wide CSV (step, exp1..exp6, avg)
  2) 개별 파일 통계(stats: mean/std/total/count)
  3) 통합 엑셀: PerFileStats + GroupAverages(그룹별 시간대 평균)
"""

import os
import re
import glob
import numpy as np
import pandas as pd
from datetime import timedelta

# ===============================
# Config
# ===============================
BASE_DIR = "./experiment_data/usages"         # ★ 실제 데이터 폴더
INPUT_GLOB = os.path.join(BASE_DIR, "CPU*.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs_cpu")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===============================
# Helpers
# ===============================
def parse_group_from_filename(fname: str) -> str:
    """
    Extract experiment group token like '05m', '10m', '15m', '20m' from filename.
    Returns the token (e.g., '05m') or 'unknown'.
    """
    m = re.search(r'(\d{2}m)', os.path.basename(fname))
    return m.group(1) if m else "no-gc"

def read_excelish_csv(path: str) -> pd.DataFrame:
    """
    Read CSV that may start with a 'sep=,' header line (Excel export artifact).
    Returns a DataFrame with raw contents (header inferred).
    """
    try:
        df = pd.read_csv(path, skiprows=1)  # skip 'sep=,' line
        # 만약 열이 너무 적고 Time도 없으면 header=None 시도
        if df.shape[1] < 3 and "Time" not in df.columns:
            df = pd.read_csv(path, skiprows=1, header=None)
    except UnicodeDecodeError:
        df = pd.read_csv(path, skiprows=1, encoding="utf-8-sig")
    return df

def to_numeric_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    return out

# ===============================
# Wide-builders (두 포맷 지원)
# ===============================
def wide_from_experiment_index(df_raw: pd.DataFrame):
    """
    Case A: 첫 컬럼이 실험번호(1..6), 나머지는 pod CPU.
    Output: wide(step, exp1..exp6, avg), counts_per_experiment
    """
    df = df_raw.copy()
    df = df.rename(columns={df.columns[0]: "experiment_id"})
    pod_cols = [c for c in df.columns if c != "experiment_id"]
    df[pod_cols] = to_numeric_df(df[pod_cols])

    # 실험번호 결측 제거 + 정수화
    df = df[~df["experiment_id"].isna()].copy()
    df["experiment_id"] = df["experiment_id"].astype(int)

    # 모든 파드 합
    df["total_cpu_usage"] = df[pod_cols].sum(axis=1, skipna=True)
    # 실험 내부 순번(step)
    df["step"] = df.groupby("experiment_id").cumcount() + 1

    # 피벗
    wide = df.pivot(index="step", columns="experiment_id", values="total_cpu_usage")
    wide.columns = [f"exp{int(c)}" for c in wide.columns]

    # exp1..exp6 보장
    for i in range(1, 7):
        col = f"exp{i}"
        if col not in wide.columns:
            wide[col] = np.nan
    wide = wide[[f"exp{i}" for i in range(1, 7)]]

    # 평균
    wide["avg"] = wide.mean(axis=1)

    wide = wide.reset_index()
    counts = df.groupby("experiment_id").size().to_dict()
    return wide, counts

def wide_from_time_based(df_raw: pd.DataFrame):
    """
    Case B: Time 컬럼이 있고 pods는 그 외 열.
    1) 큰 시간 공백(>30m)으로 1차 분할
    2) 분할 구간을 split/merge하여 정확히 6개로 보정(대략 60행씩)
    Output: wide(step, exp1..exp6, avg), counts_per_experiment
    """
    if "Time" not in df_raw.columns:
        return None, {}

    raw = df_raw.copy()
    raw["Time"] = pd.to_datetime(raw["Time"], errors="coerce")
    pods = raw.drop(columns=["Time"]).apply(pd.to_numeric, errors="coerce")

    df = pd.DataFrame({"Time": raw["Time"], "total_cpu_usage": pods.sum(axis=1, skipna=True)})
    df = df[~df["Time"].isna()].sort_values("Time").reset_index(drop=True)

    # ---- 1차 분할: 큰 공백(>30분) ----
    gap = df["Time"].diff()
    new_seg = (gap > pd.Timedelta(minutes=30)).fillna(True)  # 첫 행 True
    seg_id = new_seg.cumsum()
    segments = [(g.index.min(), g.index.max()) for _, g in df.groupby(seg_id)]

    def total_segments_len(segs):
        return [end - start + 1 for start, end in segs]

    def split_segment(segs, idx, split_size=60):
        start, end = segs[idx]
        if end <= start:
            return segs
        mid = min(start + split_size - 1, end - 1)
        return segs[:idx] + [(start, mid), (mid+1, end)] + segs[idx+1:]

    def merge_adjacent_smallest(segs):
        # 인접한 두 구간의 합 길이가 가장 작은 쌍을 병합
        if len(segs) <= 1:
            return segs
        lengths = total_segments_len(segs)
        best_i, best_sum = None, None
        for i in range(len(segs)-1):
            s = lengths[i] + lengths[i+1]
            if best_sum is None or s < best_sum:
                best_sum, best_i = s, i
        a, b = segs[best_i], segs[best_i+1]
        return segs[:best_i] + [(a[0], b[1])] + segs[best_i+2:]

    # ---- 6개로 보정 ----
    MAX_EXPS = 6
    # 긴 구간을 60행 기준으로 먼저 잘라 6개 이상 확보 시도
    while len(segments) < MAX_EXPS:
        lengths = total_segments_len(segments)
        if not lengths:
            break
        longest_i = int(np.argmax(lengths))
        if lengths[longest_i] < 70:
            break
        segments = split_segment(segments, longest_i, split_size=60)

    # 그래도 모자라면 가장 긴 구간을 반씩 쪼개서 6개까지
    while len(segments) < MAX_EXPS and len(segments) > 0:
        lengths = total_segments_len(segments)
        longest_i = int(np.argmax(lengths))
        if lengths[longest_i] <= 1:
            break
        half = max(1, lengths[longest_i] // 2)
        segments = split_segment(segments, longest_i, split_size=half)

    # 6개 초과면 가장 짧은 인접쌍부터 병합
    while len(segments) > MAX_EXPS:
        segments = merge_adjacent_smallest(segments)

    # ---- 라벨링 ----
    exp_id_series = pd.Series(index=df.index, dtype="Int64")
    for i, (start, end) in enumerate(segments, start=1):
        exp_id_series.loc[start:end] = i

    df2 = df.copy()
    df2["experiment_id"] = exp_id_series.astype("Int64")
    df2 = df2.dropna(subset=["experiment_id"]).copy()
    df2["experiment_id"] = df2["experiment_id"].astype(int)

    # step & wide
    df2["step"] = df2.groupby("experiment_id").cumcount() + 1
    wide = df2.pivot(index="step", columns="experiment_id", values="total_cpu_usage")
    wide.columns = [f"exp{int(i)}" for i in wide.columns]

    for i in range(1, 7):
        col = f"exp{i}"
        if col not in wide.columns:
            wide[col] = np.nan
    wide = wide[[f"exp{i}" for i in range(1, 7)]]
    wide["avg"] = wide.mean(axis=1)
    wide = wide.reset_index()

    counts = df2.groupby("experiment_id").size().to_dict()
    return wide, counts

# ===============================
# File processing (한 파일 처리)
# ===============================
def process_file(path: str) -> dict:
    df_try1 = read_excelish_csv(path)

    # 포맷 판별: 첫 컬럼이 대부분 1..6이면 experiment-index 모드
    first_col = df_try1.columns[0]
    first_values = pd.to_numeric(df_try1[first_col], errors="coerce")
    nonnull = first_values.dropna()
    if len(nonnull) > 0 and (nonnull.isin([1,2,3,4,5,6]).mean() >= 0.8):
        mode = "experiment_index"
        df2 = pd.read_csv(path, skiprows=1, header=None)  # 헤더 없이 다시 읽기
        wide, counts = wide_from_experiment_index(df2)
    else:
        mode = "time_based"
        df2 = pd.read_csv(path, skiprows=1)               # 헤더로 다시 읽기
        wide, counts = wide_from_time_based(df2)

    group = parse_group_from_filename(path)
    base = os.path.splitext(os.path.basename(path))[0]

    out_wide = os.path.join(OUTPUT_DIR, f"{base}__{group}__wide.csv")
    if wide is not None:
        wide.to_csv(out_wide, index=False)

    # 통계 (파일 내 exp1..exp6 전체에 대해 mean/std/total/count)
    stats = None
    if wide is not None:
        value_cols = [c for c in wide.columns if c.startswith("exp")]
        long_df = wide.melt(id_vars=["step"], value_vars=value_cols,
                            var_name="experiment", value_name="total_cpu_usage")
        stats = (
            long_df.dropna(subset=["total_cpu_usage"])
                   .groupby("experiment")["total_cpu_usage"]
                   .agg(mean="mean", std="std", total="sum", count="size")
                   .reset_index()
        )
        stats.insert(0, "file", base)
        stats.insert(1, "group", group)
        stats_path = os.path.join(OUTPUT_DIR, f"{base}__{group}__stats.csv")
        stats.to_csv(stats_path, index=False)

    return {
        "file": base,
        "group": group,
        "mode": mode,
        "counts": counts,
        "wide_path": out_wide if wide is not None else None,
        "stats_df": stats
    }

# ===============================
# Batch: 모든 CPU*.csv 처리
# ===============================
def run_batch() -> tuple[pd.DataFrame, pd.DataFrame]:
    files = glob.glob(INPUT_GLOB)
    results = []
    all_stats = []
    for f in files:
        try:
            r = process_file(f)
            results.append(r)
            if r.get("stats_df") is not None:
                all_stats.append(r["stats_df"])
        except Exception as e:
            results.append({"file": os.path.basename(f), "error": str(e)})

    summary_df = pd.DataFrame([
        {
            "file": r.get("file"),
            "group": r.get("group"),
            "mode": r.get("mode"),
            "counts": r.get("counts"),
            "wide_path": r.get("wide_path"),
            "error": r.get("error"),
        } for r in results
    ])

    stats_concat = pd.concat(all_stats, ignore_index=True) if all_stats else pd.DataFrame()
    if not stats_concat.empty:
        stats_concat.to_csv(os.path.join(OUTPUT_DIR, "ALL__stats_summary.csv"), index=False)

    return summary_df, stats_concat

# ===============================
# Build Excel: PerFileStats + GroupAverages
# ===============================
def build_group_averages_excel(excel_path: str = None):
    """
    종합 엑셀을 생성:
      - PerFileStats: ALL__stats_summary.csv (있을 경우)
      - GroupAverages: step × (05m,10m,15m,20m) 평균 (그룹별)
        * 동일 그룹 내: 각 파일 wide의 exp1..exp6 평균 → 파일들 간 평균
    """
    if excel_path is None:
        excel_path = os.path.join(OUTPUT_DIR, "CPU_ALL_experiments_summary.xlsx")

    # 모든 per-file wide CSV 수집
    wide_files = glob.glob(os.path.join(OUTPUT_DIR, "*__*__wide.csv"))

    def get_group_from_name(path: str) -> str:
        m = re.search(r"__([0-9]{2}m)__wide\.csv$", path)
        return m.group(1) if m else "unknown"

    # 그룹별 wide 모으기
    group_dfs: dict[str, list[pd.DataFrame]] = {}
    for wf in wide_files:
        group = get_group_from_name(wf)
        w = pd.read_csv(wf)
        # step + exp*만 유지
        value_cols = [c for c in w.columns if c.startswith("exp")]
        cols = ["step"] + value_cols
        w2 = w[cols].copy()
        group_dfs.setdefault(group, []).append(w2)

    # 그룹별 시간대 평균 계산
    # - 파일 내 exp1..exp6의 행 평균 → 파일 간 행 평균
    group_series = {}
    for group, df_list in group_dfs.items():
        per_file_series = []
        for w in df_list:
            exp_cols = [c for c in w.columns if c.startswith("exp")]
            s = w.set_index("step")[exp_cols].mean(axis=1)  # 파일 내 평균
            per_file_series.append(s)
        if per_file_series:
            combined = pd.concat(per_file_series, axis=1)      # step 기준 outer join
            group_avg = combined.mean(axis=1, skipna=True)     # 파일 간 평균
            group_series[group] = group_avg

    # step 축 만들기
    all_steps = sorted(set().union(*[s.index.tolist() for s in group_series.values()])) if group_series else []
    summary_df = pd.DataFrame({"step": all_steps})
    for group, s in sorted(group_series.items()):
        summary_df[group] = summary_df["step"].map(s.to_dict())

    # 엑셀 쓰기
    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        # 1) PerFileStats (있을 때만)
        stats_csv = os.path.join(OUTPUT_DIR, "ALL__stats_summary.csv")
        if os.path.exists(stats_csv):
            stats_df = pd.read_csv(stats_csv)
            stats_df.to_excel(writer, sheet_name="PerFileStats", index=False)

        # 2) GroupAverages
        summary_df.to_excel(writer, sheet_name="GroupAverages", index=False)

    return summary_df, excel_path

# ===============================
# Main
# ===============================
if __name__ == "__main__":
    summary_df, stats_concat = run_batch()
    summary_df.to_csv(os.path.join(OUTPUT_DIR, "ALL__batch_summary.csv"), index=False)
    group_avg_df, xlsx_path = build_group_averages_excel()
    print("Batch summary  :", os.path.join(OUTPUT_DIR, "ALL__batch_summary.csv"))
    print("Stats summary  :", os.path.join(OUTPUT_DIR, "ALL__stats_summary.csv"))
    print("Excel (averages):", xlsx_path)
