# -*- coding: utf-8 -*-
"""
CPU pod-count summarizer (combined)
- 입력: BASE_DIR 내 "CPU*.csv" (Excel 'sep=,' 프롤로그 지원)
- 그룹: 파일명 중 "05m", "10m", "15m", "20m" 토큰
- 포맷 A: 첫 컬럼이 실험번호(1..6) → 그 번호 기준으로 pivot
- 포맷 B: Time 컬럼 → 6개 실험(≈1시간)으로 분할 후 pivot
- 산출:
    1) per-file podcount CSV (step, exp1..exp6) → outputs_podcount
       (값은 해당 step에서 '값이 존재하는' 파드 개수 = Non-NaN count)
    2) 그룹별 시간대 평균 파드 수 / 평균 변화량(Δ) → Excel 2개 시트로 저장
       - GroupAverageCount
       - GroupAverageDelta
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
BASE_DIR = "./experiment_data/usages"          # ★ 실제 데이터 폴더
INPUT_GLOB = os.path.join(BASE_DIR, "CPU*.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "outputs_podcount")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===============================
# Helpers
# ===============================
def parse_group_from_filename(fname: str) -> str:
    """
    Extract group token like '05m', '10m', '15m', '20m' from filename.
    """
    m = re.search(r'(\d{2}m)', os.path.basename(fname))
    return m.group(1) if m else "unknown"

def read_excelish_csv(path: str) -> pd.DataFrame:
    """
    Excel 'sep=,' 프롤로그를 건너뛰어 읽기. 필요하면 header=None로 재시도.
    """
    try:
        df = pd.read_csv(path, skiprows=1)
        if df.shape[1] < 3 and "Time" not in df.columns:
            df = pd.read_csv(path, skiprows=1, header=None)
    except UnicodeDecodeError:
        df = pd.read_csv(path, skiprows=1, encoding="utf-8-sig")
    return df

def to_numeric_or_nan_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    파드 값 열을 모두 수치로 변환(변환 불가 → NaN).
    '값이 있으면 파드가 있음' 규칙을 적용하기 위한 전처리.
    """
    out = df.copy()
    for c in out.columns:
        out[c] = pd.to_numeric(out[c], errors="coerce")
    return out

# ===============================
# Wide builders (pod-count)
# ===============================
def wide_count_from_experiment_index(df_raw: pd.DataFrame):
    """
    Case A: 첫 컬럼이 실험번호(1..6), 나머지는 pod 값(수치/문자 혼재 가능).
    Output: wide(step, exp1..exp6), counts_per_experiment
    값 = 해당 step에서 NaN이 아닌 파드 값의 개수
    """
    df = df_raw.copy()
    df = df.rename(columns={df.columns[0]: "experiment_id"})
    pod_cols = [c for c in df.columns if c != "experiment_id"]

    df[pod_cols] = to_numeric_or_nan_df(df[pod_cols])

    df = df[~df["experiment_id"].isna()].copy()
    df["experiment_id"] = df["experiment_id"].astype(int)

    df["pod_count"] = df[pod_cols].notna().sum(axis=1)  # Non-NaN 개수
    df["step"] = df.groupby("experiment_id").cumcount() + 1

    wide = df.pivot(index="step", columns="experiment_id", values="pod_count")
    wide.columns = [f"exp{int(c)}" for c in wide.columns]
    for i in range(1, 7):
        col = f"exp{i}"
        if col not in wide.columns:
            wide[col] = np.nan

    wide = wide[[f"exp{i}" for i in range(1, 7)]].reset_index()
    counts = df.groupby("experiment_id").size().to_dict()
    return wide, counts

def wide_count_from_time_based(df_raw: pd.DataFrame):
    """
    Case B: Time 컬럼이 있고 pods는 그 외 열.
    1) 큰 시간 공백(>30m)으로 1차 분할
    2) split/merge로 정확히 6개 구간 보정(대략 60행씩)
    Output: wide(step, exp1..exp6), counts_per_experiment
    값 = 해당 step에서 NaN이 아닌 파드 값의 개수
    """
    if "Time" not in df_raw.columns:
        return None, {}

    raw = df_raw.copy()
    raw["Time"] = pd.to_datetime(raw["Time"], errors="coerce")
    pods = raw.drop(columns=["Time"])
    pods_num = to_numeric_or_nan_df(pods)

    df = pd.DataFrame({
        "Time": raw["Time"],
        "pod_count": pods_num.notna().sum(axis=1)  # Non-NaN 개수
    })
    df = df[~df["Time"].isna()].sort_values("Time").reset_index(drop=True)

    # ---- 1차 분할: 30분 초과 공백 ----
    gap = df["Time"].diff()
    new_seg = (gap > pd.Timedelta(minutes=30)).fillna(True)
    seg_id = new_seg.cumsum()
    segments = [(g.index.min(), g.index.max()) for _, g in df.groupby(seg_id)]

    def seg_lens(segs): return [end - start + 1 for start, end in segs]
    def split_segment(segs, idx, split_size=60):
        start, end = segs[idx]
        if end <= start: return segs
        mid = min(start + split_size - 1, end - 1)
        return segs[:idx] + [(start, mid), (mid+1, end)] + segs[idx+1:]
    def merge_adjacent_smallest(segs):
        if len(segs) <= 1: return segs
        lens = seg_lens(segs)
        best_i, best_sum = None, None
        for i in range(len(segs)-1):
            s = lens[i] + lens[i+1]
            if best_sum is None or s < best_sum:
                best_sum, best_i = s, i
        a, b = segs[best_i], segs[best_i+1]
        return segs[:best_i] + [(a[0], b[1])] + segs[best_i+2:]

    # ---- 6개로 보정 ----
    MAX_EXPS = 6
    while len(segments) < MAX_EXPS:
        lens = seg_lens(segments)
        if not lens: break
        li = int(np.argmax(lens))
        if lens[li] < 70: break
        segments = split_segment(segments, li, split_size=60)
    while len(segments) < MAX_EXPS and len(segments) > 0:
        lens = seg_lens(segments)
        li = int(np.argmax(lens))
        if lens[li] <= 1: break
        half = max(1, lens[li] // 2)
        segments = split_segment(segments, li, split_size=half)
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

    df2["step"] = df2.groupby("experiment_id").cumcount() + 1
    wide = df2.pivot(index="step", columns="experiment_id", values="pod_count")
    wide.columns = [f"exp{int(i)}" for i in wide.columns]
    for i in range(1, 7):
        col = f"exp{i}"
        if col not in wide.columns:
            wide[col] = np.nan

    wide = wide[[f"exp{i}" for i in range(1, 7)]].reset_index()
    counts = df2.groupby("experiment_id").size().to_dict()
    return wide, counts

# ===============================
# Per-file 처리
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
        wide, counts = wide_count_from_experiment_index(df2)
    else:
        mode = "time_based"
        df2 = pd.read_csv(path, skiprows=1)               # 헤더로 다시 읽기
        wide, counts = wide_count_from_time_based(df2)

    group = parse_group_from_filename(path)
    base = os.path.splitext(os.path.basename(path))[0]

    out_wide = os.path.join(OUTPUT_DIR, f"{base}__{group}__podcount.csv")
    if wide is not None:
        wide.to_csv(out_wide, index=False)

    return {
        "file": base,
        "group": group,
        "mode": mode,
        "counts": counts,
        "out_path": out_wide if wide is not None else None
    }

# ===============================
# Batch 실행 (파일별 산출)
# ===============================
def run_batch() -> pd.DataFrame:
    files = glob.glob(INPUT_GLOB)
    results = []
    for f in files:
        try:
            r = process_file(f)
            results.append(r)
        except Exception as e:
            results.append({"file": os.path.basename(f), "error": str(e)})
    summary_df = pd.DataFrame(results)
    summary_df.to_csv(os.path.join(OUTPUT_DIR, "ALL__podcount_batch_summary.csv"), index=False)
    return summary_df

# ===============================
# 그룹 평균(Count/Delta) 엑셀 생성
# ===============================
def build_group_averages_excel(excel_path: str = None):
    """
    종합 엑셀 생성:
      - GroupAverageCount: step × (05m,10m,15m,20m) 평균 파드 수
      - GroupAverageDelta: step × (05m,10m,15m,20m) 평균 변화량(Δ: step간 차분)
    계산 방식(파일/실험 반복 평균):
      (1) 각 파일의 exp1..exp6 평균 → 파일별 step 평균
      (2) 같은 그룹의 파일들을 step 기준으로 평균
    """
    if excel_path is None:
        excel_path = os.path.join(OUTPUT_DIR, "ALL_podcount_group_summary.xlsx")

    wide_files = glob.glob(os.path.join(OUTPUT_DIR, "*__*__podcount.csv"))

    def get_group_from_name(path: str) -> str:
        m = re.search(r"__([0-9]{2}m)__podcount\.csv$", path)
        return m.group(1) if m else "unknown"

    # 그룹별 파일 모으기
    group_files = {}
    for wf in wide_files:
        group = get_group_from_name(wf)
        group_files.setdefault(group, []).append(wf)

    # 그룹별 step 평균 (count)
    group_count_series = {}
    group_delta_series = {}

    for group, paths in group_files.items():
        per_file_count = []
        per_file_delta = []
        for p in paths:
            w = pd.read_csv(p)  # columns: step, exp1..exp6
            exp_cols = [c for c in w.columns if c.startswith("exp")]
            w = w.sort_values("step")

            # 파일 내부: step별 실험 평균(=exp1..exp6 평균)
            s_count = w.set_index("step")[exp_cols].mean(axis=1)

            # 변화량(Δ): step 간 차분
            s_delta = s_count.diff()  # 첫 step은 NaN

            per_file_count.append(s_count.rename(os.path.basename(p)))
            per_file_delta.append(s_delta.rename(os.path.basename(p)))

        # 파일 간 평균 (step 기준 outer join)
        if per_file_count:
            combined_count = pd.concat(per_file_count, axis=1)
            group_count_series[group] = combined_count.mean(axis=1, skipna=True).round().fillna(0).astype(int)

        if per_file_delta:
            combined_delta = pd.concat(per_file_delta, axis=1)
            group_delta_series[group] = combined_delta.mean(axis=1, skipna=True).round().fillna(0).astype(int)

    # step 축 구성
    all_steps_count = sorted(set().union(*[s.index.tolist() for s in group_count_series.values()])) if group_count_series else []
    all_steps_delta = sorted(set().union(*[s.index.tolist() for s in group_delta_series.values()])) if group_delta_series else []

    df_count = pd.DataFrame({"step": all_steps_count})
    for group, s in sorted(group_count_series.items()):
        df_count[group] = df_count["step"].map(s.to_dict())

    df_delta = pd.DataFrame({"step": all_steps_delta})
    for group, s in sorted(group_delta_series.items()):
        df_delta[group] = df_delta["step"].map(s.to_dict())

    # 엑셀 저장
    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        df_count.to_excel(writer, sheet_name="GroupAverageCount", index=False)
        df_delta.to_excel(writer, sheet_name="GroupAverageDelta", index=False)

    return df_count, df_delta, excel_path

# ===============================
# Main
# ===============================
if __name__ == "__main__":
    summary = run_batch()
    df_count, df_delta, xlsx_path = build_group_averages_excel()
    print("Saved pod-count files to:", OUTPUT_DIR)
    print("Excel:", xlsx_path)
