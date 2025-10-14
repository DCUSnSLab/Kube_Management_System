# -*- coding: utf-8 -*-
"""
Batch Memory-usage summarizer (MiB 변환 지원)
- 입력: BASE_DIR 내 "Memory*.csv" (Excel 'sep=,' 프롤로그 지원)
- 그룹: 파일명 중 "05m", "10m", "15m", "20m"
- 포맷 A: 첫 컬럼이 실험번호(1..6) → 그 번호 기준으로 pivot
- 포맷 B: Time 컬럼 → 6개 실험(약 1시간)으로 분할 후 pivot
- 산출:
  1) per-file wide CSV (step, exp1..exp6, avg) → output_mem
  2) per-file stats(mean/std/total/count) → output_mem
  3) 통합 엑셀: PerFileStats + GroupAverages(그룹별 시간대 평균) → output_mem
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
INPUT_GLOB = os.path.join(BASE_DIR, "Memory*.csv")
OUTPUT_DIR = os.path.join(BASE_DIR, "output_mem")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===============================
# Helpers
# ===============================
_UNIT_RE = re.compile(r"^\s*([0-9]*\.?[0-9]+)\s*([KMG]i)?B?\s*$", re.IGNORECASE)

def parse_bytes_like_to_mib(x):
    """
    문자열(예: '123 MiB', '0.5GiB', '800 KiB') → float(MiB) 로 변환.
    숫자/NaN은 그대로 처리. 예상치 못한 형식은 NaN.
    """
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return np.nan
    # 이미 숫자면 MiB로 간주
    try:
        return float(x)
    except (TypeError, ValueError):
        pass
    s = str(x)
    m = _UNIT_RE.match(s)
    if not m:
        return np.nan
    val = float(m.group(1))
    unit = (m.group(2) or "").lower()
    if unit == "kI".lower() or unit == "ki":
        return val / 1024.0
    elif unit == "mi":
        return val
    elif unit == "gi":
        return val * 1024.0
    # 단위가 아예 없는 경우: MiB로 간주
    return val

def parse_group_from_filename(fname: str) -> str:
    m = re.search(r'(\d{2}m)', os.path.basename(fname))
    return m.group(1) if m else "unknown"

def read_excelish_csv(path: str) -> pd.DataFrame:
    """
    Excel 'sep=,' 프롤로그를 건너뛰어 읽기. 필요하면 header=None 재시도.
    """
    try:
        df = pd.read_csv(path, skiprows=1)
        if df.shape[1] < 3 and "Time" not in df.columns:
            df = pd.read_csv(path, skiprows=1, header=None)
    except UnicodeDecodeError:
        df = pd.read_csv(path, skiprows=1, encoding="utf-8-sig")
    return df

def to_numeric_mib_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    DataFrame의 모든 컬럼을 'MiB float'로 변환 시도 (숫자/문자 혼재를 모두 커버).
    """
    out = df.copy()
    for c in out.columns:
        out[c] = out[c].map(parse_bytes_like_to_mib)
    return out

# ===============================
# Wide-builders (Memory 전용)
# ===============================
def wide_from_experiment_index(df_raw: pd.DataFrame):
    """
    Case A: 첫 컬럼이 실험번호(1..6), 나머지는 pod memory(문자열 'MiB' 등).
    Output: wide(step, exp1..exp6, avg_MiB), counts_per_exp
    """
    df = df_raw.copy()
    df = df.rename(columns={df.columns[0]: "experiment_id"})
    pod_cols = [c for c in df.columns if c != "experiment_id"]
    # 메모리(MiB)로 변환
    df[pod_cols] = to_numeric_mib_df(df[pod_cols])

    # 실험번호 정리
    df = df[~df["experiment_id"].isna()].copy()
    df["experiment_id"] = df["experiment_id"].astype(int)

    # 모든 파드 메모리 합 (MiB)
    df["total_mem_mib"] = df[pod_cols].sum(axis=1, skipna=True)
    # 실험 내부 순번(step)
    df["step"] = df.groupby("experiment_id").cumcount() + 1

    # 피벗
    wide = df.pivot(index="step", columns="experiment_id", values="total_mem_mib")
    wide.columns = [f"exp{int(c)}" for c in wide.columns]
    for i in range(1, 7):
        if f"exp{i}" not in wide.columns:
            wide[f"exp{i}"] = np.nan
    wide = wide[[f"exp{i}" for i in range(1, 7)]]
    wide["avg"] = wide.mean(axis=1)  # MiB 평균

    return wide.reset_index(), df.groupby("experiment_id").size().to_dict()

def wide_from_time_based(df_raw: pd.DataFrame):
    """
    Case B: Time 컬럼이 있고 pods는 그 외 열(문자열 'MiB' 등).
    1) 큰 공백(>30m) 1차 분할
    2) split/merge로 정확히 6개 구간 보정(대략 60행씩)
    Output: wide(step, exp1..exp6, avg_MiB), counts_per_exp
    """
    if "Time" not in df_raw.columns:
        return None, {}

    raw = df_raw.copy()
    raw["Time"] = pd.to_datetime(raw["Time"], errors="coerce")
    pods = raw.drop(columns=["Time"])
    pods_mib = to_numeric_mib_df(pods)

    df = pd.DataFrame({
        "Time": raw["Time"],
        "total_mem_mib": pods_mib.sum(axis=1, skipna=True)
    })
    df = df[~df["Time"].isna()].sort_values("Time").reset_index(drop=True)

    # 1차 분할: 30분 초과 공백
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

    # 6개로 보정
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

    # 라벨링
    exp_id_series = pd.Series(index=df.index, dtype="Int64")
    for i, (start, end) in enumerate(segments, start=1):
        exp_id_series.loc[start:end] = i

    df2 = df.copy()
    df2["experiment_id"] = exp_id_series.astype("Int64")
    df2 = df2.dropna(subset=["experiment_id"]).copy()
    df2["experiment_id"] = df2["experiment_id"].astype(int)

    df2["step"] = df2.groupby("experiment_id").cumcount() + 1
    wide = df2.pivot(index="step", columns="experiment_id", values="total_mem_mib")
    wide.columns = [f"exp{int(i)}" for i in wide.columns]
    for i in range(1, 7):
        if f"exp{i}" not in wide.columns:
            wide[f"exp{i}"] = np.nan
    wide = wide[[f"exp{i}" for i in range(1, 7)]]
    wide["avg"] = wide.mean(axis=1)

    return wide.reset_index(), df2.groupby("experiment_id").size().to_dict()

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

    # 통계 (파일 내 exp1..exp6 전체에 대해 mean/std/total/count) — 모두 MiB 기준
    stats = None
    if wide is not None:
        value_cols = [c for c in wide.columns if c.startswith("exp")]
        long_df = wide.melt(id_vars=["step"], value_vars=value_cols,
                            var_name="experiment", value_name="total_mem_mib")
        stats = (
            long_df.dropna(subset=["total_mem_mib"])
                   .groupby("experiment")["total_mem_mib"]
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
# Batch: 모든 Memory*.csv 처리
# ===============================
def run_batch():
    files = glob.glob(INPUT_GLOB)
    results, all_stats = [], []
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

    if all_stats:
        stats_concat = pd.concat(all_stats, ignore_index=True)
        stats_concat.to_csv(os.path.join(OUTPUT_DIR, "ALL__mem_stats_summary.csv"), index=False)
    else:
        stats_concat = pd.DataFrame()

    summary_df.to_csv(os.path.join(OUTPUT_DIR, "ALL__mem_batch_summary.csv"), index=False)
    return summary_df, stats_concat

# ===============================
# Build Excel: PerFileStats + GroupAverages
# ===============================
def build_group_averages_excel(excel_path: str = None):
    """
    종합 엑셀 생성:
      - PerFileStats: ALL__mem_stats_summary.csv (있을 경우)
      - GroupAverages: step × (05m,10m,15m,20m) 평균(MiB)
        * 동일 그룹 내: 각 파일 wide의 exp1..exp6 평균 → 파일 간 평균
    """
    if excel_path is None:
        excel_path = os.path.join(OUTPUT_DIR, "ALL_mem_experiments_summary.xlsx")

    wide_files = glob.glob(os.path.join(OUTPUT_DIR, "*__*__wide.csv"))

    def get_group_from_name(path: str) -> str:
        m = re.search(r"__([0-9]{2}m)__wide\.csv$", path)
        return m.group(1) if m else "unknown"

    # 그룹별 wide 모으기
    group_dfs = {}
    for wf in wide_files:
        group = get_group_from_name(wf)
        w = pd.read_csv(wf)
        value_cols = [c for c in w.columns if c.startswith("exp")]
        w2 = w[["step"] + value_cols].copy()
        group_dfs.setdefault(group, []).append(w2)

    # 그룹별 시간대 평균 계산(MiB)
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

    all_steps = sorted(set().union(*[s.index.tolist() for s in group_series.values()])) if group_series else []
    summary_df = pd.DataFrame({"step": all_steps})
    for group, s in sorted(group_series.items()):
        summary_df[group] = summary_df["step"].map(s.to_dict())

    # 엑셀 쓰기
    with pd.ExcelWriter(excel_path, engine="xlsxwriter") as writer:
        stats_csv = os.path.join(OUTPUT_DIR, "ALL__mem_stats_summary.csv")
        if os.path.exists(stats_csv):
            pd.read_csv(stats_csv).to_excel(writer, sheet_name="PerFileStats", index=False)
        summary_df.to_excel(writer, sheet_name="GroupAverages", index=False)

    return summary_df, excel_path

# ===============================
# Main
# ===============================
if __name__ == "__main__":
    summary_df, stats_concat = run_batch()
    group_avg_df, xlsx_path = build_group_averages_excel()
    print("Batch summary   :", os.path.join(OUTPUT_DIR, "ALL__mem_batch_summary.csv"))
    print("Stats summary   :", os.path.join(OUTPUT_DIR, "ALL__mem_stats_summary.csv"))
    print("Excel (averages):", xlsx_path)
