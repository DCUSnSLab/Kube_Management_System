import re
import glob
from pathlib import Path
import pandas as pd
from typing import Tuple, Dict, List

# ====== 설정 ======
DATA_DIR = Path("./experiment_data")  # CSV들이 있는 디렉터리
GLOB_PATTERN = "gc-??m_*_process_summary_experiment0.csv"  # 예: gc-05m_20251013...csv
EXCEL_OUT = Path("./gc_multi_metrics_summary.xlsx")  # 출력 엑셀 파일

# ---------- 유틸 ----------
FILENAME_RE = re.compile(r"gc-(\d{2})m_(.+)_process_summary_experiment0\.csv$")

def parse_filename_info(p: Path) -> Tuple[int, str]:
    """
    파일명 예: gc-05m_20251013_022108_process_summary_experiment0.csv
    반환: (threshold_minutes:int, run_ts:str)
    """
    m = FILENAME_RE.search(p.name)
    if not m:
        return None, None
    threshold_str, run_ts = m.group(1), m.group(2)
    try:
        threshold_minutes = int(threshold_str)
    except ValueError:
        threshold_minutes = None
    return threshold_minutes, run_ts


def load_latest_per_pod(csv_path: Path) -> pd.DataFrame:
    """
    동일 pod_name이 여러 번 등장하면 timestamp가 가장 최신인 한 건만 유지.
    timestamp 컬럼이 없다면 원본 그대로 반환.
    """
    df = pd.read_csv(csv_path)
    if "timestamp" in df.columns:
        # 정렬 후 같은 pod_name의 마지막(=최신)만 유지
        df_latest = df.sort_values("timestamp").groupby("pod_name", as_index=False).tail(1)
    else:
        df_latest = df.copy()
    return df_latest


def compute_confusion_and_metrics(df_latest: pd.DataFrame) -> Dict[str, float]:
    """
    - pod_name의 앞부분을 pod_type으로 간주 (예: 'idle-0' -> 'idle')
    - 실제(정답): pod_type == 'idle' 이면 삭제 대상
    - 예측: status == 'gc' 이면 우리 GC가 수거한 것으로 간주
    """
    df = df_latest.copy()

    # 방어적 전처리 (누락/대소문자 차이 대응)
    df["pod_name"] = df["pod_name"].astype(str)
    df["status"]   = df["status"].astype(str).str.lower()

    df["pod_type"] = df["pod_name"].str.split("-").str[0]
    df["is_idle"]  = df["pod_type"].eq("idle")
    df["is_gc"]    = df["status"].eq("gc")

    TP = int(((df["is_idle"]) & (df["is_gc"])).sum())         # idle & gc
    FP = int(((~df["is_idle"]) & (df["is_gc"])).sum())        # not idle & gc
    FN = int(((df["is_idle"]) & (~df["is_gc"])).sum())        # idle & not gc
    TN = int(((~df["is_idle"]) & (~df["is_gc"])).sum())       # not idle & not gc

    precision = TP / (TP + FP) if (TP + FP) else 0.0
    recall    = TP / (TP + FN) if (TP + FN) else 0.0
    f1        = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0

    return {
        "TP": TP, "FP": FP, "FN": FN, "TN": TN,
        "Precision": precision, "Recall": recall, "F1-Score": f1
    }


# ---------- 메인 ----------
def main():
    files: List[Path] = sorted(DATA_DIR.glob(GLOB_PATTERN))
    if not files:
        raise FileNotFoundError(f"No files match pattern: {DATA_DIR / GLOB_PATTERN}")

    per_file_rows: List[Dict] = []

    for f in files:
        thr, run_ts = parse_filename_info(f)
        if thr is None:
            # 파일명 패턴이 맞지 않으면 스킵
            continue

        df_latest = load_latest_per_pod(f)
        metrics = compute_confusion_and_metrics(df_latest)

        per_file_rows.append({
            "filename": f.name,
            "threshold_m": thr,   # 5, 10, 15 ... (분)
            "run_ts": run_ts,     # 파일명에서 추출한 실험 타임스탬프 문자열
            **metrics
        })

    if not per_file_rows:
        raise RuntimeError("No valid files with parsed threshold & metrics.")

    # (1) 파일별 지표
    per_file_df = pd.DataFrame(per_file_rows).sort_values(["threshold_m", "run_ts"]).reset_index(drop=True)

    # (2-A) threshold별 '합산 기반(정확한)' 집계  ← 권장 방식
    by_thr_sum = (
        per_file_df
        .groupby("threshold_m", as_index=False)
        [["TP", "FP", "FN", "TN"]]
        .sum(numeric_only=True)
    )

    # 합산 값으로 precision/recall/f1 재계산 (정확한 방식)
    def safe_div(num, den):
        return (num / den) if den else 0.0

    by_thr_sum["Precision"] = [safe_div(tp, tp + fp) for tp, fp in zip(by_thr_sum["TP"], by_thr_sum["FP"])]
    by_thr_sum["Recall"]    = [safe_div(tp, tp + fn) for tp, fn in zip(by_thr_sum["TP"], by_thr_sum["FN"])]
    by_thr_sum["F1-Score"]  = [
        (2 * p * r / (p + r) if (p + r) else 0.0)
        for p, r in zip(by_thr_sum["Precision"], by_thr_sum["Recall"])
    ]

    # (2-B) threshold별 '단순 평균' 집계  ← 참고용(왜곡 가능성 있음)
    by_thr_mean = (
        per_file_df
        .groupby("threshold_m", as_index=False)
        [["TP", "FP", "FN", "TN", "Precision", "Recall", "F1-Score"]]
        .mean(numeric_only=True)
        .rename(columns={
            "TP": "TP_mean", "FP": "FP_mean", "FN": "FN_mean", "TN": "TN_mean",
            "Precision": "Precision_mean", "Recall": "Recall_mean", "F1-Score": "F1_mean"
        })
    )

    # 엑셀 저장
    with pd.ExcelWriter(EXCEL_OUT, engine="openpyxl") as writer:
        per_file_df.to_excel(writer, index=False, sheet_name="per_file_metrics")
        by_thr_sum.to_excel(writer, index=False, sheet_name="by_threshold_sum_based")   # ✅ 권장
        by_thr_mean.to_excel(writer, index=False, sheet_name="by_threshold_mean_based") # 참고

    # 콘솔 출력 (옵션)
    print("\n=== Per-file metrics ===")
    print(per_file_df.to_string(index=False))
    print("\n=== By-threshold (SUM-based, recommended) ===")
    print(by_thr_sum.to_string(index=False))
    print("\n=== By-threshold (MEAN-based, for reference) ===")
    print(by_thr_mean.to_string(index=False))
    print(f"\nSaved Excel -> {EXCEL_OUT.resolve()}")


if __name__ == "__main__":
    main()