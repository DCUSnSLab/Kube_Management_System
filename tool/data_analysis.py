from pathlib import Path
import re
from typing import Dict, List, Tuple
import pandas as pd

FILENAME_REGEX = re.compile(r"^process_metrics_experiment(\d+)\.csv$", re.IGNORECASE)

def find_experiment_files(dir_path: str | Path) -> List[Path]:
    """
    주어진 디렉터리에서 'process_metrics_experiment*.csv' 파일을 모두 찾고,
    파일명 뒤의 실험 번호(정수) 기준으로 오름차순 정렬하여 Path 리스트 반환.
    """
    base = Path(dir_path)
    if not base.is_dir():
        raise NotADirectoryError(f"Not a directory: {base}")

    candidates = list(base.glob("process_metrics_experiment*.csv"))

    # (exp_no, Path) 튜플로 정리 후 정렬
    parsed: List[Tuple[int, Path]] = []
    for p in candidates:
        m = FILENAME_REGEX.match(p.name)
        if m:
            exp_no = int(m.group(1))
            parsed.append((exp_no, p))
        else:
            # 패턴에 안 맞는 경우는 스킵 (원하면 로그 출력 가능)
            pass

    # 실험 번호 기준 정렬
    parsed.sort(key=lambda x: x[0])

    # Path만 추출
    return [p for _, p in parsed]

def load_experiment_csvs(dir_path: str | Path, **read_csv_kwargs) -> Dict[int, pd.DataFrame]:
    """
    해당 디렉터리의 실험 CSV들을 모두 읽어서 {실험번호: DataFrame} 형태로 반환.
    read_csv_kwargs로 encoding='utf-8', dtype=..., usecols=... 같은 옵션을 전달 가능.
    """
    files = find_experiment_files(dir_path)
    result: Dict[int, pd.DataFrame] = {}

    for p in files:
        m = FILENAME_REGEX.match(p.name)
        exp_no = int(m.group(1))  # 정규식 매치가 보장됨
        try:
            df = pd.read_csv(p, **read_csv_kwargs)
            result[exp_no] = df
        except Exception as e:
            # 파일 읽기 실패 시: 상황에 따라 raise 하거나 스킵하도록 선택
            raise RuntimeError(f"Failed to read {p}: {e}") from e

    return result

def clean_metricData(datasets: Dict[int, pd.DataFrame]) -> Dict[int, pd.DataFrame]:
    """
    datasets 내 모든 DataFrame에서 pid == 1 이고
    comm == "/bin/bash /entrypoint.sh" 인 row를 삭제하여 반환.
    원본 datasets는 변경하지 않고, 새로운 Dict 반환.
    """
    cleaned: Dict[int, pd.DataFrame] = {}

    for exp_no, df in datasets.items():
        before_count = len(df)
        df_cleaned = df.drop(
            df[(df["pid"] == 1) & (df["comm"] == "/bin/bash /entrypoint.sh")].index
        )
        after_count = len(df_cleaned)

        print(f"Experiment {exp_no}: {before_count} → {after_count} rows (삭제 {before_count - after_count})")
        cleaned[exp_no] = df_cleaned.reset_index(drop=True)

    return cleaned

def showDF(df: pd.DataFrame) -> None:
    """
    주어진 DataFrame에서 pod_name, timestamp, pid, comm 컬럼만 출력
    """
    required_cols = ["pod_name", "timestamp", "pid", "comm"]

    # 필요한 컬럼만 선택 (혹시 컬럼이 없으면 무시)
    available_cols = [c for c in required_cols if c in df.columns]
    if not available_cols:
        print("⚠️ 출력할 수 있는 지정 컬럼이 없습니다.")
        return

    # 앞부분 5개만 출력 (전체 출력은 df[available_cols].to_string(index=False) 사용 가능)
    print("\n=== 선택된 컬럼 출력 ===")
    print(df[available_cols].head(10).to_string(index=False))


def show_active0(df: pd.DataFrame) -> None:
    """
    DataFrame에서 pod_name == 'active-0' 인 row만 출력
    """
    if "pod_name" not in df.columns:
        print("⚠️ DataFrame에 'pod_name' 컬럼이 없습니다.")
        return

    filtered = df[df["pod_name"] == "active-0"]

    if filtered.empty:
        print("⚠️ pod_name == 'active-0' 데이터가 없습니다.")
    else:
        print(f"\n=== pod_name == 'active-0' 데이터 ({len(filtered)} rows) ===")
        print(filtered.to_string(index=False))  # 앞부분 10줄만 출력

def showAll(df: pd.DataFrame) -> None:
    """
    DataFrame의 전체 데이터를 모두 출력하고,
    row 개수를 함께 출력한다.
    """
    if df is None or df.empty:
        print("⚠️ DataFrame이 비어있습니다.")
        return

    print(f"\n=== DataFrame 전체 출력 (총 {len(df)} rows) ===")
    print(df.to_string(index=False))  # 전체 row 출력

def save_to_excel(df: pd.DataFrame, file_path: str, sheet_name: str = "Sheet1") -> None:
    """
    DataFrame을 Excel 파일(.xlsx)로 저장하는 함수
    """
    if df is None or df.empty:
        print("⚠️ 저장할 DataFrame이 비어있습니다.")
        return

    try:
        df.to_excel(file_path, sheet_name=sheet_name, index=False, engine="openpyxl")
        print(f"✅ DataFrame이 Excel 파일로 저장되었습니다: {file_path}")
    except Exception as e:
        print(f"❌ Excel 저장 중 오류 발생: {e}")

def build_usage_table(
    df: pd.DataFrame,
    pod_filter: str = "active-0",        # 현재는 정확히 일치. 차후 'active' 시작은 startswith로 변경
    ticks_per_sec: int = 100,             # Linux 보통 100 또는 250, 환경에 맞게 조정
    page_size: int = 4096                 # RSS(페이지)를 바이트로 변환할 때 사용
) -> pd.DataFrame:
    """
    pod_name이 pod_filter인 데이터만 사용하여 시점별 집계 테이블을 생성.
    - utime/stime: 누적값 -> per-pid 증가분을 구해 시점별 합계
    - vsize/rss: 순간값 -> 시점별 합계
    반환 컬럼:
      timestamp, utime_delta, stime_delta, cpu_time_sec, vsize_total, rss_pages_total, rss_bytes_total
    """

    # 1) 필터링: 현재는 정확히 'active-0'만. (차후 startswith 확장 예시: df[df["pod_name"].str.startswith("active")])
    if "pod_name" not in df.columns:
        raise KeyError("DataFrame에 'pod_name' 컬럼이 없습니다.")
    filtered = df[df["pod_name"] == pod_filter].copy()
    if filtered.empty:
        # 비어있어도 빈 DataFrame 형태로 반환
        return pd.DataFrame(columns=[
            "timestamp", "utime_delta", "stime_delta", "cpu_time_sec",
            "vsize_total", "rss_pages_total", "rss_bytes_total"
        ])

    # 2) 타임스탬프 정규화 및 정렬
    if "timestamp" not in filtered.columns:
        raise KeyError("DataFrame에 'timestamp' 컬럼이 없습니다.")
    filtered["timestamp"] = pd.to_datetime(filtered["timestamp"], errors="coerce")
    filtered = filtered.dropna(subset=["timestamp"])
    filtered = filtered.sort_values(["pid", "timestamp"])

    # 3) 누적 -> 증가분 계산(프로세스별)
    for col in ["utime", "stime"]:
        if col not in filtered.columns:
            raise KeyError(f"DataFrame에 '{col}' 컬럼이 없습니다.")
    filtered["utime_delta"] = filtered.groupby("pid")["utime"].diff().clip(lower=0).fillna(0)
    filtered["stime_delta"] = filtered.groupby("pid")["stime"].diff().clip(lower=0).fillna(0)

    # 4) 메모리 순간값 합계용 컬럼 확인
    for col in ["vsize", "rss"]:
        if col not in filtered.columns:
            raise KeyError(f"DataFrame에 '{col}' 컬럼이 없습니다.")

    # 5) 타임스탬프 단위로 집계
    agg = filtered.groupby("timestamp").agg(
        utime_delta=("utime_delta", "sum"),
        stime_delta=("stime_delta", "sum"),
        vsize_total=("vsize", "sum"),
        rss_pages_total=("rss", "sum"),
    ).reset_index()

    # 6) 파생 지표: CPU 시간(초), RSS 바이트 합계
    agg["cpu_time_sec"] = (agg["utime_delta"] + agg["stime_delta"]) / float(ticks_per_sec)
    agg["rss_bytes_total"] = agg["rss_pages_total"] * int(page_size)

    # 보기 좋게 컬럼 순서 정리
    agg = agg[[
        "timestamp",
        "utime_delta", "stime_delta", "cpu_time_sec",
        "vsize_total", "rss_pages_total", "rss_bytes_total"
    ]].sort_values("timestamp").reset_index(drop=True)

    return agg

# 사용 예시
if __name__ == "__main__":
    # 예: 현재 폴더 기준
    dir_path = "/home/soobin/data/KubeGC"  # 작업 디렉터리 경로로 바꿔주세요
    files = find_experiment_files(dir_path)
    print("발견된 파일:")
    for f in files:
        print(" -", f.name)

    # CSV 로드 (필요하면 encoding, dtype 등을 지정하세요)
    datasets = load_experiment_csvs(dir_path, encoding="utf-8")
    print(f"\n총 {len(datasets)}개 실험 데이터 로드 완료.")
    # 예: 1번 실험 데이터 정보 출력
    if 1 in datasets:
        print("exp 1 shape:", datasets[1].shape)
        df = datasets[1]
        for col in df.columns:
            print(col, end=', ')

    datasets = clean_metricData(datasets)
    showDF(datasets[1])
    show_active0(datasets[1])
    df_usage = build_usage_table(datasets[1])
    showAll(df_usage)
    save_to_excel(df_usage, "usage.xlsx")