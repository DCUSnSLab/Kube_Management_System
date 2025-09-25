from pathlib import Path
import re
from typing import Dict, List, Tuple
import pandas as pd
from pathlib import PurePosixPath

from tool.data_graph import main

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

import re
import pandas as pd
from pathlib import PurePosixPath

def _split_pod_base_and_ordinal(name: str):
    """
    'active-0' -> ('active', 0)
    'worker-12' -> ('worker', 12)
    'singleton' -> ('singleton', None)
    """
    if not isinstance(name, str):
        return name, None
    m = re.match(r"^(.*?)-(\d+)$", name.strip())
    if m:
        base, idx = m.group(1), int(m.group(2))
        return base, idx
    return name.strip(), None

def _extract_comm_tail(comm: str) -> str:
    """
    comm의 마지막 토큰에서 파일명 stem 추출
    예) 'python ./programs/active/active_multithreaded.py' -> 'active_multithreaded'
    """
    if not isinstance(comm, str) or not comm.strip():
        return comm
    tokens = comm.strip().split()
    chosen = None
    for tok in reversed(tokens):
        if "/" in tok or "." in tok:
            chosen = tok
            break
    if chosen is None:
        chosen = tokens[-1]
    chosen = chosen.strip('\'"')
    stem = PurePosixPath(chosen).name
    if "." in stem:
        stem = stem.split(".")[0]
    return stem

def build_normalized_usage_table(
    df: pd.DataFrame,
    ticks_per_sec: int = 100,
    page_size: int = 4096,
    *,
    cycle_size: int = 100,  # CSV/입력 순서대로 cycle_size개씩 한 사이클
) -> pd.DataFrame:
    """
    - pod_name: 끝의 -숫자 제거하여 기본명으로 재정의, 숫자는 pod_ordinal로 분리
    - comm: 마지막 파일명의 stem으로 분류
    - 저장 컬럼:
      pod_name, pod_ordinal, timestamp, comm, state,
      utime, stime, cutime, num_threads,
      vsize, rss, rsslim, vm_rss_status,
      voluntary_ctxt_switches, nonvoluntary_ctxt_switches,
      read_bytes, write_bytes
    - 누적값에 대해 *_delta 생성:
      utime, stime, cutime,
      voluntary_ctxt_switches, nonvoluntary_ctxt_switches,
      read_bytes, write_bytes,
      vsize, rss, rsslim
    - 델타 계산은 (pod_name_base, pod_ordinal, pid) 단위로 정렬·차분
    - CSV 입력 순서대로 cycle_id 부여(0부터), 한 사이클은 cycle_size개 레코드
    """
    required = [
        "pod_name", "timestamp", "comm", "state", "pid",
        "utime", "stime", "cutime", "minflt", "majflt",
        "num_threads",
        "vsize", "rss", "rsslim", "vm_rss_status",
        "voluntary_ctxt_switches", "nonvoluntary_ctxt_switches",
        "read_bytes", "write_bytes"
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"필수 컬럼 누락: {missing}")
    if cycle_size is None or cycle_size <= 0:
        raise ValueError("cycle_size는 1 이상의 정수여야 합니다.")

    # --- 원본 순서 보존용 위치 ---
    out = df.copy()
    out["_row_pos"] = range(len(out))  # CSV/입력 순서 인덱스

    # 1) pod_name 분해 → base, ordinal
    base_and_idx = out["pod_name"].astype(str).apply(_split_pod_base_and_ordinal)
    out["pod_name"] = base_and_idx.apply(lambda x: x[0])
    out["pod_ordinal"] = base_and_idx.apply(lambda x: x[1])

    # pod_ordinal이 None이면 그룹에서 누락되지 않도록 -1로 치환
    out["pod_ordinal"] = out["pod_ordinal"].fillna(-1).astype("int64")

    # 2) comm 정규화
    out["comm"] = out["comm"].astype(str).map(_extract_comm_tail)

    # 3) timestamp 정규화
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce")
    out = out.dropna(subset=["timestamp", "pid"]).copy()

    # 누적값 컬럼 정의
    cumulative_cols = [
        "utime", "stime", "cutime", "minflt", "majflt",
        "voluntary_ctxt_switches", "nonvoluntary_ctxt_switches",
        "read_bytes", "write_bytes",
        "vsize", "rss", "rsslim"
    ]
    for col in cumulative_cols:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    # 정렬된 복사본으로 델타 계산
    sort_keys = ["pod_name", "pod_ordinal", "pid", "timestamp", "_row_pos"]
    tmp = out.sort_values(sort_keys).copy()
    gkeys = ["pod_name", "pod_ordinal", "pid"]

    for col in cumulative_cols:
        dcol = f"{col}_delta"
        tmp[dcol] = (
            tmp.groupby(gkeys, sort=False)[col]
            .diff()
            .clip(lower=0)
            .fillna(0.0)
            .astype("float64")
        )

    # --- cpu_time(utime+stime) 계산 ---
    # 🔹 utime+stime = cpu_time 계산
    tmp["cpu_time"] = tmp["utime"] + tmp["stime"]

    # 🔹 cpu_time_delta 계산
    tmp["cpu_time_delta"] = (
        tmp.groupby(gkeys, sort=False)["cpu_time"]
        .diff()
        .clip(lower=0)
        .fillna(0.0)
        .astype("float64")
    )

    # --- cpu_rate 계산 ---
    # curr_time = utime_delta + stime_delta
    tmp["cpu_time_sum"] = tmp["utime_delta"] + tmp["stime_delta"]

    # prev_time = 이전 행의 cpu_time_sum
    tmp["prev_cpu_time_sum"] = tmp.groupby(gkeys, sort=False)["cpu_time_sum"].shift(1)

    # (curr - prev) / prev * 100
    tmp["cpu_rate"] = (
                              (tmp["cpu_time_sum"] - tmp["prev_cpu_time_sum"]) / tmp["prev_cpu_time_sum"]
                      ) * 100

    # 첫 행은 NaN -> 0으로 채움
    tmp["cpu_rate"] = tmp["cpu_rate"].fillna(0.0)

    # 델타 및 cpu_rate, cpu_time, cpu_time_delta 원래 순서로 붙이기
    extra_cols = [f"{c}_delta" for c in cumulative_cols] + [
        "cpu_rate", "cpu_time", "cpu_time_delta"
    ]
    out = out.merge(tmp[["_row_pos"] + extra_cols], on="_row_pos", how="left")

    # cycle_id 부여
    out["cycle_id"] = (out["_row_pos"] // int(cycle_size)).astype("int64")

    # 최종 컬럼 정리
    keep_cols = [
        "cycle_id",
        "pod_name", "pod_ordinal", "timestamp", "comm", "state",
        "utime", "utime_delta",
        "stime", "stime_delta",
        "cutime", "cutime_delta",
        "cpu_time", "cpu_time_delta",
        "minflt", "minflt_delta", "majflt", "majflt_delta",
        "num_threads",
        "vsize", "vsize_delta",
        "rss", "rss_delta",
        "rsslim", "rsslim_delta",
        "vm_rss_status",
        "voluntary_ctxt_switches", "voluntary_ctxt_switches_delta",
        "nonvoluntary_ctxt_switches", "nonvoluntary_ctxt_switches_delta",
        "read_bytes", "read_bytes_delta",
        "write_bytes", "write_bytes_delta",
        "cpu_rate",  # 👈 추가됨
        "pid",
    ]
    out = out[keep_cols + (["_row_pos"] if "_row_pos" in out.columns else [])] \
        .sort_values(["cycle_id", "_row_pos"]) \
        .reset_index(drop=True)

    return out


# 사용 예시
if __name__ == "__main__":
    # 예: 현재 폴더 기준
    dir_path = "experiment_data/"  # 작업 디렉터리 경로로 바꿔주세요
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
    #showDF(datasets[1])
    #show_active0(datasets[1])

    normal_datas = dict()
    for i, dataset in enumerate(datasets):
        normal_datas['experiment_'+str(i)] = (build_normalized_usage_table(datasets[i+1]))
    #showAll(df_usage)
    save_to_excel(normal_datas['experiment_'+str(0)], "usage1.xlsx")
    print(normal_datas.keys())
    main(normal_datas)