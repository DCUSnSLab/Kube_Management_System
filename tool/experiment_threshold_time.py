import re
import os
from pathlib import Path
from typing import Tuple, Dict, List, Optional
import pandas as pd

# ---------------------------
# Filename & parsing helpers
# ---------------------------

FILENAME_RE = re.compile(
    r'^(?P<ns>gc-(?P<thresh>\d{2})m)_(?P<ts>\d{8}_\d{6})_(?P<kind>process_(?:summary|classification)_experiment)\d*\.csv$'
)

def parse_filename_info(filename: str) -> Optional[dict]:
    m = FILENAME_RE.match(Path(filename).name)
    if not m:
        return None
    ns = m.group('ns')
    thresh_minutes = int(m.group('thresh'))
    timestamp_key = m.group('ts')
    kind_full = m.group('kind')
    kind = 'summary' if 'summary' in kind_full else 'classification'
    return {'namespace': ns, 'thresh_minutes': thresh_minutes, 'timestamp_key': timestamp_key, 'kind': kind}


def extract_pod_state_from_name(pod_name: str) -> Optional[str]:
    if not isinstance(pod_name, str):
        return None
    m = re.match(r'^(active|background|running|idle)\b', pod_name)
    return m.group(1) if m else None


def extract_scenario_from_comm(comm: str) -> Optional[str]:
    if not isinstance(comm, str) or not comm:
        return None
    part = comm.strip().split('/')[-1]
    part = re.sub(r'\.py(?:\s.*)?$', '', part)
    part = re.sub(r'^(active_|inactive_|running_|bg_)', '', part)
    return part or None


# ---------------------------
# Core transformation
# ---------------------------

def build_per_pod_scenario(class_df: pd.DataFrame) -> pd.DataFrame:
    if class_df.empty:
        return pd.DataFrame(columns=['experiment_id', 'pod_name', 'timestamp', 'scenario'])
    tmp = class_df.copy()
    tmp['scenario'] = tmp['comm'].apply(extract_scenario_from_comm)
    def pick_mode(series: pd.Series):
        nonnull = series.dropna()
        if nonnull.empty:
            return None
        return nonnull.mode().iloc[0]
    per_pod = tmp.groupby(['experiment_id', 'pod_name', 'timestamp'], as_index=False)['scenario'].agg(pick_mode)
    return per_pod


def merge_summary_and_class(summary_df: pd.DataFrame, class_df: pd.DataFrame, ns: str, thresh_minutes: int) -> pd.DataFrame:
    per_pod_scenario = build_per_pod_scenario(class_df)
    merged = summary_df.merge(per_pod_scenario, on=['experiment_id','pod_name','timestamp'], how='left')
    merged['pod_state_from_name'] = merged['pod_name'].apply(extract_pod_state_from_name)
    merged['namespace'] = ns
    merged['threshold_minutes'] = thresh_minutes
    merged['status'] = merged['status'].astype(str).str.lower()
    return merged


# ---------------------------
# Statistics
# ---------------------------

def compute_gc_decision_per_pod(merged: pd.DataFrame) -> pd.DataFrame:
    if merged.empty:
        return pd.DataFrame(columns=['experiment_id','namespace','threshold_minutes','pod_name','actual_active','decision_deleted'])
    merged = merged.copy()
    merged['actual_active'] = merged['pod_state_from_name'].fillna('').ne('idle')
    per_pod = (
        merged
        .assign(is_gc=merged['status'].eq('gc'))
        .groupby(['experiment_id','namespace','threshold_minutes','pod_name','actual_active'], as_index=False)['is_gc']
        .max()
        .rename(columns={'is_gc':'decision_deleted'})
    )
    return per_pod


def confusion_counts(per_pod: pd.DataFrame) -> Dict[str,int]:
    if per_pod.empty:
        return {'TP':0,'FP':0,'TN':0,'FN':0}
    actual_inactive = ~per_pod['actual_active']
    predicted = per_pod['decision_deleted']
    TP = int(((predicted) & (actual_inactive)).sum())
    FP = int(((predicted) & (~actual_inactive)).sum())
    TN = int(((~predicted) & (~actual_inactive)).sum())
    FN = int(((~predicted) & (actual_inactive)).sum())
    return {'TP':TP,'FP':FP,'TN':TN,'FN':FN}


def precision_recall(tp:int, fp:int, fn:int) -> Tuple[Optional[float], Optional[float]]:
    precision = tp / (tp + fp) if (tp + fp) > 0 else None
    recall = tp / (tp + fn) if (tp + fn) > 0 else None
    return precision, recall


def false_positive_rate(per_pod: pd.DataFrame) -> Optional[float]:
    if per_pod.empty:
        return None
    predicted_deleted = per_pod['decision_deleted']
    if predicted_deleted.sum() == 0:
        return None
    actual_active = per_pod['actual_active']
    fp = int(((predicted_deleted) & (actual_active)).sum())
    total_deleted = int(predicted_deleted.sum())
    return fp / total_deleted if total_deleted > 0 else None


def miss_rate(per_pod: pd.DataFrame) -> Optional[float]:
    if per_pod.empty:
        return None
    actual_inactive_mask = ~per_pod['actual_active']
    denom = int(actual_inactive_mask.sum())
    if denom == 0:
        return None
    fn = int(((~per_pod['decision_deleted']) & actual_inactive_mask).sum())
    return fn / denom


def compute_stats_table(per_pod: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Return both per-experiment and aggregated threshold-level statistics.
    """
    if per_pod.empty:
        return pd.DataFrame(), pd.DataFrame()

    def compute_block(group: pd.DataFrame):
        cc = confusion_counts(group)
        prec, rec = precision_recall(cc['TP'], cc['FP'], cc['FN'])
        fpr = false_positive_rate(group)
        mr = miss_rate(group)
        return {
            'total_pods_created': len(group),
            'deleted_pods': int(group['decision_deleted'].sum()),
            **cc,
            'false_positive_rate': fpr,
            'miss_rate': mr,
            'precision': prec,
            'recall': rec,
        }

    # Per-experiment
    per_exp_rows = []
    for (eid, thresh, ns), group in per_pod.groupby(['experiment_id','threshold_minutes','namespace']):
        d = compute_block(group)
        d.update({'experiment_id': eid, 'threshold_minutes': thresh, 'namespace': ns})
        per_exp_rows.append(d)
    per_exp_df = pd.DataFrame(per_exp_rows)

    # Aggregated per threshold
    per_thr_rows = []
    for (thresh, ns), group in per_pod.groupby(['threshold_minutes','namespace']):
        d = compute_block(group)
        d.update({'threshold_minutes': thresh, 'namespace': ns})
        per_thr_rows.append(d)
    per_thr_df = pd.DataFrame(per_thr_rows)
    return per_exp_df, per_thr_df


# ---------------------------
# Orchestration
# ---------------------------

def find_files(base_dir: str) -> List[Path]:
    p = Path(base_dir)
    return list(p.glob('gc-??m_*_process_*_experiment*.csv'))


def group_files_by_experiment(files: List[Path]) -> Dict[Tuple[str,str], Dict[str, List[Path]]]:
    buckets: Dict[Tuple[str,str], Dict[str, List[Path]]] = {}
    for f in files:
        info = parse_filename_info(f.name)
        if not info:
            continue
        key = (info['namespace'], info['timestamp_key'])
        bucket = buckets.setdefault(key, {'summary': [], 'classification': []})
        bucket[info['kind']].append(f)
    return buckets


def process_experiment_bucket(namespace: str, timestamp_key: str, files: Dict[str, List[Path]], out_dir: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    sum_dfs = [pd.read_csv(p) for p in files.get('summary', [])]
    cls_dfs = [pd.read_csv(p) for p in files.get('classification', [])]
    if not sum_dfs:
        return pd.DataFrame(), pd.DataFrame()
    summary_df = pd.concat(sum_dfs, ignore_index=True)
    class_df = pd.concat(cls_dfs, ignore_index=True) if cls_dfs else pd.DataFrame(columns=['experiment_id','pod_name','timestamp','comm'])
    thresh_minutes = int(namespace.split('-')[1].replace('m',''))
    merged = merge_summary_and_class(summary_df, class_df, namespace, thresh_minutes)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    merged_path = out_dir / f'{namespace}_{timestamp_key}_merged.csv'
    merged.to_csv(merged_path, index=False, encoding='utf-8')
    per_pod = compute_gc_decision_per_pod(merged)
    return merged, per_pod


def process_directory(base_dir: str, out_dir: Optional[str] = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if out_dir is None:
        out_dir = base_dir
    files = find_files(base_dir)
    groups = group_files_by_experiment(files)
    all_merged = []
    all_per_pod = []

    for (ns, ts), kind_map in groups.items():
        merged, per_pod = process_experiment_bucket(ns, ts, kind_map, out_dir)
        if not merged.empty:
            all_merged.append(merged)
        if not per_pod.empty:
            all_per_pod.append(per_pod)

    merged_concat = pd.concat(all_merged, ignore_index=True) if all_merged else pd.DataFrame()
    per_pod_concat = pd.concat(all_per_pod, ignore_index=True) if all_per_pod else pd.DataFrame()

    per_exp_df, per_thr_df = compute_stats_table(per_pod_concat)
    out_dir = Path(out_dir)
    per_thr_df.to_csv(out_dir / 'gc_stats_summary.csv', index=False, encoding='utf-8')
    per_exp_df.to_csv(out_dir / 'gc_stats_per_experiment.csv', index=False, encoding='utf-8')
    return merged_concat, per_thr_df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Kubernetes GC CSV analyzer")
    parser.add_argument("--base_dir", type=str, default=r"C:\dev\kubernetes\Kube_Management_System\simulator\data",
                        help="Directory containing the CSV files")
    parser.add_argument("--out_dir", type=str, default=None, help="Directory to write outputs (defaults to base_dir)")
    args = parser.parse_args()

    merged_concat, stats = process_directory(args.base_dir, args.out_dir)
    print("Merged rows:", 0 if merged_concat is None else len(merged_concat))
    print("Stats rows:", 0 if stats is None else len(stats))
