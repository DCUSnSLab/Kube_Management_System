from kubernetes import client, config
import re

# 쿠버네티스 설정 로드
# config.load_incluster_config()  # 클러스터 내부 실행
config.load_kube_config()  # 로컬 실행 시

# API 클라이언트 생성
v1 = client.CoreV1Api()
custom_objects_api = client.CustomObjectsApi()

def convert_cpu_value(cpu_value):
    """CPU 값을 millicores(m) 또는 nanocores(n)에서 cores 단위로 변환"""
    if cpu_value.endswith("m"):
        return int(cpu_value.strip("m")) / 1000  # millicores -> cores
    elif cpu_value.endswith("n"):
        return int(cpu_value.strip("n")) / (10**9)  # nanocores -> cores
    else:
        return int(cpu_value)  # 이미 cores 단위인 경우

def convert_memory_value(memory_value):
    """메모리 값을 다양한 단위(n, Ki, Mi, Gi)에서 MB 단위로 변환"""
    unit_multipliers = {"Ki": 1/1024, "Mi": 1, "Gi": 1024, "Ti": 1024**2}

    match = re.match(r"(\d+)([A-Za-z]+)?", memory_value)
    if match:
        value, unit = match.groups()
        value = int(value)
        if unit in unit_multipliers:
            return value * unit_multipliers[unit]  # MB 단위로 변환
        elif unit == "n":  # 나노바이트(n) -> MB 변환
            return value / (1024**2)  # n -> MB
        else:
            return value / (1024**2)  # 바이트 -> MB
    return 0  # 변환 실패 시 0 MB 반환

def get_node_metrics():
    """쿠버네티스 Metrics API를 통해 CPU 및 메모리 사용량 가져오기"""
    try:
        metrics = custom_objects_api.list_cluster_custom_object(
            "metrics.k8s.io", "v1beta1", "nodes"
        )
        node_metrics = {}
        for node in metrics['items']:
            node_name = node['metadata']['name']
            cpu_usage = node['usage']['cpu']  # 예: "250m", "2401611066n"
            memory_usage = node['usage']['memory']  # 예: "500Mi", "2401611066n"

            # 변환
            cpu_cores = convert_cpu_value(cpu_usage)
            memory_mb = convert_memory_value(memory_usage)

            node_metrics[node_name] = {
                "CPU Usage (Cores)": cpu_cores,
                "Memory Usage (MB)": memory_mb
            }
        return node_metrics
    except Exception as e:
        print(f"Error fetching metrics: {e}")
        return {}

# 실행
node_metrics = get_node_metrics()

# 결과 출력
for node, metrics in node_metrics.items():
    print(f"Node: {node}")
    print(f"  CPU Usage: {metrics.get('CPU Usage (Cores)', 'N/A')} Cores")
    print(f"  Memory Usage: {metrics.get('Memory Usage (MB)', 'N/A')} MB")
    print("-" * 50)
