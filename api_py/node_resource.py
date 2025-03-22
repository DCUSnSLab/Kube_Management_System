from kubernetes import client, config

# 쿠버네티스 설정 로드 (외부에서 실행할 경우 load_kube_config() 사용)
# config.load_incluster_config()  # 클러스터 내부에서 실행할 경우
config.load_kube_config()  # 로컬에서 실행할 경우

# API 클라이언트 생성
v1 = client.CoreV1Api()
custom_objects_api = client.CustomObjectsApi()

def get_cluster_nodes():
    """클러스터 내 모든 노드 정보 가져오기"""
    nodes = v1.list_node().items
    node_names = [node.metadata.name for node in nodes]
    return node_names

def get_node_metrics():
    """쿠버네티스 Metrics API를 통해 CPU 및 메모리 사용량 가져오기"""
    try:
        metrics = custom_objects_api.list_cluster_custom_object(
            "metrics.k8s.io", "v1beta1", "nodes"
        )
        node_metrics = {}
        for node in metrics['items']:
            node_name = node['metadata']['name']
            cpu_usage = node['usage']['cpu']  # 예: "250m" (250 millicores)
            memory_usage = node['usage']['memory']  # 예: "500Mi"

            # 단위 변환
            cpu_cores = int(cpu_usage.strip('m')) / 1000  # millicores -> cores
            memory_mb = int(memory_usage.strip('Mi'))  # MiB -> MB

            node_metrics[node_name] = {
                "CPU Usage (Cores)": cpu_cores,
                "Memory Usage (MB)": memory_mb
            }
        return node_metrics
    except Exception as e:
        print(f"Error fetching metrics: {e}")
        return {}

def get_network_usage():
    """각 노드의 네트워크 사용량 가져오기"""
    nodes = v1.list_node().items
    network_usage = {}

    for node in nodes:
        node_name = node.metadata.name
        status = node.status
        addresses = status.addresses

        for addr in addresses:
            if addr.type == "InternalIP":
                internal_ip = addr.address
                break

        # 노드의 네트워크 트래픽 정보 조회 (네트워크 통계를 저장하는 곳이 없으므로, Prometheus 등을 이용해야 더 정확한 값 조회 가능)
        network_usage[node_name] = {
            "Internal IP": internal_ip,
            "Network Usage": "N/A (External monitoring needed)"
        }

    return network_usage

# 실행
node_names = get_cluster_nodes()
node_metrics = get_node_metrics()
network_metrics = get_network_usage()

# 결과 출력
for node in node_names:
    print(f"Node: {node}")
    print(f"  CPU Usage: {node_metrics.get(node, {}).get('CPU Usage (Cores)', 'N/A')} Cores")
    print(f"  Memory Usage: {node_metrics.get(node, {}).get('Memory Usage (MB)', 'N/A')} MB")
    print(f"  Internal IP: {network_metrics.get(node, {}).get('Internal IP', 'N/A')}")
    print("-" * 50)
