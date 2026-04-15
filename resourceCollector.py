from kubernetes import client
from DB_postgresql import save_namespace_resource_usage, save_pod_resource_usage
from datetime import datetime, timezone
import logging


class ResourceCollector:
    """
    Kubernetes Metrics API를 사용하여 네임스페이스/Pod별 CPU·메모리 사용량을 수집하고
    PostgreSQL에 저장하는 모듈.

    수집 항목:
        - Pod별: CPU 사용량 (millicores), Memory 사용량 (bytes)
        - 네임스페이스 레벨: 위 항목의 합산 + requests/limits + Pod/PVC 카운트
    """

    def __init__(self, namespace, exclude_list=None):
        self.metrics_api = client.CustomObjectsApi()
        self.core_api = client.CoreV1Api()
        self.namespace = namespace
        self.exclude_list = exclude_list or []

    def collect_and_save(self):
        """CPU/Memory 사용량 1회 수집 → DB 저장"""
        try:
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

            # 1. Metrics API에서 Pod별 CPU/Memory 실측값 수집
            pod_metrics = self._get_pod_metrics()

            # 2. CoreV1Api에서 Pod spec (requests/limits) 수집
            pod_specs = self._get_pod_specs()

            # 3. PVC 카운트
            pvc_count = self._get_pvc_count()

            # 4. Pod별 데이터 저장 + 네임스페이스 합산
            pod_records = []
            total_cpu_usage = 0.0
            total_memory_usage = 0
            total_cpu_requests = 0.0
            total_memory_requests = 0
            total_cpu_limits = 0.0
            total_memory_limits = 0
            running_pod_count = 0

            for pod_name, metrics in pod_metrics.items():
                cpu_usage = metrics['cpu_millicores']
                memory_usage = metrics['memory_bytes']

                pod_records.append({
                    'pod_name': pod_name,
                    'namespace': self.namespace,
                    'timestamp': timestamp,
                    'cpu_usage_millicores': cpu_usage,
                    'memory_usage_bytes': memory_usage
                })

                total_cpu_usage += cpu_usage
                total_memory_usage += memory_usage
                running_pod_count += 1

            # requests/limits 합산 (필터링된 Pod만)
            for pod_name, spec in pod_specs.items():
                total_cpu_requests += spec.get('cpu_requests', 0)
                total_memory_requests += spec.get('memory_requests', 0)
                total_cpu_limits += spec.get('cpu_limits', 0)
                total_memory_limits += spec.get('memory_limits', 0)

            # 5. DB 저장
            if pod_records:
                save_pod_resource_usage(pod_records)

            save_namespace_resource_usage(
                namespace=self.namespace,
                timestamp=timestamp,
                cpu_usage_millicores=total_cpu_usage,
                memory_usage_bytes=total_memory_usage,
                cpu_requests_millicores=total_cpu_requests,
                memory_requests_bytes=total_memory_requests,
                cpu_limits_millicores=total_cpu_limits,
                memory_limits_bytes=total_memory_limits,
                running_pod_count=running_pod_count,
                pvc_count=pvc_count
            )

            print(f"[ResourceCollector] Saved: {running_pod_count} pods, "
                  f"CPU={total_cpu_usage:.2f}m, MEM={total_memory_usage / (1024 * 1024):.1f}MiB")

        except Exception as e:
            print(f"[ResourceCollector][ERROR] Failed to collect resource usage: {e}")
            logging.error(f"ResourceCollector error: {e}")

    def _is_excluded(self, pod_name):
        """GarbageCollector와 동일한 exclude 필터 적용"""
        return any(
            pod_name == name or pod_name.startswith(name)
            for name in self.exclude_list
        )

    def _get_pod_metrics(self):
        """
        Metrics API(metrics.k8s.io/v1beta1)에서 Pod별 CPU/Memory 실측값 수집.
        Pod 내 모든 컨테이너의 사용량을 합산.
        """
        result = {}
        try:
            metrics = self.metrics_api.list_namespaced_custom_object(
                group="metrics.k8s.io",
                version="v1beta1",
                namespace=self.namespace,
                plural="pods"
            )

            for item in metrics.get('items', []):
                pod_name = item['metadata']['name']
                if self._is_excluded(pod_name):
                    continue

                total_cpu = 0.0
                total_memory = 0

                for container in item.get('containers', []):
                    usage = container.get('usage', {})
                    total_cpu += self._parse_cpu(usage.get('cpu', '0'))
                    total_memory += self._parse_memory(usage.get('memory', '0'))

                result[pod_name] = {
                    'cpu_millicores': total_cpu,
                    'memory_bytes': total_memory
                }

        except Exception as e:
            print(f"[ResourceCollector][WARN] Failed to get pod metrics: {e}")
            logging.error(f"Failed to get pod metrics: {e}")

        return result

    def _get_pod_specs(self):
        """Pod spec에서 requests/limits 값을 추출하여 반환"""
        result = {}
        try:
            pods = self.core_api.list_namespaced_pod(self.namespace).items
            if not pods:
                return result

            for pod in pods:
                pod_name = pod.metadata.name
                if self._is_excluded(pod_name):
                    continue

                cpu_requests = 0.0
                memory_requests = 0
                cpu_limits = 0.0
                memory_limits = 0

                if pod.spec and pod.spec.containers:
                    for container in pod.spec.containers:
                        resources = container.resources
                        if resources:
                            if resources.requests:
                                cpu_requests += self._parse_cpu(
                                    resources.requests.get('cpu', '0'))
                                memory_requests += self._parse_memory(
                                    resources.requests.get('memory', '0'))
                            if resources.limits:
                                cpu_limits += self._parse_cpu(
                                    resources.limits.get('cpu', '0'))
                                memory_limits += self._parse_memory(
                                    resources.limits.get('memory', '0'))

                result[pod_name] = {
                    'cpu_requests': cpu_requests,
                    'memory_requests': memory_requests,
                    'cpu_limits': cpu_limits,
                    'memory_limits': memory_limits
                }

        except Exception as e:
            print(f"[ResourceCollector][WARN] Failed to get pod specs: {e}")
            logging.error(f"Failed to get pod specs: {e}")

        return result

    def _get_pvc_count(self):
        """네임스페이스의 PVC 개수 반환"""
        try:
            pvcs = self.core_api.list_namespaced_persistent_volume_claim(
                self.namespace).items
            return len(pvcs) if pvcs else 0
        except Exception as e:
            print(f"[ResourceCollector][WARN] Failed to get PVC count: {e}")
            return 0

    @staticmethod
    def _parse_cpu(value):
        """
        CPU 문자열을 millicores(float)로 변환.

        Examples:
            "123456789n" → 123.456789 (nanocores → millicores)
            "500u"       → 0.5        (microcores → millicores)
            "100m"       → 100.0      (millicores)
            "0.5"        → 500.0      (cores → millicores)
            "1"          → 1000.0     (cores → millicores)
        """
        if not value or value == '0':
            return 0.0
        value = str(value)
        if value.endswith('n'):
            return int(value[:-1]) / 1_000_000  # nanocores → millicores
        elif value.endswith('u'):
            return int(value[:-1]) / 1_000  # microcores → millicores
        elif value.endswith('m'):
            return float(value[:-1])  # already millicores
        else:
            return float(value) * 1000  # cores → millicores

    @staticmethod
    def _parse_memory(value):
        """
        Memory 문자열을 bytes(int)로 변환.

        Examples:
            "128Mi"   → 134217728  (MiB → bytes)
            "1Gi"     → 1073741824 (GiB → bytes)
            "256Ki"   → 262144     (KiB → bytes)
            "1000000" → 1000000    (bytes)
        """
        if not value or value == '0':
            return 0
        value = str(value)
        units = {
            'Ki': 1024,
            'Mi': 1024 ** 2,
            'Gi': 1024 ** 3,
            'Ti': 1024 ** 4,
            'K': 1000,
            'M': 1000 ** 2,
            'G': 1000 ** 3,
            'T': 1000 ** 4,
        }
        for suffix, multiplier in units.items():
            if value.endswith(suffix):
                return int(float(value[:-len(suffix)]) * multiplier)
        # plain integer = bytes
        return int(value)
