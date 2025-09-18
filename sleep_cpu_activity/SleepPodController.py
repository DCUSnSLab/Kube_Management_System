import csv
from datetime import datetime
import os

from kubernetes import client, config, watch
import time
import random

from pod import Pod
from processManager import ProcessManager
from simulator.generator import Generator

class SleepPodController(Generator):
    """
    기존 생성기를 상속받도록 만듦
    """
    def __init__(self, namespace: str = 'gc-simulator'):
        config.load_kube_config()
        self.core_v1 = client.CoreV1Api()
        self.namespace = namespace

        self.interval = 60  # 1분 간격 검사
        self.cnt = 10  # 10분 (1분 * 10회)

        self.sleep_pod_manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "experiment-pod"
            },
            "spec": {
                "containers": [
                    {
                        "name": "experiment-container",
                        "image": "harbor.cu.ac.kr/swlabpods/gc_sleep_pod:0.1",
                        "resources": {
                            "requests": {
                                "cpu": "50m",
                                "memory": "150Mi"
                            },
                            "limits": {
                                "cpu": "100m",
                                "memory": "200Mi"
                            }
                        },
                        "env": [
                            {
                                "name": "PROCESS_STATE",
                                "value": "active"
                            }
                        ]
                    }
                ]
            }
        }

    def experiment(self):
        """
        pod 생성 및 삭제, 데이터를 가져와서 저장
        숫자 랜덤 생성
        """
        try:
            active, idle = self.createRamdomNumPair()
            self.createSleepPod(active, 'active')
            self.createSleepPod(idle, 'idle')

            manager = {}
            i = 0
            while i < self.cnt:
                pods = self.core_v1.list_namespaced_pod(self.namespace).items
                pod_names = [pod.metadata.name for pod in pods]
                self.waitForPodRunning(pod_names)

                for p in pods:
                    if p.metadata.name not in manager:
                        manager[p.metadata.name] = ProcessManager(self.core_v1, p)
                    pod = Pod(self.core_v1, p)
                    pod.insertProcessData()

                    pm = manager[p.metadata.name]
                    classification, summary = pm.analyze(pod.processes)
                    self.saveClassificationToCsv(classification, p.metadata.name)
                    self.saveSummaryToCsv(summary, p.metadata.name)
                i += 1
                time.sleep(self.interval)

        except KeyboardInterrupt:
            print("Keyboard Interrupted. Cleanning up...")
            self.deletePod()

    def createRamdomNumPair(self, total=100, min_ratio=0.3, max_ratio=0.7):
        while True:
            a = random.randint(1, total - 1)
            b = total - a

            ratio = min(a, b) / max(a, b)

            if min_ratio <= ratio <= max_ratio:
                return a, b

    def createSleepPod(self, total: int, state: str):
        count = 0
        while count < total:
            self.sleep_pod_manifest['metadata']['name'] = state+'-'+str(count)
            next(env for env in self.sleep_pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'PROCESS_STATE')['value'] = state

            self.core_v1.create_namespaced_pod(namespace=self.namespace, body=self.sleep_pod_manifest)
            print(f"{state} pod {count} created")
            count += 1

    def waitForPodRunning(self, pods, interval=5):
        start_time = time.time()
        pod_statuses = {p: "Pending" for p in pods}

        while True:
            all_running = True
            for pod_name in pods:
                pod = self.core_v1.read_namespaced_pod(pod_name, self.namespace)
                phase = pod.status.phase
                pod_statuses[pod_name] = phase
                print(f"[STATUS] Pod {pod_name} -> {phase}")

                if phase != "Running":
                    all_running = False

            if all_running:
                print("[READY] All pods are Running")
                break

            time.sleep(interval)

    def saveClassificationToCsv(self, classification, pod_name):
        """
        분류한 딕셔너리와 분류 결과 요약한 딕셔너리를 csv로 저장
        classification: 프로세스별 분석 결과
        summary: 프로세스 분석 결과 요약 (active, idle 등 분류 결과를 요약)
        """
        filename = "./data/experiment_sleep_classification.csv"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")

        if not classification:
            return

        for proc in classification:
            proc["pod_name"] = pod_name
            proc["timestamp"] = timestamp

        file_exists = os.path.isfile(filename)
        keys = classification[0].keys()

        with open(filename, "a", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=keys)
            if not file_exists:
                writer.writeheader()
            writer.writerows(classification)

        print(f"[SAVE] Appended {len(classification)} rows from {pod_name} to {filename}")

    def saveSummaryToCsv(self, summary, pod_name):
        """
        모든 파드 summary 결과를 하나의 CSV에 누적 저장
        """
        filename = "./data/experiment_sleep_summary.csv"
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")

        if not summary:
            return

        row = {"pod_name": pod_name, "timestamp": timestamp}
        row.update(summary)

        file_exists = os.path.isfile(filename)
        keys = row.keys()

        with open(filename, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)

        print(f"[SAVE] Appended summary for {pod_name} to {filename}")


if __name__ == "__main__":
    spc = SleepPodController()
    spc.experiment()
