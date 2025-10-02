from concurrent.futures import ThreadPoolExecutor, as_completed

from kubernetes import client, config
from pod import Pod
# from processDB import initialize_database
from DB_postgresql import initialize_database, is_deleted_in_DB, is_exist_in_DB

from datetime import datetime, timezone, timedelta
import time
from multiprocessing import Event

class GarbageCollector():
    def __init__(self, namespace='default', container=None, isDev=False, stop_event=None):
        config.load_kube_config()  # 필수 config값 불러옴
        self.v1 = client.CoreV1Api()  # api
        self.namespace: str = namespace
        self.container = container
        self.devMode: bool = isDev
        self.exclude: list = ["ssh-wldnjs269", "ssh-marsberry", "swlabssh"]
        self.podlist: dict = {}
        self.intervalTime = 60
        self.count = 1
        self._stop_event = stop_event or Event()

    def manage(self, interval=60, worker=10):
        if self.devMode is True:
            self.namespace = 'gc-simulator'

        start_anchor = time.perf_counter()  # 고정 기준 시각
        while True:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"{timestamp} Update Pod List...")

            # 목표 시각 계산 (fixed-rate)
            target_time = start_anchor + (self.count * interval)

            self.getPodList()
            print('='*10+f"Start to Check Data {self.count} times"+'='*10)

            # 시간 측정
            start_ts = time.perf_counter()
            now_wall = timestamp
            print(f"[TIMING] Collecting Pod Data... for {len(self.pod_list)} pods with {worker} workers "
                  f"at {now_wall} (perf_counter={start_ts:.3f}s)")

            futures = []
            with ThreadPoolExecutor(max_workers=worker) as executor:
                for p_name in self.podlist.keys():
                    pod = self.podlist[p_name]
                    futures.append(executor.submit(pod.collectProcessAndHistory))

                for fut in as_completed(futures):
                    try:
                        _ = fut.result()
                    except Exception as e:
                        print(f"[WARN] Fail to collect status for a pod: {e}")
            elapsed = time.perf_counter() - start_ts
            print(f"[TIMING] Collected statuses for {len(self.pod_list)} pods [{elapsed:.3f}s]")

            for p_name, p_obj in self.podlist.items():
                print(p_name)
                # save logging data
                p_obj.insertPodInfo()
                p_obj.saveProcessDataToDB()

                should_gc, gc_reason, type = p_obj.shouldGarbageCollection()

                if should_gc is True:
                    print(f"\n[Garbage Collector] Pod '{p_name}' will be deleted")
                    print(f"  Reason: {gc_reason}")
                    print(f"  Type: {type}")
                    p_obj.insert_DeleteReason(gc_reason)
                    p_obj.save_DeleteReason_to_DB()
                    self.deletePod(p_name)  # pod 삭제

                print('-' * 50)

            self.count += 1
            if self._stop_event.is_set():
                break

            now = time.perf_counter()
            sleep_time = target_time - now
            if sleep_time > 0:
                # 종료 예정 시각 (UTC 기준)
                wakeup_wall = datetime.now(timezone.utc) + timedelta(seconds=sleep_time)
                print(f"[SLEEP] Sleeping {sleep_time:.2f}s "
                      f"(until {wakeup_wall.strftime('%Y-%m-%d %H:%M:%S %Z')})")
                time.sleep(sleep_time)
            else:
                # 오버런: 작업이 60초를 넘김 -> 드리프트 경고만 하고 바로 다음 회차 진행
                print(f"[DRIFT] Overran by {-sleep_time:.3f}s; skipping sleep to realign")

        print("Garbage Collector Stopped")

    def getPodList(self):
        #현재 네임스테이스의 Pod 목록을 가져옴
        pods = self.v1.list_namespaced_pod(self.namespace).items
        if not pods:
            print(f"No resources found in {self.namespace} namespace.")
            self.recordDeletedPod(self.podlist)
            self.podlist = {}
            return

        #제외할 pod 필터링
        filtering_pods = [
            pod for pod in pods
            if not any(
                pod.metadata.name == name or pod.metadata.name.startswith(name)
                for name in self.exclude
            )
        ]
        new_podlist = {}
        for p in filtering_pods:
            pod_name = p.metadata.name
            if pod_name in self.podlist:
                #기존 Pod객체 재사용
                new_podlist[pod_name] = self.podlist[pod_name]
            else:
                core_api = client.CoreV1Api()
                new_podlist[pod_name] = Pod(core_api, p)
                pod_obj = new_podlist[pod_name]

                if not pod_obj.isExistInDB() or pod_obj.isDeletedInDB():
                    print(f"Initializing new pod: {pod_name}")
                    pod_obj.initPodData()

        removed_pod = set(self.podlist.keys()) - set(new_podlist.keys())
        self.recordDeletedPod(removed_pod)

        # 새로운 목록으로 변경
        self.podlist = new_podlist

    def recordDeletedPod(self, removed_pods):
        """
        Record deleted pods
        Reason is 'UNKOWN' when pod is deleted
        """
        for rm_p in removed_pods:
            pod_obj = self.podlist[rm_p]
            if not pod_obj.isDeletedInDB():  # DB에 삭제된 시간이 없는 경우만 처리
                pod_obj.insert_DeleteReason("UNKNOWN - Pod deleted")
                pod_obj.save_DeleteReason_to_DB()
            print(f"Pod removed: {rm_p}")

    def deletePod(self, p_name):
        print(p_name, "______REMOVE____")
        self.v1.delete_namespaced_pod(p_name, self.namespace)

if __name__ == "__main__":
    # initialize_database()  # DB 초기화 (sqlite)
    initialize_database()  # PostgreSQL DB 초기화

    #네임스페이스 값을 비워두면 'default'로 지정
    gc = GarbageCollector(namespace='swlabpods', isDev=True)
    gc.manage()
    # gc.logging()
