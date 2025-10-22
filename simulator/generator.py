from typing import Dict

from kubernetes import client, config, utils
import time
from datetime import datetime, timezone, timedelta
from garbagecollector import GarbageCollector
from multiprocessing import Process, Event
import random

from pod import Pod
from processManager import ProcessManager
from concurrent.futures import ThreadPoolExecutor, as_completed

from simulator.orchestrator import QueueWriter


def run_gc(ns, sc, log_q=None):
    # 표준출력 리다이렉트 (GC 프로세스 안에서 실행)
    if log_q is not None:
        import sys
        sys.stdout = QueueWriter(log_q, ns, "stdout")
        sys.stderr = QueueWriter(log_q, ns, "stderr")

    gc = GarbageCollector(namespace=ns, isDev=False, stop_event=sc)
    gc.manage()


class Generator:
    def __init__(self, namespace: str = 'gc-simulator', log_queue=None):
        config.load_kube_config()
        self.coreV1 = client.CoreV1Api()
        self.appV1 = client.AppsV1Api()
        self.namespace: str = namespace
        self.pod_list: dict = {}
        self.intervalTime = 120  # pod 생성 반복 주기
        self.times = 1  # 반복할 횟수
        self.count = 0  # 반복한 횟수
        self.active = 0  # active pods
        self.idle = 0  # idle pods
        self.running = 0  # running pods
        self.bg_active = 0  # background active pods
        self.stop_event = Event()
        self.log_queue = log_queue
        self.stop_event = Event()
        self.gc_process = Process(target=run_gc, args=(self.namespace, self.stop_event, self.log_queue))  # 시뮬레이터와 동시 수행을 위해 멀티프로세싱 사용

        self.pod_manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "experiment-pod"
            },
            "spec": {
                "containers": [
                    {
                        "name": "experiment-container",
                        "image": "harbor.cu.ac.kr/swlabpods/gcpod:latest",
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
                            },
                            {
                                "name": "NUM_PROCS",
                                "value": "1"
                            },
                            {
                                "name": "PROCESS_MIX",
                                "value": "single"
                            }
                        ]
                    }
                ]
            }
        }

    def _sample_interarrival_seconds(self, rate_per_min: float) -> float:
        # rate per min : 분당 평균 도착률 G, ex) 6 -> 분당 6개
        if rate_per_min <= 0:
            return 0.0
        rate_per_sec = rate_per_min / 60.0
        return random.expovariate(rate_per_sec)

    # 하나의 실험 주기에서 상태의 분포를 설정하고, 해상 상태분포에 따라 상태 리턴
    # state_mix = {"active": 0.4, "running": 0.35, "background": 0.15, "idle": 0.10}
    def _sample_state(self, state_mix: Dict[str, float]) -> str:
        states = list(state_mix.keys())
        weights = list(state_mix.values())
        # 정규화 안전장치
        total = sum(w for w in weights if w > 0)
        if total <= 0:
            states, weights = ["idle"], [1.0]
        else:
            weights = [w / total for w in weights]
        return random.choices(states, weights=weights, k=1)[0]

    def run_poisson(self,
                           duration_s: int = 180,  # 한 사이클 전체 실험 길이
                           generate_until_min: int = 2,  # 생성 구간: 앞 X분까지만 생성
                           rate_per_min: float = 3,  # 포아송 생성률(분당 G)
                           inactive_threshold_s: int = 60
                           ):

        # 생성 구간(초)
        generate_until_s = max(0, min(duration_s, int(generate_until_min * 60)))

        # GC 프로세스 시작 TODO: inactive_threashold 전달 필요
        self.gc_process.start()

        try:
            # ===== 전체 실험 반복 =====
            while self.count < self.times:
                created = 0
                cycle_start = time.time()
                creation_end_ts = cycle_start + generate_until_s
                base_cycle_end_ts = cycle_start + duration_s  # 기본 종료시각(1시간)
                last_creation_ts = None

                print(f"\n\n====== Start cycle #{self.count + 1}/{self.times} "
                      f"(total {duration_s}s, generate first {generate_until_s}s) ======\n")

                # 생성 구간
                next_arrival_ts = cycle_start + self._sample_interarrival_seconds(rate_per_min)
                while True:
                    # 중단 조건
                    if hasattr(self, "stop_event") and self.stop_event.is_set():
                        print("[POISSON] stop_event set. exit creation window.")
                        break

                    now = time.time()
                    if now >= creation_end_ts:
                        print("[POISSON] creation window ended.")
                        break

                    # 다음 도착까지 대기
                    sleep_for = max(0.0, next_arrival_ts - now)
                    if sleep_for > 0:
                        time.sleep(sleep_for)

                    # 파드 생성
                    try:
                        print(f"[POISSON] + createPod at {datetime.now():%H:%M:%S}")
                        total = random.randint(4, 6)
                        active, idle = self.generateRandomNumber(total, 2)
                        active, running, bg_active = self.generateRandomNumber(active, 3)
                        self.createPod(active, idle, running, bg_active)  # active, idle, running, background
                        created += 1
                    except Exception as e:
                        print(f"[POISSON] createPod error: {e}")

                    # 다음 도착 예약
                    next_arrival_ts = time.time() + self._sample_interarrival_seconds(rate_per_min)

                # 유지구간 종료시간 계산 - 마지막 생성이 없었다면 생성구간 종료시점을 기준으로 유지 임계값 적용
                anchor_ts = last_creation_ts if last_creation_ts is not None else creation_end_ts
                # 마지막 임계 시간+300초 더 추가함 5분더 시뮬레이터가 진행할 수 있도록, 300초는 없애도 되고 줄여도됨
                hold_until_ts = max(base_cycle_end_ts, anchor_ts + max(0, inactive_threshold_s))

                print(f"[POISSON] holding until "
                      f"{int(hold_until_ts - time.time())}s from now "
                      f"(base_end={int(base_cycle_end_ts - time.time())}s, "
                      f"last+thr={int(anchor_ts + inactive_threshold_s - time.time())}s).")

                # 유지 구간
                while True:
                    if hasattr(self, "stop_event") and self.stop_event.is_set():
                        print("[POISSON] stop_event set. exit hold window.")
                        break
                    if time.time() >= hold_until_ts:
                        print("[POISSON] cycle duration reached.")
                        break
                    time.sleep(0.2)

                # 삭제 단계
                print("[POISSON] Deleting all pods...")
                self.deletePod()
                while True:
                    if self.checkStatus():
                        break
                    print("Deleting pod ------")
                    time.sleep(1)

                # 사이클 종료
                self.count += 1
                print(f"[POISSON] Cycle #{self.count} finished. created_pods={created}")

        except KeyboardInterrupt:
            print("Keyboard Interrupted. Cleaning up...")
            self.deletePod()
        finally:
            if self.gc_process.is_alive():
                self.stop_event.set()
                self.gc_process.join()

    def run(self):
        """
        Run simulation
        """
        self.gc_process.start()
        try:
            while self.count < self.times:
                # 실험 시작 전 카운트 횟수 및 파드 수 초기화
                cnt = 0
                self.active = 0
                self.idle = 0
                self.running = 0
                self.bg_active = 0


                print(f"\n\n======Start {self.count+1}======\n")

                while cnt < 5:  # 120초마다 pod 생성, 총 10분동안 진행 (5회)
                    print(f"\n---create pod {cnt+1} times---")
                    self.createPod(1, 1, 1, 1)  # active, idle, running, background active
                    time.sleep(self.intervalTime)
                    cnt += 1

                # 실험 한 사이클 종료 후 pod 모두 삭제
                self.deletePod()
                while True:  # check delete pod status
                    if self.checkStatus():
                        break
                    print("Deleting pod ------")
                    time.sleep(1)
                self.count += 1

        except KeyboardInterrupt:
            print("Keyboard Interrupted. Cleaning up...")
            self.deletePod()
        finally:
            # simulator end(or except) and gc stop
            if self.gc_process.is_alive():
                self.stop_event.set()
                self.gc_process.join()

    def experimentDataCollection(self, interval=60, cnt=60, worker=10):
        i = 0
        while i < self.times:  # times = 10
            self.pod_list = {}
            self.count = 0

            active, idle = self.generateRandomNumber(100, 2)
            active, bg_active, running = self.generateRandomNumber(active, 3)
            try:
                self.createPod_atOnce(active, 'active', 'active')
                self.createPod_atOnce(bg_active, 'background', 'background_active')
                self.createPod_atOnce(running, 'running', 'running')
                self.createPod_atOnce(idle, 'idle', 'idle')

                self.getPodList()
                self.waitForPodRunning(self.pod_list)

                manager = {}
                start_anchor = time.perf_counter()  # 고정 기준시각
                while self.count < cnt:
                    print("\n\n")
                    print("=" * 50)
                    print(f"Start experiment {i}, {self.count} times")
                    print("=" * 50)

                    # 목표 시각 계산 (fixed-rate)
                    target_time = start_anchor + (self.count + 1) * interval

                    self.getPodList()

                    #시간 측정
                    start_ts = time.perf_counter()
                    now_wall = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    print(f"[TIMING] Collecting Pod Data... for {len(self.pod_list)} pods with {worker} workers "
                          f"at {now_wall} (perf_counter={start_ts:.3f}s)")

                    for p_name, p in self.pod_list.items():
                        if p_name not in manager:
                            #cfg = client.Configuration().get_default_copy()
                            #api_client = client.ApiClient(configuration=cfg)
                            core_api = client.CoreV1Api()
                            manager[p_name] = Pod(core_api, p)

                    futures = []
                    with ThreadPoolExecutor(max_workers=worker) as executor:
                        for p_name in self.pod_list.keys():
                            pod = manager[p_name]
                            futures.append(executor.submit(pod.getPodProcessStatus, i+1))

                        for fut in as_completed(futures):
                            try:
                                _ = fut.result()
                            except Exception as e:
                                print(f"[WARN] Fail to collect status for a pod: {e}")

                    elapsed = time.perf_counter() - start_ts
                    print(f"[TIMING] Collected statuses for {len(self.pod_list)} pods [{elapsed:.3f}s]")

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

                    self.count += 1
                self.deletePod()
                while True:  # check delete pod status
                    if self.checkStatus():
                        break
                    print("Deleting pod ------")
                    time.sleep(1)

            except KeyboardInterrupt:
                print("Keyboard Interrupted. Cleanning up...")

            finally:
                self.deletePod()

                # simulator end(or except) and gc stop
                if self.gc_process.is_alive():
                    self.stop_event.set()
                    self.gc_process.join()
            i += 1
        print("Finished generating")

    def createPod(self, ac, idle, run, bg):
        """
        Create pod
        """
        # active pod
        ac += self.active  # 현재 파드의 수 + 생성할 파드의 수
        while self.active < ac:
            self.pod_manifest['metadata']['name'] = 'active-'+str(self.active)
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'PROCESS_STATE')['value'] = 'active'
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'NUM_PROCS')['value'] = '1'

            self.coreV1.create_namespaced_pod(namespace=self.namespace, body=self.pod_manifest)
            print('active pod', self.active, ' created')
            self.active += 1

        # idle pod
        idle += self.idle
        while self.idle < idle:
            self.pod_manifest['metadata']['name'] = 'idle-' + str(self.idle)
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'PROCESS_STATE')['value'] = 'idle'
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'NUM_PROCS')['value'] = '1'

            self.coreV1.create_namespaced_pod(namespace=self.namespace, body=self.pod_manifest)
            print('idle pod', self.idle, ' created')
            self.idle += 1

        # running pod
        run += self.running
        while self.running < run:
            self.pod_manifest['metadata']['name'] = 'running-' + str(self.running)
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'PROCESS_STATE')['value'] = 'running'
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'NUM_PROCS')['value'] = '1'

            self.coreV1.create_namespaced_pod(namespace=self.namespace, body=self.pod_manifest)
            print('running pod', self.running, ' created')
            self.running += 1

        # background active pod
        bg += self.bg_active
        while self.bg_active < bg:
            self.pod_manifest['metadata']['name'] = 'background-ac-' + str(self.bg_active)
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'PROCESS_STATE')['value'] = 'background_active'
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'NUM_PROCS')['value'] = '1'

            self.coreV1.create_namespaced_pod(namespace=self.namespace, body=self.pod_manifest)
            print('background active pod', self.bg_active, ' created')
            self.bg_active += 1

    def createPod_atOnce(self, total, name, state, numProc=1, isMIX='single'):
        """
        create pod
        arg: total(생성 수), state(상태), numProc(프로세스 수), isMIX(single or mix)
        """
        count = 0
        while count < total:
            self.pod_manifest['metadata']['name'] = name+'-'+str(count)
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'PROCESS_STATE')['value'] = state
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'NUM_PROCS')['value'] = str(numProc)
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'PROCESS_MIX')['value'] = isMIX

            self.coreV1.create_namespaced_pod(namespace=self.namespace, body=self.pod_manifest)
            print(f"{state} pod {count} created")
            count += 1

    def generateRandomNumber(self, total, numCreate=2, active_ratio=0.9):
        """
        랜덤 숫자 생성 (비율에 맞게 생성)
        - numCreate == 2 : active:idle = 약 0.9 : 0.1
        - numCreate >= 3 : active, running, background, idle = 0.5, 0.25, 0.15, 0.10
        """
        if numCreate < 2:
            return total

        if numCreate == 2:
            a = int(total * active_ratio)
            b = total - a
            return a, b

        else:
            # # 비율 나누기 (0 ~ 1)
            # parts = [random.random() for _ in range(numCreate)]
            # s = sum(parts)
            # numbers = [int(total * p / s) for p in parts]
            #
            # # 합이랑 맞추기 위해 보정
            # diff = total - sum(numbers)
            # numbers[-1] += diff
            ratio_map = [0.5, 0.25, 0.25]  # active, running, background
            numbers = [int(total * r) for r in ratio_map]
            diff = total - sum(numbers)
            numbers[-1] += diff  # 보정
            return numbers

    def waitForPodRunning(self, pods, interval=5):
        pod_statuses = {p: "Pending" for p in pods}

        while True:
            all_running = True
            for pod_name in pods:
                pod = self.coreV1.read_namespaced_pod(pod_name, self.namespace)
                phase = pod.status.phase
                pod_statuses[pod_name] = phase
                print(f"[STATUS] Pod {pod_name} -> {phase}")

                if phase != "Running":
                    all_running = False

            if all_running:
                print("[READY] All pods are Running")
                break

            time.sleep(interval)

    def deletePod(self):
        """
        Delete all pod
        """
        self.getPodList()
        if not self.pod_list:
            return

        for p in self.pod_list:
            self.coreV1.delete_namespaced_pod(p, self.namespace)

    def getPodList(self):
        """
        Get pod list in namespace
        """
        pods = self.coreV1.list_namespaced_pod(self.namespace).items
        if not pods:
            self.pod_list = {}
            return

        for p in pods:
            p_name = p.metadata.name
            self.pod_list[p_name] = p

    def checkStatus(self):
        """
        Pod가 모두 삭제되었는지 확인하는 함수
        """
        pods = self.coreV1.list_namespaced_pod(self.namespace).items
        if not pods:  # No pod = clear to delete pods
            print("Delete all pods")
            return True
        else:
            return False


if __name__ == "__main__":
    #네임스페이스 값을 비워두면 'default'로 지정
    # generator = Generator()
    # generator = Generator(namespace='gc-10m')
    # generator = Generator(namespace='gc-15m')
    # generator = Generator(namespace='gc-20m')
    # generator = Generator(namespace='gc-25m')
    generator = Generator(namespace='gc-30m')
    #generator.experimentDataCollection()
    # generator.run()
    # generator.deletePod()
    # while True:
    #     if generator.checkStatus():
    #         break
    #     print("Deleting pod ------")
    #     time.sleep(1)
    # generator.createPod()
    generator.run_poisson()