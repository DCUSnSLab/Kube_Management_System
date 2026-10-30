from concurrent.futures import ThreadPoolExecutor, as_completed

from kubernetes import client, config
from pod import Pod
from DB_postgresql import initialize_database
from resourceCollector import ResourceCollector

from datetime import datetime, timezone, timedelta
import os
import time
from multiprocessing import Event

TARGET_NAMESPACE_LABEL = os.getenv("GC_TARGET_LABEL", "kms-gc/enabled=true")
ANNOTATION_POD_SELECTOR = "kms-gc/pod-selector"
ANNOTATION_PROTECT_LABELS = "kms-gc/protect-labels"
DEFAULT_EXCLUDE = ["ssh-wldnjs269", "ssh-marsberry", "ssh-min8180", "swlabssh"]


def load_kube_config():
    try:
        config.load_incluster_config()
    except config.ConfigException:
        config.load_kube_config()


def parsePolicy(annotations):
    """네임스페이스 어노테이션 → (파드 라벨 셀렉터, 보호 라벨 키 튜플)"""
    annotations = annotations or {}
    selector = (annotations.get(ANNOTATION_POD_SELECTOR) or "").strip() or None
    protect = tuple(x.strip() for x in (annotations.get(ANNOTATION_PROTECT_LABELS) or "").split(",")
                    if x.strip())
    return selector, protect


class NamespaceGC():
    """네임스페이스 하나의 파드 추적·수집·판정·삭제 상태"""

    def __init__(self, namespace, inactive_threshold_s, delete_enabled,
                 label_selector=None, protect_labels=(), exclude=None):
        self.v1 = client.CoreV1Api()
        self.namespace: str = namespace
        self.exclude: list = list(exclude if exclude is not None else DEFAULT_EXCLUDE)
        self.labelSelector = label_selector
        self.protectLabels = tuple(protect_labels)
        self.podlist: dict = {}
        self.notRunningPods: dict = {}
        self.protectedPods: dict = {}
        self.Inactive_Threshold_s = inactive_threshold_s
        self.deleteEnabled = delete_enabled
        self.resourceCollector = ResourceCollector(self.namespace, self.exclude, self.labelSelector)

    def setPolicy(self, label_selector, protect_labels):
        """네임스페이스 어노테이션 변경을 반영 (파드 추적 상태는 유지)"""
        self.labelSelector = label_selector
        self.protectLabels = tuple(protect_labels)
        self.resourceCollector.label_selector = label_selector

    def log(self, msg):
        print(f"[{self.namespace}] {msg}")

    def runCycle(self, cycle_no, worker=10):
        """파드 목록 갱신 → 수집 → 저장 → 판정 → 삭제를 1회 수행"""
        self.getPodList()
        self.log('=' * 10 + f"Start to Check Data {cycle_no} times" + '=' * 10)

        start_ts = time.perf_counter()
        self.log(f"[TIMING] Collecting Pod Data... for {len(self.podlist)} pods with {worker} workers")

        futures = []
        with ThreadPoolExecutor(max_workers=worker) as executor:
            for pod in self.podlist.values():
                futures.append(executor.submit(pod.collectProcessAndHistory))
            for fut in as_completed(futures):
                try:
                    _ = fut.result()
                except Exception as e:
                    self.log(f"[WARN] Fail to collect status for a pod: {e}")
        elapsed = time.perf_counter() - start_ts
        self.log(f"[TIMING] Collected statuses for {len(self.podlist)} pods [{elapsed:.3f}s]")

        # 1단계: 파드 정보 저장 (모든 파드가 아직 살아있는 상태)
        for p_obj in self.podlist.values():
            p_obj.insertPodInfo()
            p_obj.saveProcessDataToDB()

        # 2단계: 리소스 사용량(CPU/Memory) 수집 및 DB 저장
        # GC 삭제 전에 수집하여 삭제 대상 파드의 마지막 사용량도 보존
        self.resourceCollector.collect_and_save()

        # 3단계: GC 판단 및 삭제
        decisions = []
        with ThreadPoolExecutor(max_workers=worker) as executor:
            decide_futures = {
                executor.submit(p_obj.shouldGarbageCollection): (p_name, p_obj)
                for p_name, p_obj in self.podlist.items()
            }
            for fut in as_completed(decide_futures):
                p_name, p_obj = decide_futures[fut]
                try:
                    should_gc, gc_reason, cause = fut.result()
                    decisions.append((p_name, p_obj, should_gc, gc_reason, cause))
                except Exception as e:
                    self.log(f"[WARN] Fail to analyze pod {p_name}: {e}")

        exec_fail_deletes = sum(1 for d in decisions if d[2] and d[4] == 'exec_failure')
        breaker = (exec_fail_deletes >= 3 and exec_fail_deletes > len(self.podlist) // 2)
        if breaker:
            self.log(f"[CIRCUIT-BREAKER] {exec_fail_deletes}/{len(self.podlist)} pods "
                     f"hit exec_failure; suspending exec_failure deletions this cycle "
                     f"(possible cluster-wide issue)")

        for p_name, p_obj, should_gc, gc_reason, cause in decisions:
            if not should_gc:
                continue
            if cause == 'exec_failure' and breaker:
                self.log(f"[CIRCUIT-BREAKER] skip delete: {p_name}")
                p_obj.saveGcDecisionToDB(cause, gc_reason, self.deleteEnabled, False, "circuit_breaker")
                continue
            if not self.deleteEnabled:
                self.log(f"[DRY-RUN] Pod '{p_name}' would be deleted ({cause}): {gc_reason}")
                p_obj.saveGcDecisionToDB(cause, gc_reason, False, False, "dry_run")
                continue
            self.log(f"[Garbage Collector] Pod '{p_name}' will be deleted")
            self.log(f"  Reason: {gc_reason}")
            self.log(f"  Type: {cause}")
            try:
                if not self.deletePod(p_name, p_obj.pod.metadata.uid):
                    p_obj.saveGcDecisionToDB(cause, gc_reason, True, False, "protected_or_replaced")
                    continue
                p_obj.insert_DeleteReason(gc_reason)
                p_obj.save_DeleteReason_to_DB()
                p_obj.saveGcDecisionToDB(cause, gc_reason, True, True, "")
            except Exception as e:
                self.log(f"[ERROR] {p_name}: {e}")
                p_obj.saveGcDecisionToDB(cause, gc_reason, True, False, str(e)[:200])

    def isProtected(self, p):
        """보호 라벨(예: 시험 중 lablab.exam)이 붙은 파드인지 확인"""
        labels = p.metadata.labels or {}
        return any(key in labels for key in self.protectLabels)

    def getPodList(self):
        kwargs = {"label_selector": self.labelSelector} if self.labelSelector else {}
        pods = self.v1.list_namespaced_pod(self.namespace, **kwargs).items
        if not pods:
            self.log("No resources found.")
            self.recordDeletedPod(set(self.podlist) | set(self.notRunningPods) | set(self.protectedPods))
            self.podlist = {}
            self.notRunningPods = {}
            self.protectedPods = {}
            return

        filtering_pods = [
            pod for pod in pods
            if not any(
                pod.metadata.name == name or pod.metadata.name.startswith(name)
                for name in self.exclude
            )
        ]
        new_podlist = {}
        new_not_running = {}
        new_protected = {}
        for p in filtering_pods:
            pod_name = p.metadata.name
            pod_status = p.status.phase
            existing = (self.podlist.get(pod_name) or self.notRunningPods.get(pod_name)
                        or self.protectedPods.get(pod_name))
            if existing is not None and existing.pod.metadata.uid != p.metadata.uid:
                existing = None
            if existing is not None:
                existing.pod = p
                pod_obj = existing
            else:
                pod_obj = Pod(self.v1, p, self.Inactive_Threshold_s)
                if not pod_obj.isExistInDB() or pod_obj.isDeletedInDB():
                    self.log(f"Initializing new pod: {pod_name}")
                    pod_obj.initPodData()

            if self.isProtected(p):
                matched = [k for k in self.protectLabels if k in (p.metadata.labels or {})]
                self.log(f"[PROTECTED] {pod_name} ({','.join(matched)})")
                pod_obj.podInactiveSince.clear()
                pod_obj.execFailCount = 0
                new_protected[pod_name] = pod_obj
            elif pod_status == "Running":
                new_podlist[pod_name] = pod_obj
            else:
                self.log(f"[NOT-RUNNING] {pod_name} phase={pod_status}{self.describePodState(p)}")
                new_not_running[pod_name] = pod_obj

        known = set(self.podlist) | set(self.notRunningPods) | set(self.protectedPods)
        still_present = set(new_podlist) | set(new_not_running) | set(new_protected)
        self.recordDeletedPod(known - still_present)

        self.podlist = new_podlist
        self.notRunningPods = new_not_running
        self.protectedPods = new_protected

    def describePodState(self, p):
        """컨테이너 waiting/terminated 사유를 로그용 문자열로 반환"""
        reasons = []
        for cs in (p.status.container_statuses or []):
            st = cs.state
            if st and st.waiting and st.waiting.reason:
                reasons.append(f"{cs.name}:{st.waiting.reason}")
            elif st and st.terminated and st.terminated.reason:
                reasons.append(f"{cs.name}:{st.terminated.reason}")
        return f" ({', '.join(reasons)})" if reasons else ""

    def recordDeletedPod(self, removed_pods):
        """GC 밖에서 사라진 파드를 UNKNOWN 사유로 기록"""
        for rm_p in removed_pods:
            pod_obj = (self.podlist.get(rm_p) or self.notRunningPods.get(rm_p)
                       or self.protectedPods.get(rm_p))
            if pod_obj is None:
                continue
            if not pod_obj.isDeletedInDB():
                last_phase = getattr(pod_obj.pod.status, "phase", None)
                pod_obj.insert_DeleteReason(f"UNKNOWN - Pod deleted (last_phase={last_phase})")
                pod_obj.save_DeleteReason_to_DB()
            self.log(f"Pod removed: {rm_p}")

    def deletePod(self, p_name, expected_uid=None):
        """삭제 직전 재조회해 보호 라벨이 붙었거나 다른 파드로 교체됐으면 삭제하지 않음"""
        current = self.v1.read_namespaced_pod(p_name, self.namespace)
        if self.isProtected(current):
            self.log(f"[PROTECTED] skip delete: {p_name}")
            return False
        if expected_uid is not None and current.metadata.uid != expected_uid:
            self.log(f"[REPLACED] skip delete: {p_name} (uid changed)")
            return False
        self.log(f"{p_name} ______REMOVE____")
        self.v1.delete_namespaced_pod(p_name, self.namespace)
        return True


class GarbageCollector():
    """
    대상 네임스페이스를 사이클마다 찾아 병렬로 GC 수행
        대상 선정: kms-gc/enabled=true 라벨 (GC_NAMESPACES 환경변수가 있으면 그 목록 사용)
        네임스페이스 어노테이션:
            kms-gc/pod-selector   : 대상 파드 라벨 셀렉터 (예: lablab.role=workspace)
            kms-gc/protect-labels : 이 라벨 키가 붙은 파드는 판정·삭제 제외 (쉼표 구분, 예: lablab.exam)
    """

    def __init__(self, namespaces=None, stop_event=None, Inactive_Threshold_s=None,
                 namespace_parallelism=None):
        load_kube_config()
        self.v1 = client.CoreV1Api()
        self.staticNamespaces = list(namespaces or [])
        self.Inactive_Threshold_s = Inactive_Threshold_s
        self._stop_event = stop_event or Event()
        self.targets: dict = {}
        self.count = 1
        self.deleteEnabled = os.getenv("GC_DELETE", "true").lower() in ("1", "true", "yes")
        self.namespaceParallelism = namespace_parallelism or int(os.getenv("GC_NAMESPACE_PARALLELISM", "4"))

    def discoverTargets(self):
        """대상 네임스페이스와 어노테이션 조회"""
        if self.staticNamespaces:
            targets = {}
            for ns in self.staticNamespaces:
                try:
                    targets[ns] = self.v1.read_namespace(ns).metadata.annotations or {}
                except Exception as e:
                    print(f"[TARGET][WARN] cannot read namespace {ns}: {e}")
                    targets[ns] = {}
            return targets
        namespaces = self.v1.list_namespace(label_selector=TARGET_NAMESPACE_LABEL).items
        return {ns.metadata.name: (ns.metadata.annotations or {}) for ns in namespaces}

    def syncTargets(self, discovered):
        for ns, annotations in discovered.items():
            selector, protect = parsePolicy(annotations)
            target = self.targets.get(ns)
            if target is None:
                print(f"[TARGET] add {ns} (pod-selector={selector}, protect-labels={','.join(protect) or '-'})")
                self.targets[ns] = NamespaceGC(ns, self.Inactive_Threshold_s, self.deleteEnabled,
                                               label_selector=selector, protect_labels=protect)
            elif (target.labelSelector, target.protectLabels) != (selector, protect):
                print(f"[TARGET] update {ns} (pod-selector={selector}, protect-labels={','.join(protect) or '-'})")
                target.setPolicy(selector, protect)
        for ns in list(self.targets):
            if ns not in discovered:
                print(f"[TARGET] remove {ns}")
                del self.targets[ns]

    def runCycle(self, worker=10):
        try:
            self.syncTargets(self.discoverTargets())
        except Exception as e:
            print(f"[TARGET][ERROR] failed to discover target namespaces, keeping previous targets: {e}")
        if not self.targets:
            print(f"[TARGET] no target namespaces (label {TARGET_NAMESPACE_LABEL})")
            return

        cycle_no = self.count
        with ThreadPoolExecutor(max_workers=min(self.namespaceParallelism, len(self.targets))) as executor:
            futures = {executor.submit(t.runCycle, cycle_no, worker): ns for ns, t in self.targets.items()}
            for fut in as_completed(futures):
                ns = futures[fut]
                try:
                    fut.result()
                except Exception as e:
                    print(f"[NAMESPACE-ERROR] {ns}: {e}")
        self.count += 1

    def manage(self, interval=60, worker=10):
        print(f"[GC] delete mode: {'ENABLED' if self.deleteEnabled else 'DRY-RUN (observe only)'}")
        source = f"GC_NAMESPACES={','.join(self.staticNamespaces)}" if self.staticNamespaces \
            else f"label {TARGET_NAMESPACE_LABEL}"
        print(f"[GC] target namespaces from {source}, parallelism={self.namespaceParallelism}")
        start_anchor = time.perf_counter()
        try:
            while True:
                target_time = start_anchor + (self.count * interval)
                print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Cycle {self.count} start")

                self.runCycle(worker)

                if self._stop_event.is_set():
                    break

                sleep_time = target_time - time.perf_counter()
                if sleep_time > 0:
                    wakeup_wall = datetime.now(timezone.utc) + timedelta(seconds=sleep_time)
                    print(f"[SLEEP] Sleeping {sleep_time:.2f}s "
                          f"(until {wakeup_wall.strftime('%Y-%m-%d %H:%M:%S %Z')})")
                    time.sleep(sleep_time)
                else:
                    print(f"[DRIFT] Overran by {-sleep_time:.3f}s; skipping sleep to realign")

        except KeyboardInterrupt:
            print("\n[INFO] Garbage Collector interrupted by user. Shutting down gracefully...")
        finally:
            print("Garbage Collector Stopped")


if __name__ == "__main__":
    initialize_database()

    static = [ns.strip() for ns in os.getenv("GC_NAMESPACES", "").split(",") if ns.strip()]
    gc = GarbageCollector(namespaces=static, Inactive_Threshold_s=20 * 60)
    gc.manage()
