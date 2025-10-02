import csv
from poddata import Pod_Info, Pod_Lifecycle, Reason_Deletion
from historyManager import HistoryManager
from processManager import ProcessManager
from DB_postgresql import (
    save_bash_history_result,
    save_pod_status,
    save_pod_lifecycle,
    save_to_process,
    get_last_bash_history,
    save_bash_history,
    save_delete_reason,
    is_deleted_in_DB, is_exist_in_DB
)

from datetime import datetime, timezone, timedelta
# pod.py 맨 위 import 근처에 추가
import threading
from collections import defaultdict
import os

# 파일별 락 (동일 프로세스/스레드 간 보호)
_FILE_LOCKS = defaultdict(threading.Lock)
# 헤더 썼는지 빠르게 체크하기 위한 집합(프로세스 내 캐시)
_HEADER_WRITTEN = set()
_HEADER_WRITTEN_LOCK = threading.Lock()

def _ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def _with_file_lock(fname: str):
    return _FILE_LOCKS[fname]

def _need_write_header_once(fname: str) -> bool:
    """
    프로세스 내에서 헤더를 한 번만 쓰게 하는 보조.
    파일이 없을 때만 헤더를 쓰되, 스레드 경쟁을 방지.
    """
    with _HEADER_WRITTEN_LOCK:
        if fname in _HEADER_WRITTEN:
            return False
        # 디스크에서도 확인 (첫 작성자만 True)
        need = not os.path.exists(fname)
        # 바로 표시해 중복 진입 방지
        _HEADER_WRITTEN.add(fname)
        return need

class Pod():
    def __init__(self, api, pod):
        self.api = api
        self.pod = pod
        self.podName = pod.metadata.name
        self.namespace = pod.metadata.namespace

        self.timeBashHistory = None
        self.processes = list()
        self.pod_status = None  # list -> obj
        self.pod_lifecycle = None  # list -> obj
        self.hm = HistoryManager(self.api, self.pod)
        self.pm = ProcessManager(self.api, self.pod)

        # 분석 결과 (커맨드 히스토리, 프로세스)
        self.isActiveResultCommandHistory: bool = None
        self.isActiveResultProcess: bool = None
        self.processStateDescription: str = None

        # Pod 클래스 내부(클래스 변수)로 두면 좋습니다.
        self.PROCESS_HEADERS = [
            "pod_name", "timestamp", "pid", "comm", "state", "ppid", "pgrp", "session", "tty_nr", "tpgid", "flags",
            "minflt", "cminflt", "majflt", "cmajflt", "utime", "stime", "cutime", "cstime", "priority", "nice",
            "num_threads", "itrealvalue", "starttime", "vsize", "rss", "rsslim", "startcode", "endcode",
            "startstack", "kstkesp", "kstkeip", "signal", "blocked", "sigignore", "sigcatch", "wchan", "nswap",
            "cnswap", "exit_signal", "processor", "rt_priority", "policy", "delayacct_blkio_ticks", "guest_time",
            "cguest_time", "start_data", "end_data", "start_brk", "arg_start", "arg_end", "env_start", "env_end",
            "exit_code", "voluntary_ctxt_switches", "nonvoluntary_ctxt_switches", "vm_rss_status",
            "read_bytes", "write_bytes"
        ]

        self.CGROUP_HEADERS = [
            "pod_name", "timestamp", "memory_current", "memory_limit", "io_read_bytes", "io_write_bytes"
        ]

        self.CLASSIFICATION_KEYS_ORDER = [
            "pod_name", "timestamp", "pid", "comm", "role", "state", "score", "reason"
        ]
        self.SUMMARY_KEYS_ORDER = [
            "pod_name", "timestamp", "total", "active_cnt", "inactive_cnt", "gc_candidates_cnt", "zombie_cnt"
        ]

    def getTimestamp(self):
        """시간대를 UTC로 통일"""
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    def initPodData(self):
        """새로운 pod가 만들어지면, 초기 데이터 저장"""
        self.insertPodLifecycle()
        self.insertPodInfo()
        self.savePodLiftcycleToDB()
        self.savePodInfoToDB()


    def isDeletedInDB(self):
        """Pod이 삭제되었는지 DB에서 확인"""
        return is_deleted_in_DB(self.podName, self.namespace)

    def isExistInDB(self):
        """Pod이 DB에 존재하는지 확인"""
        return is_exist_in_DB(self.podName, self.namespace)

    def insertPodInfo(self):
        """pod's status save"""
        p = Pod_Info

        p.uid = self.pod.metadata.uid
        # p.labels = self.pod.metadata.labels
        # p.annotations = self.pod.metadata.annotations
        p.creation_timestamp = self.pod.metadata.creation_timestamp
        p.deletion_timestamp = self.pod.metadata.deletion_timestamp
        p.generate_name = self.pod.metadata.generate_name
        # p.owner_references = self.pod.metadata.owner_references
        # p.finalizers = self.pod.metadata.finalizers
        # p.managed_fields = self.pod.metadata.managed_fields

        # p.volumes = self.pod.spec.volumes
        # p.containers = self.pod.spec.containers
        p.node_name = self.pod.spec.node_name

        p.phase = self.pod.status.phase
        # p.conditions = self.pod.status.conditions
        p.hostIP = self.pod.status.host_ip
        p.podIP = self.pod.status.pod_ip
        p.startTime = self.pod.status.start_time

        self.pod_status = p

    def savePodInfoToDB(self):
        """pod's status save to DB"""
        save_pod_status(self.podName, self.namespace, self.pod_status)

    def insertPodLifecycle(self):
        """Save pod's created time"""
        pl = Pod_Lifecycle()
        pl.createTime = self.pod.metadata.creation_timestamp
        self.pod_lifecycle = pl

    def savePodLiftcycleToDB(self):
        """Pod's lifecycle save to DB"""
        save_pod_lifecycle(self.podName, self.namespace, self.pod_lifecycle)

    def insert_DeleteReason(self, reason):
        if self.pod_lifecycle is None:
            self.pod_lifecycle = Pod_Lifecycle()
        """When pod deleted, save time and because of pod deleted"""
        self.pod_lifecycle.reason_deletion = reason
        self.pod_lifecycle.deleteTime = self.getTimestamp()

    def save_DeleteReason_to_DB(self):
        """Delete time and reason save to DB"""
        save_delete_reason(self.podName, self.namespace, self.pod_lifecycle)

    def collectProcessAndHistory(self):
        """
        Collect process status and bash history info
        """
        self.timeBashHistory = self.hm.getLastUseTime()
        self.processes = self.pm.getPorcessData()
        # self.processes = processData['processes']
        # cgroups = processData['cgroups']

    def isActiveFromHistory(self) -> bool:
        """
        retrn True
        """
        result = self.hm.analyze(self.timeBashHistory)
        print(result)

        # pod_lifecycle에 리스토리 검사 결과 저장
        save_bash_history_result(self.podName, self.namespace, result)

        if self.timeBashHistory is not None:
            lastTimeStamp_Bash_history = self.hm.checkTimestamp(self.timeBashHistory)
            self.saveCommandHistoryToDB(lastTimeStamp_Bash_history)

        # 7일이상 사용하지 않으면 false 반환
        if not result and self.checkCreateTime():
            self.isActiveResultCommandHistory = False
        else:
            self.isActiveResultCommandHistory = True

        return self.isActiveResultCommandHistory

    def saveCommandHistoryToDB(self, last_modified_time):
        """Save bash history data to DB"""
        if last_modified_time is None:
            print(f"No bash_history found for pod: {self.podName}")
            return

        last_saved = get_last_bash_history(self.podName)

        if last_saved is None or str(last_saved).strip() != str(last_modified_time).strip():
            print(f"New bash_history detected for pod: {self.podName}, saving to DB.")
            save_bash_history(self.podName, self.namespace, last_modified_time)
        else:
            print(f"No changes in bash_history for pod: {self.podName}, skipping DB save.")

    def checkCreateTime(self):
        """
        pod 생성된 지 7일이 지났는지 확인 후 반환
        """
        creation_time = self.pod_status.creation_timestamp
        now = datetime.now(creation_time.tzinfo)
        result = (now - creation_time) > timedelta(days=7)
        return result

    def isActiveFromProcess(self, experiment_id=0):
        self.isActiveResultProcess, self.processStateDescription, classification, summary = self.pm.analyzePodProcess(self.processes)

        timestamp = self.getTimestamp()
        self.saveStatDataToCSV(timestamp, experiment_id)
        # self.saveCgroupMetricsToCSV(cgroups, timestamp, experiment_id)
        self.saveClassificationToCsv(classification, self.podName, experiment_id)
        self.saveSummaryToCsv(summary, self.podName, experiment_id)
        return self.isActiveResultProcess

    def printProcList(self):
        print('-'*50)
        for p in self.processes:
            print(p.comm, p.state, p.pid, p.ppid, p.policy)
        print('-' * 50)

    def saveStatDataToCSV(self, timestamp, experiment_id=None):
        """
        Save process data in csv file
        """
        log_dir = os.path.join(os.getcwd(), "data")
        _ensure_dir(log_dir)

        file_name = os.path.join(log_dir, f"process_metrics_experiment{experiment_id}.csv")
        lock = _with_file_lock(file_name)

        # 한 번의 atomic 구간으로 헤더/레코드 쓰기
        with lock, open(file_name, mode="a", newline="", encoding="utf-8") as file:
            # 헤더는 파일 최초 생성자만 기록
            if _need_write_header_once(file_name):
                file.write(",".join(self.PROCESS_HEADERS) + "\n")

            for process in self.processes:
                stat_values = [
                    self.podName, timestamp,
                    str(process.pid), process.comm, process.state, str(process.ppid),
                    str(process.pgrp), str(process.session), str(process.tty_nr),
                    str(process.tpgid), str(process.flags), str(process.minflt),
                    str(process.cminflt), str(process.majflt), str(process.cmajflt),
                    str(process.utime), str(process.stime), str(process.cutime),
                    str(process.cstime), str(process.priority), str(process.nice),
                    str(process.num_threads), str(process.itrealvalue),
                    str(process.starttime), str(process.vsize), str(process.rss),
                    str(process.rsslim), str(process.startcode), str(process.endcode),
                    str(process.startstack), str(process.kstkesp), str(process.kstkeip),
                    str(process.signal), str(process.blocked), str(process.sigignore),
                    str(process.sigcatch), str(process.wchan), str(process.nswap),
                    str(process.cnswap), str(process.exit_signal), str(process.processor),
                    str(process.rt_priority), str(process.policy), str(process.delayacct_blkio_ticks),
                    str(process.guest_time), str(process.cguest_time), str(process.start_data),
                    str(process.end_data), str(process.start_brk), str(process.arg_start),
                    str(process.arg_end), str(process.env_start), str(process.env_end),
                    str(process.exit_code)
                ]

                if process.metrics:
                    metrics_values = [
                        str(process.metrics.voluntary_ctxt_switches or ""),
                        str(process.metrics.nonvoluntary_ctxt_switches or ""),
                        str(process.metrics.vm_rss or ""),
                        str(process.metrics.read_bytes or ""),
                        str(process.metrics.write_bytes or "")
                    ]
                else:
                    metrics_values = ["", "", "", "", ""]

                file.write(",".join([*stat_values, *metrics_values]) + "\n")

    def saveCgroupMetricsToCSV(self, cgroup, timestamp, experiment_id=None):
        """
        Save cgroup metrics (memory, I/O) into CSV file
        """
        log_dir = os.path.join(os.getcwd(), "data")
        _ensure_dir(log_dir)

        file_name = os.path.join(log_dir, f"cgroup_experiment{experiment_id}.csv")
        lock = _with_file_lock(file_name)

        with lock, open(file_name, mode="a", newline="", encoding="utf-8") as file:
            if _need_write_header_once(file_name):
                file.write(",".join(self.CGROUP_HEADERS) + "\n")

            row = [
                self.podName, timestamp,
                str(cgroup.memory_current or ""),
                str(cgroup.memory_limit or ""),
                str(cgroup.io_read_bytes or ""),
                str(cgroup.io_write_bytes or "")
            ]
            file.write(",".join(row) + "\n")

    def saveProcessDataToDB(self):
        """Save Pod's process data to DB"""
        timestamp = self.getTimestamp()
        processes = []

        for process in self.processes:
            processes.append({
                "timestamp": timestamp,
                "pid": process.pid,
                "comm": process.comm,
                "state": process.state,
                "ppid": process.ppid,
                "pgrp": process.pgrp,
                "session": process.session,
                "tty_nr": process.tty_nr,
                "tpgid": process.tpgid,
                "flags": process.flags,
                "minflt": process.minflt,
                "cminflt": process.cminflt,
                "majflt": process.majflt,
                "cmajflt": process.cmajflt,
                "utime": process.utime,
                "stime": process.stime,
                "cutime": process.cutime,
                "cstime": process.cstime,
                "priority": process.priority,
                "nice": process.nice,
                "num_threads": process.num_threads,
                "itrealvalue": process.itrealvalue,
                "starttime": process.starttime,
                "vsize": process.vsize,
                "rss": process.rss,
                "rsslim": process.rsslim,
                "startcode": process.startcode,
                "endcode": process.endcode,
                "startstack": process.startstack,
                "kstkesp": process.kstkesp,
                "kstkeip": process.kstkeip,
                "signal": process.signal,
                "blocked": process.blocked,
                "sigignore": process.sigignore,
                "sigcatch": process.sigcatch,
                "wchan": process.wchan,
                "nswap": process.nswap,
                "cnswap": process.cnswap,
                "exit_signal": process.exit_signal,
                "processor": process.processor,
                "rt_priority": process.rt_priority,
                "policy": process.policy,
                "delayacct_blkio_ticks": process.delayacct_blkio_ticks,
                "guest_time": process.guest_time,
                "cguest_time": process.cguest_time,
                "start_data": process.start_data,
                "end_data": process.end_data,
                "start_brk": process.start_brk,
                "arg_start": process.arg_start,
                "arg_end": process.arg_end,
                "env_start": process.env_start,
                "env_end": process.env_end,
                "exit_code": process.exit_code
            })

        save_to_process(self.podName, self.namespace, processes)

    def saveClassificationToCsv(self, classification, pod_name, experiment_id=None):
        """
        분류한 딕셔너리와 분류 결과 요약한 딕셔너리를 csv로 저장
        classification: 프로세스별 분석 결과
        summary: 프로세스 분석 결과 요약 (active, idle 등 분류 결과를 요약)
        """
        if not classification:
            return

        log_dir = os.path.join(os.getcwd(), "data")
        _ensure_dir(log_dir)

        filename = os.path.join(log_dir, f"process_classification_experiment{experiment_id}.csv")
        lock = _with_file_lock(filename)

        ts = self.getTimestamp()

        # 스키마 고정: 필요한 키만 뽑고, 없으면 빈칸
        def _row_from(proc: dict):
            base = {
                "pod_name": pod_name,
                "timestamp": ts
            }
            base.update(proc)  # 기존 키를 넣되, 최종 출력은 고정 순서대로
            return [str(base.get(k, "")) for k in self.CLASSIFICATION_KEYS_ORDER]

        with lock, open(filename, "a", newline="", encoding="utf-8") as f:
            if _need_write_header_once(filename):
                f.write(",".join(self.CLASSIFICATION_KEYS_ORDER) + "\n")
            for proc in classification:
                f.write(",".join(_row_from(proc)) + "\n")

    def saveSummaryToCsv(self, summary, pod_name, experiment_id=None):
        """
        모든 파드 summary 결과를 하나의 CSV에 누적 저장
        """
        if not summary:
            return

        log_dir = os.path.join(os.getcwd(), "data")
        _ensure_dir(log_dir)

        filename = os.path.join(log_dir, f"process_summary_experiment{experiment_id}.csv")
        lock = _with_file_lock(filename)

        ts = self.getTimestamp()
        # 고정 키 순서에 맞춰 값 매핑
        base = {"pod_name": pod_name, "timestamp": ts}
        base.update(summary)

        with lock, open(filename, "a", newline="", encoding="utf-8") as f:
            if _need_write_header_once(filename):
                f.write(",".join(self.SUMMARY_KEYS_ORDER) + "\n")
            row = [str(base.get(k, "")) for k in self.SUMMARY_KEYS_ORDER]
            f.write(",".join(row) + "\n")

    def shouldGarbageCollection(self):
        """
        pod가 가비지 컬렉션에 의해 삭제되어야 하는지 판단
        프로세스 분석 결과와 명령어 히스토리 결과를 확인

        return:
            - GC 여부: bool
            - 이유: str
            - 종류(hisotry or process): str
        """
        # 1. 명령어 히스토리 기반 분석
        self.isActiveFromHistory()

        # 2. 먼저 프로세스 기반 분석
        self.isActiveFromProcess()

        # 3. 판단
        if not self.isActiveResultCommandHistory:
            return True, 'No usage history for more than a week', 'history'
        if self.isActiveResultProcess:
            return True, self.processStateDescription, 'process'
        else:
            return False, 'active', None
