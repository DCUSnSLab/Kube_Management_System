from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional
from process import CgroupMetrics, ProcessMetrics, Process, Mode_State, Policy_State
from podexec import pod_exec, ExecStatus, DEFAULT_EXEC_TIMEOUT

from kubernetes import client, config, stream
import time


@dataclass
class ProcessCollection:
    collected: bool
    processes: List = field(default_factory=list)
    exec_status: str = ""
    detail: str = ""


class ProcessStateClassification(Enum):
    """프로세스 상태 분류"""
    ACTIVE = "active"  # 활성 프로세스
    INACTIVE = "inactive"  # 비활성 프로세스


class ProcessStatePolicy:
    """프로세스 상태 분류 기준"""
    # State 기반 분류 기준
    ACTIVE_STATES: dict = {'Running', 'Uninterruptible Sleep'}
    IDLE_STATES: dict = {'Sleeping', 'Stopped'}
    INACTIVE_STATES: dict = {'Zombie', 'Dead'}
    # CPU 자원 관련 지표(변화률 기반)
    CPU_TIME_DELTA_THRESHOLD = 215  # CPU time(stime + utime) 임계치, jiffies(틱) 단위
    VOLUNTARY_CTXT_SWITCH_DELTA_THRESHOLD = 716  # Voluntary Context Switch 임계치
    NON_VOLUNTARY_CTXT_SWITCH_DELTA_THRESHOLD = 158  # Non-Voluntary Context Switch 임계치


_PROC_STAT_COMMAND = [
    "sh", "-c",
    "SELF_PID=$$ && "
    "for stat in /proc/[0-9]*/stat; do "
    "  if [ -r \"$stat\" ]; then "
    "    PID=$(basename $(dirname \"$stat\")) && "
    "    if [ \"$PID\" != \"$SELF_PID\" ] && [ \"$PID\" != 1 ]; then "
    "      STAT_LINE=$(cat \"$stat\" 2>/dev/null) && "
    "      COMM=$(echo \"$STAT_LINE\" | awk '{print $2}') && "
    "      PPID=$(echo \"$STAT_LINE\" | awk '{print $4}') && "
    "      if ! ([ \"$PPID\" = \"1\" ] && [ \"$COMM\" = \"(sleep)\" ]); then "
    "        echo \"$STAT_LINE\"; "
    "      fi; "
    "    fi; "
    "  fi; "
    "done"
]


class ProcessManager:
    def __init__(self, api_instance, pod, exec_timeout=DEFAULT_EXEC_TIMEOUT):
        self.v1 = api_instance
        self.pod = pod
        self.namespace: str = pod.metadata.namespace
        self.exec_timeout = exec_timeout

        self.previous_states: dict = {}  # pod별 이전 통계 저장하는 딕셔너리
        self.podInactiveSince: Dict[str, float] = {}  # pod 비활성 시작 시간 저장 (name, time)
        self.time = time

    def collect(self) -> ProcessCollection:
        """
        프로세스 정보를 수집하고 exec 실패 원인을 구분해 반환
            collected=True  : 수집 성공 (processes가 빈 리스트면 진짜 프로세스 0개)
            collected=False : exec 실패 (exec_status에 원인 분류)
        """
        r = pod_exec(self.v1, self.pod.metadata.name, self.namespace,
                     _PROC_STAT_COMMAND, timeout=self.exec_timeout)

        if not r.reachable:
            print(f"[COLLECT-FAIL] {self.pod.metadata.name} proc: "
                  f"{r.status.value} ({(r.error or r.stderr)[:120]})")
            return ProcessCollection(collected=False, exec_status=r.status.value,
                                     detail=(r.error or r.stderr)[:200])

        if r.status is ExecStatus.COMMAND_FAILED:
            print(f"[COLLECT-FAIL] {self.pod.metadata.name} proc: "
                  f"partial_toolchain ({r.stderr.strip()[:120]})")
            return ProcessCollection(collected=False, exec_status="partial_toolchain",
                                     detail=r.stderr.strip()[:200])

        processes = self.insertProcessStatData(r.stdout)
        return ProcessCollection(collected=True, processes=processes)

    def getPorcessData(self):
        """하위호환: 수집 성공 시 프로세스 리스트, 실패 시 None"""
        c = self.collect()
        return c.processes if c.collected else None

    def getCmdlineInPod(self, pid):
        """
        풀 커맨드(cmdline)를 얻으려면 Pod 안의 /proc/[pid]/cmdline을 읽어야함
        """
        r = pod_exec(self.v1, self.pod.metadata.name, self.namespace,
                     ["cat", f"/proc/{pid}/cmdline"], timeout=self.exec_timeout)
        if not r.ok:
            return ""
        return r.stdout.replace("\x00", " ").strip()

    def insertProcessStatData(self, processStat) -> list[Process]:
        """get /proc/stat data amd split into 52"""
        processes = []
        if processStat is None:
            print(f"Skipping Pod '{self.pod.metadata.name}': Failed to retrieve process data.")
            return []

        for line in processStat.splitlines():
            fields = line.split()
            if len(fields) < 52:
                continue

            try:
                p = self._parseStatFields(fields)
            except (ValueError, IndexError, KeyError) as e:
                print(f"Skipping invalid stat line ({type(e).__name__}: {e}): {line[:120]}")
                continue

            # memory, context switch, i/o data
            self.getProcessMetrics(p)

            processes.append(p)

        return processes

    def _parseStatFields(self, fields) -> Process:
        """/proc/[pid]/stat 필드를 Process 객체로 매핑"""
        p = Process()
        p.pid = int(fields[0])
        p.comm = self.getCmdlineInPod(p.pid)
        if not p.comm:
            p.comm = fields[1].strip('()')
        try:
            p.state = Mode_State[fields[2]].value
        except KeyError:
            p.state = f"Unknown({fields[2]})"
        p.ppid = int(fields[3])
        p.pgrp = int(fields[4])
        p.session = int(fields[5])
        p.tty_nr = int(fields[6])
        p.tpgid = int(fields[7])
        p.flags = int(fields[8])
        p.minflt = int(fields[9])
        p.cminflt = int(fields[10])
        p.majflt = int(fields[11])
        p.cmajflt = int(fields[12])
        p.utime = int(fields[13])
        p.stime = int(fields[14])
        p.cutime = int(fields[15])
        p.cstime = int(fields[16])
        p.priority = int(fields[17])
        p.nice = int(fields[18])
        p.num_threads = int(fields[19])
        p.itrealvalue = int(fields[20])
        p.starttime = int(fields[21])
        p.vsize = int(fields[22])
        p.rss = int(fields[23])
        p.rsslim = int(fields[24])
        p.startcode = int(fields[25])
        p.endcode = int(fields[26])
        p.startstack = int(fields[27])
        p.kstkesp = int(fields[28])
        p.kstkeip = int(fields[29])
        p.signal = int(fields[30])
        p.blocked = int(fields[31])
        p.sigignore = int(fields[32])
        p.sigcatch = int(fields[33])
        p.wchan = int(fields[34])
        p.nswap = int(fields[35])
        p.cnswap = int(fields[36])
        p.exit_signal = int(fields[37])
        p.processor = int(fields[38])
        p.rt_priority = int(fields[39])
        p.policy = Policy_State(int(fields[40])).name
        p.delayacct_blkio_ticks = int(fields[41])
        p.guest_time = int(fields[42])
        p.cguest_time = int(fields[43])
        p.start_data = int(fields[44])
        p.end_data = int(fields[45])
        p.start_brk = int(fields[46])
        p.arg_start = int(fields[47])
        p.arg_end = int(fields[48])
        p.env_start = int(fields[49])
        p.env_end = int(fields[50])
        p.exit_code = int(fields[51])

        return p

    def getProcessMetrics(self, process):
        """
        특정 PID의 컨텍스트 스위치, 메모리, I/O 메트릭 수집
        """
        metrics = ProcessMetrics()
        pid = process.pid

        # --- /proc/[pid]/status 읽기 (context switch + VmRSS) ---
        r = pod_exec(self.v1, self.pod.metadata.name, self.namespace,
                     ["cat", f"/proc/{pid}/status"], timeout=self.exec_timeout)
        if r.ok:
            try:
                for line in r.stdout.splitlines():
                    line = line.strip()
                    if line.startswith("voluntary_ctxt_switches:"):
                        metrics.voluntary_ctxt_switches = int(line.split()[1])
                    elif line.startswith("nonvoluntary_ctxt_switches:"):
                        metrics.nonvoluntary_ctxt_switches = int(line.split()[1])
                    elif line.startswith("VmRSS:"):
                        metrics.vm_rss = int(line.split()[1]) * 1024  # kB → bytes
            except (ValueError, IndexError) as e:
                print(f"[WARN] Failed to parse /proc/{pid}/status: {e}")
        elif not r.reachable:
            print(f"[WARN] Failed to read /proc/{pid}/status "
                  f"({self.pod.metadata.name}): {r.status.value} {r.error[:120]}")

        # --- /proc/[pid]/io 읽기 (I/O workload) ---
        r = pod_exec(self.v1, self.pod.metadata.name, self.namespace,
                     ["cat", f"/proc/{pid}/io"], timeout=self.exec_timeout)
        if r.ok:
            try:
                for line in r.stdout.splitlines():
                    line = line.strip()
                    if line.startswith("read_bytes:"):
                        metrics.read_bytes = int(line.split()[1])
                    elif line.startswith("write_bytes:"):
                        metrics.write_bytes = int(line.split()[1])
            except (ValueError, IndexError) as e:
                print(f"[WARN] Failed to parse /proc/{pid}/io: {e}")

        process.metrics = metrics

    def getCgroupMetrics(self) -> Optional[CgroupMetrics]:
        """
        cgroup 통계 정보 수집 (exec로 memory.current, memory.max, io.stat 읽음)
        """
        cgroup_metrics = CgroupMetrics()

        try:
            command = [
                "sh", "-c",
                "cat /sys/fs/cgroup/memory.current "
                "/sys/fs/cgroup/memory.max "
                "/sys/fs/cgroup/io.stat"
            ]
            r = pod_exec(self.v1, self.pod.metadata.name, self.namespace,
                         command, timeout=self.exec_timeout)
            if not r.ok:
                print(f"Error collecting cgroup metrics: {r.status.value} {r.error[:120]}")
                return cgroup_metrics

            # 결과 파싱
            lines = r.stdout.strip().splitlines()

            if len(lines) >= 1:
                try:
                    cgroup_metrics.memory_current = int(lines[0].strip())
                except ValueError:
                    pass

            if len(lines) >= 2:
                val = lines[1].strip()
                if val.isdigit():
                    cgroup_metrics.memory_limit = int(val)
                elif val == "max":
                    cgroup_metrics.memory_limit = None  # 무제한이면 None 처리

            if len(lines) >= 3:
                total_rbytes, total_wbytes = 0, 0
                for line in lines[2:]:
                    if line.strip():
                        parts = line.split()
                        if len(parts) >= 2:
                            stats = parts[1:]
                            for stat in stats:
                                if "=" in stat:
                                    key, value = stat.split("=", 1)
                                    try:
                                        if key == "rbytes":
                                            total_rbytes += int(value)
                                        elif key == "wbytes":
                                            total_wbytes += int(value)
                                    except ValueError:
                                        pass
                cgroup_metrics.io_read_bytes = total_rbytes
                cgroup_metrics.io_write_bytes = total_wbytes

        except Exception as e:
            print(f"Error collecting cgroup metrics: {e}")

        return cgroup_metrics

    def analyzePodProcess(self, processes):
        """
        return:
        분석결과
          - detailed_classification(프로세스 분류 정보): dict
          - process_summary(프로세스 요약정보): dict
        """
        if not processes:
            return [], {'total': 0, 'active': 0, 'inactive': 0, 'zombie': 0}
        pod_name = self.pod.metadata.name
        current_time = time.time()

        process_classification: list = []
        process_summary: dict = {
            'total': len(processes),
            'active': 0,  # 활성
            'inactive': 0,  # 비활성
            'zombie': 0,  # 좀비
        }
        for process in processes:
            classification = self._classify_process(process, pod_name)
            # print(classification)
            process_classification.append(classification)

            # 분류 결과 요약
            if classification['state'] == ProcessStateClassification.ACTIVE:
                process_summary['active'] += 1
            elif classification['state'] == ProcessStateClassification.INACTIVE:
                process_summary['inactive'] += 1
                if classification['reason'] == 'Zombie':
                    process_summary['zombie'] += 1

        # print(process_summary)
        # 현재 CPU 통계 저장
        self._updateState(pod_name, processes, current_time)

        return process_classification, process_summary

    def _classify_process(self, p, podName: str) -> Dict:
        """
        각 프로세스의 상태를 분류
        p = process
        return:
            프로세스의 상태: dict
            pid, comm, state, reason, CPUtime_delta, ctxt_delta, non_ctxt_delta, rss_delta, minflt_delta, io_delta
        """
        # 1. 프로세스 상태 기반 판단
        # Zombie/Dead 프로세스
        if p.state in ProcessStatePolicy.INACTIVE_STATES:
            return {
                'pid': p.pid,
                'comm': p.comm,
                'state': ProcessStateClassification.INACTIVE,
                'reason': 'Zombie',
            }

        # 이전 상태가 없으면 활성으로 간주
        if podName not in self.previous_states:
            return {
                'pid': p.pid,
                'comm': p.comm,
                'state': ProcessStateClassification.ACTIVE,
                'reason': 'no_prev_state',
            }

        prev_states = self.previous_states[podName].get('processes', {})
        if p.pid not in prev_states:
            return {
                'pid': p.pid,
                'comm': p.comm,
                'state': ProcessStateClassification.ACTIVE,
                'reason': 'new_process',
            }

        # 증가량(delta) 계산
        prev = prev_states[p.pid]
        deltas = self._calculateDeltas(p, prev)

        # 2. Running/Uninterruptible 프로세스는 활성
        if p.state in ProcessStatePolicy.ACTIVE_STATES:
            return self._makeActiveResult(p, 'Running_state', deltas)

        # 3. CPU delta 체크
        if deltas['CPUtime'] >= ProcessStatePolicy.CPU_TIME_DELTA_THRESHOLD:
            return self._makeActiveResult(p, 'CPUtime_high', deltas)

        # 4. context switch delta 체크
        if deltas['voluntary_ctxt'] >= ProcessStatePolicy.VOLUNTARY_CTXT_SWITCH_DELTA_THRESHOLD:
            return self._makeActiveResult(p, 'voluntary_ctxt_switch_high', deltas)
        if deltas['nonvoluntary_ctxt'] >= ProcessStatePolicy.NON_VOLUNTARY_CTXT_SWITCH_DELTA_THRESHOLD:
            return self._makeActiveResult(p, 'non_voluntary_ctxt_switch_high', deltas)

        # 5. RSS, IO, Page fault 변화 여부
        if deltas['rss'] != 0:
            return self._makeActiveResult(p, 'rss_changed', deltas)
        if deltas['io_bytes'] > 0:
            return self._makeActiveResult(p, 'io_bytes_increase', deltas)
        if deltas['minflt'] > 0:
            return self._makeActiveResult(p, 'minflt_increase', deltas)

        # 6. 비활성
        return {
            'pid': p.pid,
            'comm': p.comm,
            'state': ProcessStateClassification.INACTIVE,
            'reason': 'inactive',
            'CPUtime_delta': deltas['CPUtime'],
            'ctxt_delta': deltas['voluntary_ctxt'],
            'non_ctxt_delta': deltas['nonvoluntary_ctxt'],
            'rss_delta': deltas['rss'],
            'minflt_delta': deltas['minflt'],
            'io_delta': deltas['io_bytes']
        }

    def _calculateDeltas(self, p, prev) -> Optional[dict]:
        """
        CPU 활동률 계산 (이전 계산 값과 비교)
        return:
            None or CPUtime 증가값: float
            이전 계산 값이 없을 경우 None 반환
        """
        deltas = {}
        deltas['CPUtime'] = (p.utime + p.stime) - (prev.get('utime', 0) + prev.get('stime', 0))
        deltas['voluntary_ctxt'] = (p.metrics.voluntary_ctxt_switches or 0) - prev.get('voluntary_ctxt', 0)
        deltas['nonvoluntary_ctxt'] = (p.metrics.nonvoluntary_ctxt_switches or 0) - prev.get('nonvoluntary_ctxt', 0)
        deltas['rss'] = (p.rss or 0) - prev.get('rss', 0)
        deltas['minflt'] = p.minflt - prev.get('minflt', 0)
        deltas['io_bytes'] = ((p.metrics.read_bytes or 0) + (p.metrics.write_bytes or 0)) - prev.get('io_bytes', 0)

        return deltas

    def _updateState(self, pod_name, processes, current_time):
        """
        현재 CPU 통계를 저장
        """
        self.previous_states[pod_name] = {
            'timestamp': current_time,
            'processes': {}
        }

        for p in processes:
            write_byte = p.metrics.write_bytes or 0
            read_byte = p.metrics.read_bytes or 0
            self.previous_states[pod_name]['processes'][p.pid] = {
                'CPUtime': p.utime + p.stime,
                'voluntary_ctxt': p.metrics.voluntary_ctxt_switches or 0,
                'nonvoluntary_ctxt': p.metrics.nonvoluntary_ctxt_switches or 0,
                'rss': p.rss or 0,
                'minflt': p.minflt or 0,
                'io_bytes': write_byte + read_byte,
                'comm': p.comm,
            }

    def _makeActiveResult(self, p, reason: str, deltas) -> dict:
        return {
            'pid': p.pid,
            'comm': p.comm,
            'state': ProcessStateClassification.ACTIVE,
            'reason': reason,
            'CPUtime_delta': deltas['CPUtime'],
            'ctxt_delta': deltas['voluntary_ctxt'],
            'non_ctxt_delta': deltas['nonvoluntary_ctxt'],
            'rss_delta': deltas['rss'],
            'minflt_delta': deltas['minflt'],
            'io_delta': deltas['io_bytes']
        }

if __name__ == "__main__":
    startTime = time.time()

    config.load_kube_config()
    v1 = client.CoreV1Api()
    pods: dict = v1.list_namespaced_pod('gc-simulator').items
    podlist = {}
    process_data = {}
    cnt = 0
    for pod in pods:
        if cnt == 30:
            break
        p = ProcessManager(v1, pod)
        podlist[pod.metadata.name] = p
        process_data[pod.metadata.name] = p.getPorcessData()
        print(cnt, pod.metadata.name)
        cnt += 1
    endTime = time.time()
    runtime = endTime - startTime
    print(f"전체 수행 시간: {runtime:.2f}초")

    for i in range(10):
        startTime = time.time()
        cnt = 0
        for pod in pods:
            if cnt == 30:
                break
            print(podlist[pod.metadata.name].analyzePodProcess(process_data[pod.metadata.name]))
            cnt += 1
        endTime = time.time()
        runtime = endTime - startTime
        print(f"알고리즘 수행 시간: {runtime:.2f}초")
        if i == 9:
            break
        time.sleep(60)
        startTime = time.time()
        for pod in pods:
            processes = podlist[pod.metadata.name].getPorcessData()
            process_data[pod.metadata.name] = processes
        endTime = time.time()
        runtime = endTime - startTime
        print(f"전체 수행 시간: {runtime:.2f}초")
