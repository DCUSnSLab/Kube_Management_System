import os
from datetime import datetime
import csv
from enum import Enum
from typing import Dict, Optional
from process import CgroupMetrics, ProcessMetrics, Process, Mode_State, Policy_State

from kubernetes import client, config, stream
import time

class ProcessStateClassification(Enum):
    """프로세스 상태 분류"""
    ACTIVE = "active"          # 활성 프로세스
    INACTIVE = "inactive"      # 비활성 프로세스
    GC = "gc"                  # GC 대상 (비활성 상태)

class ProcessStatePolicy:
    """프로세스 상태 분류 기준"""
    # State 기반 분류 기준
    ACTIVE_STATES: dict = {'Running', 'Uninterruptible Sleep'}
    IDLE_STATES: dict = {'Sleeping', 'Stopped'}
    INACTIVE_STATES: dict = {'Zombie', 'Dead'}
    # CPU 자원 관련 지표(변화률 기반)
    CPU_TIME_DELTA_THRESHOLD = 215                      # CPU time(stime + utime) 임계치, jiffies(틱) 단위
    VOLUNTARY_CTXT_SWITCH_DELTA_THRESHOLD = 716         # Voluntary Context Switch 임계치
    NON_VOLUNTARY_CTXT_SWITCH_DELTA_THRESHOLD = 158     # Non-Voluntary Context Switch 임계치
    # 예: N=5분, 특정 시간동안 비활성일 경우 GC 대상으로 변경
    INACTIVE_DURATION_THRESHOLD = 5 * 60

class ProcessManager:
    def __init__(self, api_instance, pod):
        self.v1 = api_instance
        self.pod = pod
        self.namespace: str = pod.metadata.namespace

        self.previous_states: dict = {}  # pod별 이전 통계 저장하는 딕셔너리
        self.podInactiveSince: Dict[str, float] = {}   # pod 비활성 시작 시간 저장 (name, time)
        self.time = time

    def getPorcessData(self):
        """
        프로세스 정보를 수집하는 함수를 최종적으로 실햄
        """
        stat_data = self.getProcStat()
        if not stat_data:
            return None

        processes = self.insertProcessStatData(stat_data)
        # cgroups = self.getCgroupMetrics()

        # return {
        #     'processes': processes,
        #     'cgroups': cgroups,
        # }
        return processes

    def getProcStat(self):
        # 자기 자신을 제외하고, PPID가 1인 'sleep' 프로세스도 제외하는 쉘 스크립트 사용
        command = [
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
        try:
            exec_command = stream.stream(
                self.v1.connect_get_namespaced_pod_exec,
                self.pod.metadata.name,
                self.namespace,
                command=command,
                stderr=True, stdin=False,
                stdout=True, tty=False
            )
            return exec_command
        except Exception as e:
            if "Connection to remote host was lost" in str(e):
                print(f"Connection to Pod '{self.pod.metadata.name}' was lost. Skipping this Pod.")
            else:
                print(f"An unexpected error occurred: {e}")
            return None

    def getCmdlineInPod(self, pid):
        """
        풀 커맨드(cmdline)를 얻으려면 Pod 안의 /proc/[pid]/cmdline을 읽어야함
        """
        command = ["cat", f"/proc/{pid}/cmdline"]
        exec_command = stream.stream(
            self.v1.connect_get_namespaced_pod_exec,
            self.pod.metadata.name,
            self.namespace,
            command=command,
            stderr=True, stdin=False,
            stdout=True, tty=False
        )
        return exec_command.replace("\x00", " ").strip()

    def insertProcessStatData(self, processStat) -> list[Process]:
        """get /proc/stat data amd split into 52"""
        processes = []
        if processStat is None:
            print(f"Skipping Pod '{self.pod.metadata.name}': Failed to retrieve process data.")
            return

        for line in processStat.splitlines():
            fields = line.split()
            if len(fields) < 2:  # 최소 2개의 필드가 있어야 함
                continue

            p = Process()

            # Map fields to Process attributes
            try:
                p.pid = int(fields[0])
            except ValueError:
                print(f"Skipping invalid PID in line: {line}")
                continue
            try:
                p.comm = self.getCmdlineInPod(p.pid)
            except Exception as e:
                print(f"Skipping full name of process: {e}")
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

            # memory, context switch, i/o data
            self.getProcessMetrics(p)

            processes.append(p)

        return processes

    def getProcessMetrics(self, process):
        """
        특정 PID의 컨텍스트 스위치, 메모리, I/O 메트릭 수집
        """
        metrics = ProcessMetrics()
        pid = process.pid

        try:
            # /proc/[pid]/status 읽기 (context switch + VmRSS)
            command = ["cat", f"/proc/{pid}/status"]
            exec_command = stream.stream(
                self.v1.connect_get_namespaced_pod_exec,
                self.pod.metadata.name,
                self.namespace,
                command=command,
                stderr=True, stdin=False,
                stdout=True, tty=False
            )
            for line in exec_command.splitlines():
                line = line.strip()
                if line.startswith("voluntary_ctxt_switches:"):
                    metrics.voluntary_ctxt_switches = int(line.split()[1])
                elif line.startswith("nonvoluntary_ctxt_switches:"):
                    metrics.nonvoluntary_ctxt_switches = int(line.split()[1])
                elif line.startswith("VmRSS:"):
                    # VmRSS 값은 kB 단위 → bytes로 변환
                    metrics.vm_rss = int(line.split()[1]) * 1024

            # /proc/[pid]/io 읽기 (I/O workload)
            command = ["cat", f"/proc/{pid}/io"]
            exec_command = stream.stream(
                self.v1.connect_get_namespaced_pod_exec,
                self.pod.metadata.name,
                self.namespace,
                command=command,
                stderr=True, stdin=False,
                stdout=True, tty=False
            )
            for line in exec_command.splitlines():
                line = line.strip()
                if line.startswith("read_bytes:"):
                    metrics.read_bytes = int(line.split()[1])
                elif line.startswith("write_bytes:"):
                    metrics.write_bytes = int(line.split()[1])

        except Exception as e:
            print(f"Error collecting metrics for PID {pid}: {e}")

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
            exec_command = stream.stream(
                self.v1.connect_get_namespaced_pod_exec,
                self.pod.metadata.name,
                self.namespace,
                command=command,
                stderr=True, stdin=False,
                stdout=True, tty=False
            )

            # 결과 파싱
            lines = exec_command.strip().splitlines()

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
          - should_gc(gc여부): bool
          - reason(gc이유): str
          - detailed_classification(프로세스 분류 정보): dict
          - process_summary(프로세스 요약정보): dict
        """
        if not processes:
            return{
                'should_gc': False,
                'reason': 'no processes found',
                'detailed_classification': {},
                'process_summary': {}
            }
        pod_name = self.pod.metadata.name
        current_time = time.time()

        process_classification: list = []
        process_summary: dict = {
            'total': len(processes),
            'active': 0,            # 활성
            'inactive': 0,          # 비활성
            'zombie': 0,            # 좀비
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

        # GC 여부 결정
        gc_decision = self._make_gc_decision(process_summary, pod_name, current_time)

        return gc_decision['should_gc'], gc_decision['reason'], process_classification, process_summary

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
        return{
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
        deltas['CPUtime'] = (p.utime + p.stime) - (prev.get('utime', 0) + prev.get('stime',0))
        deltas['voluntary_ctxt'] = p.metrics.voluntary_ctxt_switches - prev.get('voluntary_ctxt', 0)
        deltas['nonvoluntary_ctxt'] = p.metrics.nonvoluntary_ctxt_switches - prev.get('nonvoluntary_ctxt', 0)
        deltas['rss'] = (p.metrics.vm_rss or 0) - prev.get('rss', 0)
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
            self.previous_states[pod_name]['processes'][p.pid] = {
                'CPUtime': p.utime + p.stime,
                'voluntary_ctxt': p.metrics.voluntary_ctxt_switches,
                'nonvoluntary_ctxt': p.metrics.nonvoluntary_ctxt_switches,
                'rss': p.rss,
                'minflt': p.minflt,
                'io_bytes': p.metrics.write_bytes + p.metrics.read_bytes,
                'comm': p.comm,
            }

    def _makeActiveResult(self, p, reason: str, deltas) -> dict:
        return{
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

    def _make_gc_decision(self, summary: dict, pod_name, current_time) -> Dict:
        """
        프로세스 분석 결과를 바탕으로 GC 결정
        Return:
            GC 결정 결과: dict
        """
        # 1. 활성 프로세스가 있으면 유지
        if summary['active'] > 0:
            if pod_name in self.podInactiveSince:
                self.podInactiveSince.pop(pod_name, None)
            return{
                'should_gc': False,
                'reason': f"Pod has {summary['active']} active process(es)"
            }

        # 2. 모든 프로세스가 비활성이면 GC 여부 확인을 위해 비활성 상태 유지 시간 확인
        if summary['inactive'] == summary['total']:
            self.podInactiveSince.setdefault(pod_name, current_time)
            inactive_elapsed = current_time - self.podInactiveSince[pod_name]
            if inactive_elapsed >= ProcessStatePolicy.INACTIVE_DURATION_THRESHOLD:
                reason = f"All processes inactive for {inactive_elapsed / 60:.1f} min (≥ {ProcessStatePolicy.INACTIVE_DURATION_THRESHOLD / 60:.0f} min)"
                return {
                    'should_gc': True,
                    'reason': reason
                }
            else:
                # 아직 임계시간 미도달
                remain = ProcessStatePolicy.INACTIVE_DURATION_THRESHOLD - inactive_elapsed
                reason = f"All processes inactive, waiting {remain / 60:.1f} more min to GC"
                return {
                    'should_gc': False,
                    'reason': reason
                }

        # 3. 기본적으로 GC하지 않음
        return{
            'should_gc': False,
            'reason': f"Pod has {summary['inactive']} inactive process"
        }

if __name__ == "__main__":
    startTime = time.time()

    config.load_kube_config()
    v1 = client.CoreV1Api()
    pods: dict = v1.list_namespaced_pod('swlabpods').items
    cnt = 0
    p = None
    processes = None
    for pod in pods:
        if cnt == 30:
            break
        p = ProcessManager(v1, pod)
        processes = p.getPorcessData()
        print(cnt, pod.metadata.name)
        cnt += 1
    endTime = time.time()
    runtime = endTime - startTime
    print(f"전체 수행 시간: {runtime:.2f}초")

    startTime = time.time()
    cnt = 0
    for pod in pods:
        if cnt == 30:
            break
        print(p.analyzePodProcess(processes))
        cnt += 1

    endTime = time.time()
    runtime = endTime - startTime
    print(f"전체 수행 시간: {runtime:.2f}초")