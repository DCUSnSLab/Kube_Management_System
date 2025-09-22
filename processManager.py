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
    IDLE = "idle"              # 유휴 프로세스
    GC = "gc"                  # GC 대상 (비활성 상태)

class ProcessStatePolicy:
    """프로세스 상태 분류 기준"""
    # State 기반 분류 기준
    ACTIVE_STATES: dict = {'Running', 'Uninterruptible Sleep'}
    IDLE_STATES: dict = {'Sleeping', 'Stopped'}
    INACTIVE_STATES: dict = {'Zombie', 'Dead'}
    # CPU 활동률기반 분류 기준
    ACTIVE_CPU_THRESHOLD = 0.01     # 1% 초과
    IDLE_CPU_THRESHOLD = 0          # 0 초과
    # 경과 시간(나이) 기준 (초 단위)
    ACTIVE_NEW_AGE_THRESHOLD = 5 * 60           # 5분 미만(신규 프로세스)
    ACTIVE_AGE_THRESHOLD = 1 * 60 * 60          # 1시간 미만 (활동률 0일 경우 idle)
    IDLE_AGE_THRESHOLD = 24 * 60 * 60           # 24시간 미만 (활동률 0일 경우 inactive)

class ProcessManager:
    def __init__(self, api_instance, pod):
        self.v1 = api_instance
        self.pod = pod
        self.namespace: str = pod.metadata.namespace

        self.cpu_ticks_per_sec = 10
        self.previous_cpu_states: dict = {}  # pod별 이전 CPU 통계 저장하는 딕셔너리
        self.sampling_interval = 60
        self.time = time

    def getPorcessData(self):
        """
        프로세스 정보를 수집하는 함수를 최종적으로 실햄
        """
        stat_data = self.getProcStat()
        if not stat_data:
            return None

        processes = self.insertProcessStatData(stat_data)
        cgroups = self.getCgroupMetrics()

        return {
            'processes': processes,
            'cgroups': cgroups,
        }

    def getProcStat(self):
        # 자기 자신을 제외하고, PPID가 1인 'sleep' 프로세스도 제외하는 쉘 스크립트 사용
        command = [
            "sh", "-c",
            "SELF_PID=$$ && "
            "for stat in /proc/[0-9]*/stat; do "
            "  if [ -r \"$stat\" ]; then "
            "    PID=$(basename $(dirname \"$stat\")) && "
            "    if [ \"$PID\" != \"$SELF_PID\" ]; then "
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
                'process_summary': {}
            }
        pod_name = self.pod.metadata.name
        current_time = time.time()

        # btime 계산 (시스템 부팅 시간)
        exec_command = stream.stream(
            self.v1.connect_get_namespaced_pod_exec,
            self.pod.metadata.name,
            self.namespace,
            command=["cat", "/proc/uptime"],
            stderr=True, stdin=False,
            stdout=True, tty=False
        )
        uptime = float(exec_command.split()[0])
        boot_time = current_time - uptime

        process_classification: list = []
        process_summary: dict = {
            'total': len(processes),
            'active': 0,            # 활성
            'idle': 0,              # 유휴
            'gc_candidates': 0,     # 비활성 = gc 대상
            'zombie': 0,            # 좀비
        }
        for process in processes:
            classification = self._classify_process(process, pod_name, current_time, boot_time)
            print(classification)
            process_classification.append(classification)

            # 분류 결과 요약
            if classification['state'] == ProcessStateClassification.ACTIVE:
                process_summary['active'] += 1
            elif classification['state'] == ProcessStateClassification.IDLE:
                process_summary['idle'] += 1
            elif classification['state'] == ProcessStateClassification.GC:
                process_summary['gc_candidates'] += 1
                if classification['reason'] == 'Zombie':
                    process_summary['zombie'] += 1

        # print(process_summary)
        # 현재 CPU 통계 저장
        self._update_cpu_states(pod_name, processes, current_time)

        # GC 여부 결정
        gc_decision = self._make_gc_decision(process_summary)

        return gc_decision['should_gc'], gc_decision['reason'], process_classification, process_summary

    def _classify_process(self, p, pod_name: str, current_time, btime) -> Dict:
        """
        각 프로세스의 상태를 분류
        return:
            프로세스의 상태: dict
        """
        # 1. 프로세스 상태 기반 판단
        # Zombie/Dead 프로세스
        if p.state in ProcessStatePolicy.INACTIVE_STATES:
            return {
                'pid': p.pid,
                'comm': p.comm,
                'state': ProcessStateClassification.GC,
                'reason': 'Zombie',
                'cpu_activity': 0
            }

        # CPU 활동률 계산
        cpu_activity = self._calculate_cpu_activity(p.pid, p.utime, p.stime, pod_name, current_time)
        # 프로세스 나이(경과 시간) 계산
        process_age = self._calculate_process_age(p.starttime, btime, current_time)

        # Running/Uninterruptible 프로세스는 활성
        if p.state in ProcessStatePolicy.ACTIVE_STATES:
            return {
                'pid': p.pid,
                'comm': p.comm,
                'state': ProcessStateClassification.ACTIVE,
                'reason': 'Running_state',
                'cpu_activity': cpu_activity,
                'age_hours': process_age / 3600
            }

        # # 2. 프로세스 경과 시간 기반 판단 (활성)
        # # 결과 시간 < 5분
        # if process_age < ProcessStatePolicy.ACTIVE_NEW_AGE_THRESHOLD:
        #     return {
        #         'pid': p.pid,
        #         'comm': p.comm,
        #         'state': ProcessStateClassification.ACTIVE,
        #         'reason': 'new_process_5m',
        #         'cpu_activity': cpu_activity,
        #         'age_hours': process_age / 3600
        #     }

        # 3. CPU 활동률 기반 상태 판단
        # CPU 활동률이 None일 경우
        if cpu_activity is None:
            return {
                'pid': p.pid,
                'comm': p.comm,
                'state': ProcessStateClassification.IDLE,
                'reason': 'cpu_activity_None',
                'cpu_activity': cpu_activity,
                'age_hours': process_age / 3600
            }
        # CPU 활동률 > 1%
        if cpu_activity > ProcessStatePolicy.ACTIVE_CPU_THRESHOLD:
            return {
                'pid': p.pid,
                'comm': p.comm,
                'state': ProcessStateClassification.ACTIVE,
                'reason': 'high_cpu_activity',
                'cpu_activity': cpu_activity,
                'age_hours': process_age / 3600
            }

        # 4. 종합적인 분류
        # CPU 활동률이 매우 낮거나 없는 경우 (CPU activity < 1%)
        # 경과 시간 >= 24h
        if process_age >= ProcessStatePolicy.IDLE_AGE_THRESHOLD:
            return {
                'pid': p.pid,
                'comm': p.comm,
                'state': ProcessStateClassification.GC,
                'reason': 'very_old_process',
                'cpu_activity': cpu_activity,
                'age_hours': process_age / 3600
            }
        # 경과 시간 < 1h
        elif process_age < ProcessStatePolicy.ACTIVE_AGE_THRESHOLD:
            # 0 < CPU 활동률 <= 1%
            if cpu_activity > ProcessStatePolicy.IDLE_CPU_THRESHOLD:
                return {
                    'pid': p.pid,
                    'comm': p.comm,
                    'state': ProcessStateClassification.ACTIVE,
                    'reason': 'low_cpu_activity_1h',
                    'cpu_activity': cpu_activity,
                    'age_hours': process_age / 3600
                }
            # CPU 활동률 = 0
            elif cpu_activity == ProcessStatePolicy.IDLE_CPU_THRESHOLD:
                return {
                    'pid': p.pid,
                    'comm': p.comm,
                    'state': ProcessStateClassification.IDLE,
                    'reason': 'very_low_cpu_activity_1h',
                    'cpu_activity': cpu_activity,
                    'age_hours': process_age / 3600
                }
        # 경과 시간 < 24h
        elif process_age < ProcessStatePolicy.IDLE_AGE_THRESHOLD:
            # 0 < CPU 활동률 <= 1%
            if cpu_activity > ProcessStatePolicy.IDLE_CPU_THRESHOLD:
                return {
                    'pid': p.pid,
                    'comm': p.comm,
                    'state': ProcessStateClassification.IDLE,
                    'reason': 'old_process',
                    'cpu_activity': cpu_activity,
                    'age_hours': process_age / 3600
                }
            # CPU 활동률 = 0
            elif cpu_activity == ProcessStatePolicy.IDLE_CPU_THRESHOLD:
                return {
                    'pid': p.pid,
                    'comm': p.comm,
                    'state': ProcessStateClassification.GC,
                    'reason': 'old_and_very_low_cpu_activity',
                    'cpu_activity': cpu_activity,
                    'age_hours': process_age / 3600
                }
        # 모두 해당하지 않는 경우(예외, idle로 간주)
        return{
            'pid': p.pid,
            'comm': p.comm,
            'state': ProcessStateClassification.IDLE,
            'reason': 'except_idle',
            'cpu_activity': cpu_activity,
            'age_hours': process_age / 3600
        }

    def _calculate_cpu_activity(self, pid, utime, stime, pod_name, current_time) -> Optional[float]:
        """
        CPU 활동률 계산 (이전 계산 값과 비교)
        return:
            None or CPU 활동률 (0.0 ~ 1.0): float
            이전 계산 값이 없을 경우 None 반환
        """
        if pod_name not in self.previous_cpu_states:
            # print(f"No previous CPU data process {pid} in {pod_name}")
            return None

        prev_states = self.previous_cpu_states[pod_name].get('processes', {})
        if pid not in prev_states:
            return None

        prev_process = prev_states[pid]
        time_diff = current_time - self.previous_cpu_states[pod_name]['timestamp']

        if time_diff <= 0:
            return None

        # CPU 시간 차이 계산 (utime + stime)
        current_cpu_time = (utime + stime) / self.cpu_ticks_per_sec
        prev_cpu_time = (prev_process['utime'] + prev_process['stime']) / self.cpu_ticks_per_sec

        cpu_diff = current_cpu_time - prev_cpu_time

        # CPU 활동률 = CPU 시간 증가량 / 실제 경과 시간
        cpu_activity = cpu_diff / time_diff

        return max(0.0, min(1.0, cpu_activity))  # 0.0 ~ 1.0 범위 제한

    def _calculate_process_age(self, starttime, btime, current_time) -> float:
        """
        프로세스 나이 계산 (단위: 초)
        = 현재 시간 - 프로세스 시작 시각(boot_time + starttime)
        starttime: 부팅 후 프로세스가 시간된 시점
        return:
            프로세스 나이{초}: float
        """
        p_start_time = btime + (starttime / self.cpu_ticks_per_sec)
        return current_time - p_start_time

    def _update_cpu_states(self, pod_name, processes, current_time):
        """
        현재 CPU 통계를 저장
        """
        self.previous_cpu_states[pod_name] = {
            'timestamp': current_time,
            'processes': {}
        }

        for p in processes:
            self.previous_cpu_states[pod_name]['processes'][p.pid] = {
                'utime': p.utime,
                'stime': p.stime,
                'comm': p.comm
            }

    def _make_gc_decision(self, summary: dict) -> Dict:
        """
        프로세스 분석 결과를 바탕으로 GC 결정
        Return:
            GC 결정 결과: dict
        """
        # 1. 활성 프로세스가 있으면 유지
        if summary['active'] > 0:
            return{
                'should_gc': False,
                'reason': f"Pod has {summary['active']} active process(es)"
            }

        # 2. Zombie 프로세스가 있으면 즉시 GC
        if summary['zombie'] > 0:
            return {
                'should_gc': True,
                'reason': f"Found {summary['zombie']} zombie process(es)"
            }

        # 3. 모든 프로세스가 비활성이면 GC 고려
        if summary['gc_candidates'] == summary['total']:
            return {
                'should_gc': True,
                'reason': f"All process inactive, {summary['gc_candidates']} GC candidates"
            }

        # 4. 모든 프로세스가 유휴인 경우
        if summary['idle'] == summary['total']:
            return {
                'should_gc': False,
                'reason': f"All process idle, {summary['idle']} idle"
            }

        # 5. 기본적으로 GC하지 않음
        return{
            'should_gc': False,
            'reason': f"Pod has {summary['idle']} idle and {summary['gc_candidates']} inactive process"
        }

if __name__ == "__main__":
    startTime = time.time()

    config.load_kube_config()
    v1 = client.CoreV1Api()
    pods: dict = v1.list_namespaced_pod('swlabpods').items
    cnt = 0
    for pod in pods:
        if cnt == 30:
            break
        p = ProcessManager(v1, pod)
        print(cnt, pod.metadata.name)
        print(p.getProcStat(), '\n')
        # print(p.getCgroupMetrics(), '\n')
        # print(p.getProcessMetrics(517239), '\n')
        cnt += 1

    endTime = time.time()
    runtime = endTime - startTime
    print(f"전체 수행 시간: {runtime:.2f}초")