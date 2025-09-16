from enum import Enum
from typing import Dict, Optional

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
        self.used_commands: list = ["xargs", "sh", "cat", "bash"]

        self.cpu_ticks_per_sec = 100
        self.previous_cpu_states: dict = {}  # pod별 이전 CPU 통계 저장하는 딕셔너리
        self.sampling_interval = 60

    def getProcStat(self):  # legacy (No use)
        command = ["sh", "-c", "ls -d /proc/[0-9]* | xargs -I {} sh -c 'cat {}/stat 2>/dev/null'"]
        try:
            exec_command = stream.stream(
                self.v1.connect_get_namespaced_pod_exec,
                self.pod.metadata.name,
                self.namespace,
                command=command,
                stderr=True, stdin=False,
                stdout=True, tty=False
            )
            return self._filter_command_processes(exec_command)
        except Exception as e:
            if "Connection to remote host was lost" in str(e):
                print(f"Connection to Pod '{self.pod.metadata.name}' was lost. Skipping this Pod.")
            else:
                print(f"An unexpected error occurred: {e}")
            return None

    def getProcStat_v2(self):
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

    def _filter_command_processes(self, exec_command):
        """
        Exclude processes where:
        processes created by command execution
        """
        if not exec_command:
            return ""

        filtered_processes: list = []
        processes_info: list = []

        for line in exec_command.splitlines():
            fields = line.split()
            if len(fields) > 2:
                try:
                    pid = int(fields[0])
                    comm = fields[1].strip("()")
                    ppid = int(fields[3])

                    processes_info.append({
                        'line': line,
                        'pid': pid,
                        'comm': comm,
                        'ppid': ppid
                    })
                except (ValueError, IndexError):
                    continue

        # 필터링할 PID 수집 (set: 중복없이 수집)
        filter_pids = set()

        # used_commands에 있는 프로세스와 그 자식들 필터링
        for proc in processes_info:
            if proc['comm'] in self.used_commands:
                filter_pids.add(proc['pid'])
                # 이 프로세스의 자식들도 필터링
                for child in processes_info:
                    if child['ppid'] == proc['pid']:
                        filter_pids.add(child['pid'])

        # 필터링되지 않은 프로세스만 반환
        for proc in processes_info:
            if proc['pid'] not in filter_pids:
                filtered_processes.append(proc['line'])

        return "\n".join(filtered_processes)

    def analyze(self, processes) -> Dict:
        """
        return:
        분석결과
          - should_gc(gc여부): bool
          - reason(gc이유): str
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
            # print(classification)
            # print(process.ppid)
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
        # Running/Uninterruptible 프로세스는 활성
        if p.state in ProcessStatePolicy.ACTIVE_STATES:
            return {
                'pid': p.pid,
                'comm': p.comm,
                'state': ProcessStateClassification.ACTIVE,
                'reason': 'Running_state',
                'cpu_activity': None
            }

        # CPU 활동률 계산
        cpu_activity = self._calculate_cpu_activity(p.pid, p.utime, p.stime, pod_name, current_time)
        # 프로세스 나이(경과 시간) 계산
        process_age = self._calculate_process_age(p.starttime, btime, current_time)

        # 2. 프로세스 경과 시간 기반 판단 (활성)
        # 결과 시간 < 5분
        if process_age < ProcessStatePolicy.ACTIVE_NEW_AGE_THRESHOLD:
            return {
                'pid': p.pid,
                'comm': p.comm,
                'state': ProcessStateClassification.ACTIVE,
                'reason': 'new_process_5m',
                'cpu_activity': cpu_activity,
                'age_hours': process_age / 3600
            }

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
        print(p.getProcStat_v2(),'\n')
        cnt += 1

    endTime = time.time()
    runtime = endTime - startTime
    print(f"전체 수행 시간: {runtime:.2f}초")