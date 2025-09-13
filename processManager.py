from enum import Enum
from typing import Dict, Optional

from kubernetes import client, config, stream
import time

class ProcessStateClassification(Enum):
    """프로세스 상태 분류"""
    ACTIVE = "active"          # 활성 프로세스
    IDLE = "idle"              # 유휴 프로세스
    INACTIVE = "inactive"      # 비활성 프로세스
    GC = "gc"                  # GC 대상

class ProcessManager:
    def __init__(self, api_instance, pod):
        self.v1 = api_instance
        self.pod = pod
        self.namespace: str = pod.metadata.namespace
        self.used_commands: list = ["xargs", "sh", "cat", "bash"]

        self.cpu_ticks_per_sec = 100
        self.previous_cpu_states: dict = {}  # pod별 이전 CPU 통계 저장하는 딕셔너리
        self.sampling_interval = 60

        # State 기반 분류
        self.active_states: dict = {'R':'Running', 'D':'Uninterruptible Sleep'}
        self.idle_states: dict = {'S':'Sleeping', 'T':'Stopped'}
        self.gc_states: dict = {'Z':'Zombie', 'X':'Dead'}

        # CPU 활동률기반 분류 기준
        self.active_cpu_threshold = 0.01  # 1% 이상
        self.idle_cpu_threshold = 0.001  # 0.1% 이상

        # 시간(나이) 기준 (초 단위)
        self.active_age_threshold = 1 * 60 * 60
        self.idle_age_threshold = 24 * 60 * 60

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
        # 자기 자신을 제외하는 쉘 스크립트 사용
        command = [
            "sh", "-c",
            "SELF_PID=$$ && "
            "for stat in /proc/[0-9]*/stat; do "
            "  if [ -r \"$stat\" ]; then "
            "    PID=$(basename $(dirname \"$stat\")) && "
            "    [ \"$PID\" != \"$SELF_PID\" ] && cat \"$stat\" 2>/dev/null; "
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
            'inactive': 0,          # 비활성
            'zombie': 0,            # 좀비
            'gc_candidates': 0      # gc 대상
        }
        for process in processes:
            classification = self._classify_process(process, pod_name, current_time, boot_time)
            process_classification.append(classification)


    def _classify_process(self, p, pod_name: str, current_time, btime) -> Dict:
        """
        각 프로세스의 상태를 분류
        return:
            프로세스의 상태: dict
        """
        # 1. 프로세스 상태 기반 판단
        # Zombie/Dead 프로세스
        if p.state in self.gc_states:
            return {
                'pid': p.pid,
                'comm' : p.comm,
                'state': ProcessStateClassification.GC,
                'reason': 'Zombie',
                'cpu_activity': 0
            }
        # Running/Uninterruptible 프로세스는 활성
        if p.state in self.active_states:
            return {
                'pid': p.pid,
                'comm' : p.comm,
                'state': ProcessStateClassification.ACTIVE,
                'reason': 'Active_state',
                'cpu_activity': None
            }

        # CPU 활동률 계산
        cpu_activity = self._calculate_cpu_activity(p.pid, p.utime, p.stime, pod_name, current_time)

        # 프로세스 나이 계산
        process_age = self._calculate_process_age(p.starttime, btime, current_time)

        # 2. 프로세스의 경과 시간 기반 판단 (활성)
        if process_age < self.active_age_threshold:
            return {
                'pid': p.pid,
                'comm': p.comm,
                'state': ProcessStateClassification.ACTIVE,
                'reason': 'new_process',
                'cpu_activity': cpu_activity,
                'age_hours': process_age / 3600
            }

        # 3. 프로세스 활동률 기반 판단
        if cpu_activity is not None:
            if cpu_activity < self.active_age_threshold:
                return {
                    'pid': p.pid,
                    'comm': p.comm,
                    'state': ProcessStateClassification.ACTIVE,
                    'reason': 'high_cpu_activity',
                    'cpu_activity': cpu_activity,
                    'age_hours': process_age / 3600
                }
            elif cpu_activity > self.idle_age_threshold:
                return {
                    'pid': p.pid,
                    'comm': p.comm,
                    'state': ProcessStateClassification.IDLE,
                    'reason': 'low_cpu_activity',
                    'cpu_activity': cpu_activity,
                    'age_hours': process_age / 3600
                }
        # 4. CPU 활동률이 없거나 매우 낮은 경우
        # 작성 필요

    def _calculate_cpu_activity(self, pid, utime, stime, pod_name, current_time) -> Optional[float]:
        """
        CPU 활동률 계산 (이전 계산 값과 비교)
        return:
            None or CPU 활동률 (0.0 ~ 1.0): float
            이전 계산 값이 없을 경우 None 반환
        """
        if pod_name in self.previous_cpu_states:
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