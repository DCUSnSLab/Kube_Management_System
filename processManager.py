from kubernetes import client, config, stream
import time

class ProcessManager:
    def __init__(self, api_instance, pod):
        self.v1 = api_instance
        self.pod = pod
        self.namespace = pod.metadata.namespace
        self.used_commands: list = ["xargs", "sh", "cat", "bash"]

    def run(self):
        pass

    def getProcStat(self):
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