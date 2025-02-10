from kubernetes import client, config, stream
import re

class CheckProcess:
    def __init__(self, api_instance, pod):
        self.v1 = api_instance
        self.pod = pod
        self.namespace = pod.metadata.namespace
        self.used_commands = ["xargs", "sh"]

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
            return self._filter_sh_xargs(exec_command)
        except Exception as e:
            if "Connection to remote host was lost" in str(e):
                print(f"Connection to Pod '{self.pod.metadata.name}' was lost. Skipping this Pod.")
            else:
                print(f"An unexpected error occurred: {e}")
            return None

    def _filter_sh_xargs(self, exec_command):
        """
        Exclude processes where:
        `sh` process PID == `xargs` process PPID
        """
        filtered_processes = []
        sh_pids = set()
        xargs_ppids = set()

        # 1. Traverses all processes and collects `sh` and `xargs` information
        for line in exec_command.splitlines():
            fields = line.split()
            if len(fields) > 2:
                pid = fields[0]
                comm = fields[1].strip("()")
                ppid = fields[3]

                if comm == "sh":
                    sh_pids.add(pid)
                elif comm == "xargs":
                    xargs_ppids.add(ppid)
        # 2. Check the relationship between sh and xargs to determine which PID and PPID pairs to exclude
        exclude_pairs = sh_pids.intersection(xargs_ppids)

        # 3. Include only processes that do not apply
        for line in exec_command.splitlines():
            fields = line.split()
            if len(fields) > 2:
                pid = fields[0]  # PID of the process
                ppid = fields[3]  # Parent PID (PPID)

                if not (pid in exclude_pairs or ppid in exclude_pairs):
                    filtered_processes.append(line)

        return "\n".join(filtered_processes)

    def _filter_used_commands(self, exec_command):
        """
        Specific command filter
        """
        filtered_processes = []
        if not exec_command:
            return filtered_processes

        for line in exec_command.splitlines():
            fields = line.split()
            if len(fields) > 1:
                process_name = fields[1].strip("()")
                if process_name not in self.used_commands:
                    filtered_processes.append(line)

        return "\n".join(filtered_processes)