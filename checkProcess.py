from kubernetes import client, config, stream

class CheckProcess:
    def __init__(self, api_instance, pod):
        self.v1 = api_instance
        self.pod = pod
        self.namespace = pod.metadata.namespace

    def run(self):
        pass

    def getProcStat(self):
        command = ["sh", "-c", "ls -d /proc/[0-9]* | xargs -I {} sh -c 'cat {}/stat 2>/dev/null'"]
        try:
            exec_command = stream.stream(self.v1.connect_get_namespaced_pod_exec,
                                         self.pod.metadata.name,
                                         self.namespace,
                                         command=command,
                                         stderr=True, stdin=False,
                                         stdout=True, tty=False)
            return exec_command
        except Exception as e:
            print(f"occured error: {e}")
            return None