from kubernetes import client, config, stream

class CheckProcess:
    def __init__(self, api_instance, pod):
        self.v1 = api_instance
        self.pod = pod
        self.namespace = pod.metadata.namespace

    def run(self):
        self.getProcess()
        pass

    def getProcess(self):
        command = ["cat", "-c", "%Y", self.file]
        try:
            exec_command = stream.stream(self.v1.connect_get_namespaced_pod_exec,
                                         self.pod.metadata.name,
                                         self.namespace,
                                         command=command,
                                         stderr=True, stdin=False,
                                         stdout=True, tty=False)
            print(exec_command)
            return exec_command
        except Exception as e:
            print(f"occured error: {e}")
            return None

