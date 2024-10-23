from kubernetes import client, config, stream

class CheckProcess:
    def __init__(self, api_instance, pod, namespace):
        self.v1 = api_instance
        self.pod = pod

    def getProcess(self):
        pass

    def getStatus(self):
        pass
