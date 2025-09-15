from time import sleep

from kubernetes import client, config, stream
from processManager import ProcessManager
from pod import Pod

class DummyPod:
    name = "ssh-wldnjs269"
    namespace = "swlabpods"

class TestProcessManager:
    def __init__(self, coreV1):
        self.coreV1 = coreV1
        self.namespace = "swlabpods"

    def testPM(self):
        pods = self.coreV1.list_namespaced_pod(self.namespace).items
        time = 0
        manager = {}

        while True:
            if time == 3:
                break
            cnt = 0
            for p in pods:
                if cnt == 3:
                    break
                print("\n\n\n",p.metadata.name)
                if p.metadata.name not in manager:
                    manager[p.metadata.name] = ProcessManager(self.coreV1, p)
                pod = Pod(coreV1, p)
                pod.insertProcessData()

                pm = manager[p.metadata.name]
                pm.analyze(pod.processes)

                cnt += 1
            time += 1
            sleep(30)



if __name__ == "__main__":
    config.load_kube_config()
    coreV1 = client.CoreV1Api()

    tpm = TestProcessManager(coreV1)
    tpm.testPM()
