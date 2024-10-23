from kubernetes import client, config
from kubernetes.stream import stream
from checkHistory import CheckHistory

class Pod():
    def __init__(self, api, pod):
        self.api = api
        self.pod = pod
        self.pod_name = pod.metadata.name
        self.namespace = pod.metadata.namespace

    def getHistory(self):
        #히스토리를 들고오도록 만듫고, manage에서 비교결과값 가져오도록 수정
        ch = CheckHistory(self.api, self.pod, self.namespace)
        return ch.getResult()

    def getProcess(self):
        #/proc/[pid]/stat 값을 가져오거나 ps 명령어를 활용
        pass

class GarbageCollector():
    def __init__(self, namespace='default', container=None, isDev=False):
        config.load_kube_config()  # 필수 config값 불러옴
        self.v1 = client.CoreV1Api()  # api
        self.namespace = namespace
        self.container = container
        self.devMode = isDev
        self.exclude = ["ssh-wldnjs269", "swlabssh"]
        self.podlist = []

    def manage(self):
        if self.devMode is True:
            self.namespace = 'swlabpods-gc'
            self.listPods()
        else:
            self.listPods()

        for p in self.podlist:
            print(p.pod_name)
            print(p.getHistory())

    def listPods(self):
        pods = self.v1.list_namespaced_pod(self.namespace).items
        if not pods:
            print(f"No resources found in {self.namespace} namespace.")
            return
        #제외할 pod 필터링
        filtering_pods = [
            pod for pod in pods
            if not any(
                pod.metadata.name == name or pod.metadata.name.startswith(name)
                for name in self.exclude
            )
        ]
        for p in filtering_pods:
            pod_instance = Pod(self.v1, p)
            self.podlist.append(pod_instance)

    def execTest(self, pod):
        #exec test
        command = ["ls", "-al", ".bash_history"]
        exec_commmand = stream.stream(self.v1.connect_get_namespaced_pod_exec,
                                      name=pod.name,
                                      namespace=self.namespace,
                                      command=command,
                                      stdout=True, stdin=False, stderr=True, tty=False)
        print(exec_commmand)

    def checkStatus(self, pod):
        pass
        #true = 사용, false = idle
        # if not result:
        #     print(f"Not used for more than 7 days.\nDelete pod {pod.metadata.name} now.\n" + "-" * 50)
        #     self.deletePod(pod)
        # else:
        #     print(f"Pod {pod.metadata.name} is running.\n" + "-" * 50)

    def deletePod(self, pod):
        pod_name = pod.metadata.name
        self.v1.delete_namespaced_pod(pod_name, self.namespace)

if __name__ == "__main__":
    #네임스페이스 값을 비워두면 'default'로 지정
    gc = GarbageCollector(namespace='swlabpods', isDev=True)
    gc.manage()