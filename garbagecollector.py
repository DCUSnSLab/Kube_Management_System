from kubernetes import client, config
from kubernetes.stream import stream
from history.checkHistory import CheckHistory

class GarbageCollector():
    def __init__(self, namespace='default', container=None, isDev=False):
        config.load_kube_config()  # 필수 config값 불러옴
        self.v1 = client.CoreV1Api()  # api
        self.namespace = namespace
        self.container = container
        self.devMode = isDev
        self.exclude = ["ssh-wldnjs269", "swlabssh"]
        self.ver_test = ["ssh-test", "ssh-stutest"]

        self.listPods()

    def listPods(self):
        if self.devMode is True:
            self.namespace = 'swlabpods-gc'
            list_pods = self.v1.list_namespaced_pod(self.namespace)
            for pod in list_pods.items:
                print(pod.metadata)
                self.checkStatus(pod)
        else:
            list_pods = self.v1.list_namespaced_pod(self.namespace)
            for pod in list_pods.items:
                if pod.metadata.name not in self.exclude[0] and not pod.metadata.name.startswith(self.exclude[1]):
                    print(pod.metadata.name)
                    self.checkStatus(pod)
    def execTest(self, pod):
        #exec
        command = ["ls", "-al", ".bash_history"]
        #bash history 확인?
        exec_commmand = stream.stream(self.v1.connect_get_namespaced_pod_exec,
                                      name=pod.name,
                                      namespace=self.namespace,
                                      command=command,
                                      stdout=True, stdin=False, stderr=True, tty=False)
        print(exec_commmand)

    def checkStatus(self, pod):
        ch = CheckHistory(self.v1, pod, self.namespace)
        result = ch.getResult()
        if not result:
            print(f"Not used for more than 7 days.\nDelete pod {pod.metadata.name} now.\n" + "-" * 50)
            self.deletePod(pod)
        else:
            print(f"Pod {pod.metadata.name} is running.\n" + "-" * 50)

    def deletePod(self, pod):
        pod_name = pod.metadata.name
        self.v1.delete_namespaced_pod(pod_name, self.namespace)



if __name__ == "__main__":
    #네임스페이스 값을 비워두면 'default'로 지정
    gc=GarbageCollector(namespace='swlabpods', isDev=True)
    gc.listPods()