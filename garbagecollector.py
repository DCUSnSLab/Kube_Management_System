from kubernetes import client, config
from history.checkHistory import CheckHistory
class GarbageCollector():
    def __init__(self, namespace='default'):
        config.load_kube_config()  # 필수 config값 불러옴
        self.v1 = client.CoreV1Api()  # api
        self.namespace = namespace
        self.exclude = ["ssh-wldnjs269", "swlabssh"]

    def listPods(self):
        list_pods = self.v1.list_namespaced_pod(self.namespace)
        for pod in list_pods.items:
            if pod.metadata.name not in self.exclude:
                print(pod.metadata.name)
    def checkStatus(self, pod_name):
        #exec
        command = ["ls", "/hosme/dcuuser"]
        #bash history 확인?
        #결과값 리턴
        return 0

    def deletePod(self, pod_name):
        self.v1.delete_namespaced_pod(self.namespace)



if __name__ == "__main__":
    #swlabpods 네임스페이스
    gc=GarbageCollector(namespace='swlabpods')
    gc.listPods()