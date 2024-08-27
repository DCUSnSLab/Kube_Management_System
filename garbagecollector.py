from kubernetes import client, config

class GarbageCollector():
    def __init__(self, namespace='default'):
        config.load_kube_config()  # 필수 config값 불러옴
        self.v1 = client.CoreV1Api()  # api
        self.namespace = namespace

    def listPods(self):
        list_pods = self.v1.list_namespaced_pod(self.namespace)
        return list_pods.items

    def checkStatus(self, pod_name):

    def deletePod(self, pod_name):
        self.v1.delete_namespaced_pod(self.namespace)


if __name__ == "__main__":