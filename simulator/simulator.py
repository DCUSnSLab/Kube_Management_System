from kubernetes import client, config, utils
from os import path
import yaml

class Simulator:
    def __init__(self, namespace: str = 'gc-simulator'):
        config.load_kube_config()
        self.coreV1 = client.CoreV1Api()
        self.appV1 = client.AppsV1Api()
        self.namespace = namespace
        self.pod_list = {}
        self.intervalTime = 120  # pod 생성 반복 주기
        self.times = 5  # 반복할 횟수
        self.count = 0  # 반복한 횟수

        self.pod_manifest = {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "experiment-pod"
            },
            "spec": {
                "containers": [
                    {
                        "name": "experiment-container",
                        "image": "harbor.cu.ac.kr/swlabpods/gcpod:latest",
                        "env": [
                            {
                                "name": "MODE",
                                "value": "active"
                            },
                            {
                                "name": "NUM_PROCS",
                                "value": "1"
                            }
                        ]
                    }
                ]
            }
        }

    def run(self):
        """
        Run simulation
        """
        while self.count < self.times:
            pass


    def createPod(self):
        """
        Create pod
        """
        cnt = 0

        #active pod
        while cnt < 5:
            self.pod_manifest['metadata']['name'] = 'experiment-active-'+str(cnt)
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'MODE')['value'] = 'active'
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'NUM_PROCS')['value'] = '2'

            # with open(path.join(path.dirname(__file__), "../deploy/simulation/nginx-deployment.yaml")) as f:
            #     dep = yaml.safe_load(f)
            #     resp = self.appV1.create_namespaced_deployment(
            #         body=dep, namespace=self.namespace)
            #     print(f"Deployment created. Status='{resp.metadata.name}'")

            dir_yaml = "../deploy/simulation/create-testpod.yaml"
            self.coreV1.create_namespaced_pod(namespace=self.namespace, body=self.pod_manifest)
            # utils.create_from_yaml(apiclient, dir_yaml, namespace=self.namespace, verbose=True)
            print('pod',cnt,' created')
            cnt += 1

        cnt = 0
        # idle pod
        while cnt < 5:
            self.pod_manifest['metadata']['name'] = 'experiment-idle-' + str(cnt)
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'MODE')['value'] = 'idle'
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'NUM_PROCS')['value'] = '2'

            self.coreV1.create_namespaced_pod(namespace=self.namespace, body=self.pod_manifest)
            print('pod', cnt, ' created')
            cnt += 1

    def deletePod(self):
        """
        Delete all pod
        """
        self.getPodList()
        for p in self.pod_list:
            self.coreV1.delete_namespaced_pod(p, self.namespace)

    def getPodList(self):
        """
        Get pod list in namespace
        """
        pods = self.coreV1.list_namespaced_pod(self.namespace).items
        for p in pods:
            p_name = p.metadata.name
            self.pod_list[p_name] = p

if __name__ == "__main__":
    #네임스페이스 값을 비워두면 'default'로 지정
    gc = Simulator()
    # gc.run()
    # gc.deletePod()
    gc.createPod()