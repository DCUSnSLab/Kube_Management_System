from kubernetes import client, config, utils
import time
from garbagecollector import GarbageCollector
from multiprocessing import Process, Event

def run_gc(ns, sc):
    gc = GarbageCollector(namespace=ns, isDev=False, stop_event=sc)
    gc.manage()

class Simulator:
    def __init__(self, namespace: str = 'gc-simulator'):
        config.load_kube_config()
        self.coreV1 = client.CoreV1Api()
        self.appV1 = client.AppsV1Api()
        self.namespace: str = namespace
        self.pod_list: dict = {}
        self.intervalTime = 120  # pod 생성 반복 주기
        self.times = 1  # 반복할 횟수
        self.count = 0  # 반복한 횟수
        self.active = 0  # active pods
        self.idle = 0  # idle pods
        self.mixed = 0  # mixed pods
        self.stop_event = Event()
        self.gc_process = Process(target=run_gc, args=(self.namespace, self.stop_event))  # 시뮬레이터와 동시 수행을 위해 멀티프로세싱 사용

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
        self.gc_process.start()
        try:
            while self.count < self.times:
                # 실험 시작 전 카운트 횟수 및 파드 수 초기화
                cnt = 0
                self.active = 0
                self.idle = 0
                self.mixed = 0

                print(f"\n\n======Start {self.count+1}======\n")

                while cnt < 1:  # 120초마다 pod 생성
                    print(f"\n---create pod {cnt+1} times---")
                    self.createPod(10, 6, 6)  # active 50, idle 30, mixed 30
                    time.sleep(self.intervalTime)
                    cnt += 1

                # 실험 한 사이클 종료 후 pod 모두 삭제
                self.deletePod()
                while True:  # check delete pod status
                    if self.checkStatus():
                        break
                    print("Deleting pod ------")
                    time.sleep(1)
                self.count += 1

        except KeyboardInterrupt:
            print("Keyboard Interrupted. Cleanning up...")
        finally:
            # simulator end(or except) and gc stop
            if self.gc_process.is_alive():
                self.stop_event.set()
                self.gc_process.join()

            self.deletePod()

    def createPod(self, ac, i, mx):
        """
        Create pod
        """
        #active pod
        ac += self.active  # 현재 파드의 수 + 생성할 파드의 수
        while self.active < ac:
            self.pod_manifest['metadata']['name'] = 'experiment-active-'+str(self.active)
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'MODE')['value'] = 'active'
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'NUM_PROCS')['value'] = '3'

            self.coreV1.create_namespaced_pod(namespace=self.namespace, body=self.pod_manifest)
            print('active pod', self.active, ' created')
            self.active += 1

        # idle pod
        i += self.idle
        while self.idle < i:
            self.pod_manifest['metadata']['name'] = 'experiment-idle-' + str(self.idle)
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'MODE')['value'] = 'idle'
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'NUM_PROCS')['value'] = '1'

            self.coreV1.create_namespaced_pod(namespace=self.namespace, body=self.pod_manifest)
            print('idle pod', self.idle, ' created')
            self.idle += 1

        # mixed pod
        mx += self.mixed
        while self.mixed < mx:
            self.pod_manifest['metadata']['name'] = 'experiment-mixed-' + str(self.mixed)
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'MODE')['value'] = 'mixed'
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'NUM_PROCS')['value'] = '3'

            self.coreV1.create_namespaced_pod(namespace=self.namespace, body=self.pod_manifest)
            print('mixed pod', self.mixed, ' created')
            self.mixed += 1

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
        if not pods:
            self.pod_list = {}
            return

        for p in pods:
            p_name = p.metadata.name
            self.pod_list[p_name] = p

    def checkStatus(self):
        """
        Pod가 모두 삭제되었는지 확인하는 함수
        """
        pods = self.coreV1.list_namespaced_pod(self.namespace).items
        if not pods:  # No pod = clear to delete pods
            print("Delete all pods")
            return True
        else:
            return False

if __name__ == "__main__":
    #네임스페이스 값을 비워두면 'default'로 지정
    simulator = Simulator()
    simulator.run()
    # simulator.deletePod()
    # while True:
    #     if simulator.checkStatus():
    #         break
    #     print("Deleting pod ------")
    #     time.sleep(1)
    # simulator.createPod()