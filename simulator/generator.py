from kubernetes import client, config, utils
import time
from garbagecollector import GarbageCollector
from multiprocessing import Process, Event

def run_gc(ns, sc):
    gc = GarbageCollector(namespace=ns, isDev=False, stop_event=sc)
    gc.manage()

class Generator:
    def __init__(self, namespace: str = 'gc-simulator'):
        config.load_kube_config()
        self.coreV1 = client.CoreV1Api()
        self.appV1 = client.AppsV1Api()
        self.namespace: str = namespace
        self.pod_list: dict = {}
        self.intervalTime = 120  # pod 생성 반복 주기
        self.times = 5  # 반복할 횟수
        self.count = 0  # 반복한 횟수
        self.active = 0  # active pods
        self.idle = 0  # idle pods
        self.running = 0  # running pods
        self.bg_active = 0  # background active pods
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
                                "name": "PROCESS_STATE",
                                "value": "active"
                            },
                            {
                                "name": "NUM_PROCS",
                                "value": "1"
                            },
                            {
                                "name": "PROCESS_MIX",
                                "value": "single"
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
                self.running = 0
                self.bg_active = 0


                print(f"\n\n======Start {self.count+1}======\n")

                while cnt < 5:  # 120초마다 pod 생성, 총 10분동안 진행 (5회)
                    print(f"\n---create pod {cnt+1} times---")
                    self.createPod(2, 2, 2, 2)  # active, idle, running, bg
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
            self.deletePod()
        finally:
            # simulator end(or except) and gc stop
            if self.gc_process.is_alive():
                self.stop_event.set()
                self.gc_process.join()

    def createPod(self, ac, idle, run, bg):
        """
        Create pod
        """
        #active pod
        ac += self.active  # 현재 파드의 수 + 생성할 파드의 수
        while self.active < ac:
            self.pod_manifest['metadata']['name'] = 'experiment-active-'+str(self.active)
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'PROCESS_STATE')['value'] = 'active'
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'NUM_PROCS')['value'] = '3'

            self.coreV1.create_namespaced_pod(namespace=self.namespace, body=self.pod_manifest)
            print('active pod', self.active, ' created')
            self.active += 1

        # idle pod
        idle += self.idle
        while self.idle < idle:
            self.pod_manifest['metadata']['name'] = 'experiment-idle-' + str(self.idle)
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'PROCESS_STATE')['value'] = 'idle'
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'NUM_PROCS')['value'] = '1'

            self.coreV1.create_namespaced_pod(namespace=self.namespace, body=self.pod_manifest)
            print('idle pod', self.idle, ' created')
            self.idle += 1

        # running pod
        run += self.running
        while self.running < run:
            self.pod_manifest['metadata']['name'] = 'experiment-running-' + str(self.running)
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'PROCESS_STATE')['value'] = 'running'
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'NUM_PROCS')['value'] = '2'

            self.coreV1.create_namespaced_pod(namespace=self.namespace, body=self.pod_manifest)
            print('running pod', self.running, ' created')
            self.running += 1

        # background active pod
        bg += self.bg_active
        while self.bg_active < bg:
            self.pod_manifest['metadata']['name'] = 'experiment-running-' + str(self.bg_active)
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'PROCESS_STATE')['value'] = 'background_active'
            next(env for env in self.pod_manifest['spec']['containers'][0]['env'] if env['name'] == 'NUM_PROCS')['value'] = '2'

            self.coreV1.create_namespaced_pod(namespace=self.namespace, body=self.pod_manifest)
            print('background active pod', self.bg_active, ' created')
            self.bg_active += 1

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
    generator = Generator()
    generator.run()
    # generator.deletePod()
    # while True:
    #     if generator.checkStatus():
    #         break
    #     print("Deleting pod ------")
    #     time.sleep(1)
    # generator.createPod()