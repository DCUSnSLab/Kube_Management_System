from kubernetes import client, config
from kubernetes.stream import stream
from pod import Pod
# from processDB import initialize_database
from DB_postgresql import initialize_database, is_deleted_in_DB, is_exist_in_DB

from datetime import datetime
import time

class GarbageCollector():
    def __init__(self, namespace='default', container=None, isDev=False):
        config.load_kube_config()  # 필수 config값 불러옴
        self.v1 = client.CoreV1Api()  # api
        self.namespace = namespace
        self.container = container
        self.devMode = isDev
        self.exclude = ["ssh-wldnjs269", "ssh-marsberry", "swlabssh"]
        self.podlist = {}
        self.intervalTime = 60
        self.count = 1
        self.running = False

    # def manage(self):
    #     if self.devMode is True:
    #         self.namespace = 'swlabpods-gc'
    #     self.listPods()
    #
    #     for p_name, p_obj in self.podlist.items():
    #         print(p_name)
    #         p_obj.getResultHistory()
    #
    #         p_obj.insertProcessData()
    #         # p.getResultProcess()

    def manage(self):
        if self.devMode is True:
            self.namespace = 'swlabpods-gc'

        self.running = True

        while self.running:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"{timestamp} Update Pod List...")
            self.listPods()
            print('='*10+f"Start to Check Process Data {self.count} times"+'='*10)
            for p_name, p_obj in self.podlist.items():
                print(p_name)
                p_obj.insertProcessData()
                p_obj.insert_Pod_Info()
                result_history = p_obj.getResultHistory()

                # save logging data
                # p_obj.saveDataToCSV()
                p_obj.saveProcessDataToDB()

                if not result_history and p_obj.checkCreateTime():  # 7일이상 사용하지않으면 false 반환
                    p_obj.insert_DeleteReason('GC_h')
                    p_obj.save_DeleteReason_to_DB()
                    self.deletePod(p_name)  # pod 삭제

                print('-' * 50)

            print("Clear!!\n\n")
            self.count+=1
            time.sleep(self.intervalTime)

    def listPods(self):
        #현재 네임스테이스의 Pod 목록을 가져옴
        pods = self.v1.list_namespaced_pod(self.namespace).items
        if not pods:
            print(f"No resources found in {self.namespace} namespace.")
            self.podlist = {}
            return

        #제외할 pod 필터링
        filtering_pods = [
            pod for pod in pods
            if not any(
                pod.metadata.name == name or pod.metadata.name.startswith(name)
                for name in self.exclude
            )
        ]
        new_podlist = {}
        for p in filtering_pods:
            pod_name = p.metadata.name
            if pod_name in self.podlist:
                #기존 Pod객체 재사용
                new_podlist[pod_name] = self.podlist[pod_name]
            else:
                new_podlist[pod_name] = Pod(self.v1, p)
                pod_obj = new_podlist[pod_name]

                if not pod_obj.is_exist_in_DB() or pod_obj.is_deleted_in_DB():
                    print(f"Initializing new pod: {pod_name}")
                    pod_obj.init_pod_data()

        removed_pod = set(self.podlist.keys()) - set(new_podlist.keys())

        for rm_p in removed_pod:
            pod_obj = self.podlist[rm_p]
            if not pod_obj.is_deleted_in_DB():  # DB에 삭제된 시간이 없는 경우만 처리
                pod_obj.insert_DeleteReason('UNKNOWN')  # 삭제 사유 기록
                pod_obj.save_DeleteReason_to_DB()
            print(f"Pod removed: {rm_p}")

        # 새로운 목록으로 변경
        self.podlist = new_podlist

    def deletePod(self, p_name):
        print(p_name, "______REMOVE____")
        self.v1.delete_namespaced_pod(p_name, self.namespace)

    def stop(self):
        """스레드 종료를 위해 추가함"""
        self.running = False

if __name__ == "__main__":
    # initialize_database()  # DB 초기화 (sqlite)
    initialize_database()  # PostgreSQL DB 초기화

    #네임스페이스 값을 비워두면 'default'로 지정
    gc = GarbageCollector(namespace='swlabpods', isDev=False)
    gc.manage()
    # gc.logging()
