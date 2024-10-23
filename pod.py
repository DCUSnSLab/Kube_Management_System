from checkHistory import CheckHistory
from checkProcess import CheckProcess

class Pod():
    def __init__(self, api, pod):
        self.api = api
        self.pod = pod
        self.pod_name = pod.metadata.name
        self.namespace = pod.metadata.namespace

    def getResultHistory(self):
        #히스토리를 들고오도록 만듫고, manage에서 비교결과값 가져오도록 수정
        ch = CheckHistory(self.api, self.pod, self.namespace)
        return ch.getResult()

    def getResultProcess(self):
        #/proc/[pid]/stat 값을 가져오거나 ps 명령어를 활용
        pass