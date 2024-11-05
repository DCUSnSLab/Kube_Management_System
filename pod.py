from Kube_Management_System.process import Process
from checkHistory import CheckHistory
from checkProcess import CheckProcess

class Pod():
    def __init__(self, api, pod):
        self.api = api
        self.pod = pod
        self.pod_name = pod.metadata.name
        self.namespace = pod.metadata.namespace
        self.processes = list()
        p = Process()
        p.PID = 1
        p.ppid = 2
        self.processes.append(p)

    def getResultHistory(self):
        # manage에서 비교결과값을 가져오도록
        ch = CheckHistory(self.api, self.pod)
        chResult = ch.run()
        print(chResult)
        return chResult

    def getResultProcess(self):
        #/proc/[pid]/stat 값을 가져오거나 ps 명령어를 활용
        cp = CheckProcess(self.api, self.pod)
        cpResult = cp.run()
        print(cpResult)
        return cpResult

    def saveData(self):
        pass

    def printProcList(self):
        for p in self.processes:
            print(p.PID, p.ppid)