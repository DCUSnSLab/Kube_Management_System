import os
from datetime import datetime, timedelta
from kubernetes import client, config, stream

class CheckHistory():
    def __init__(self, api_instance, pod, namespace):
        self.file = "/home/dcuuser/.bash_history"
        self.v1 = api_instance
        self.pod = pod
        self.namespace = namespace

    def getNowTime(self):
        # 현재 시스템은 utc기준
        now = datetime.now().timestamp()
        return now

    def getLastUseTime(self):
        # last = os.path.getmtime(self.file)
        # 유닉스
        command = ["stat", "-c", "%Y", self.file]
        exec_command = stream.stream(self.v1.connect_get_namespaced_pod_exec,
                                     self.pod.metadata.name,
                                     self.namespace,
                                     command=command,
                                     stderr=True, stdin=False,
                                     stdout=True, tty=False)
        last = int(exec_command.strip())
        print(last)
        return last

    def checkTimestamp(self, time):
        '''
        운영제체 별로 날짜/시간을 표현하는 방식이 다르며 유닉스와 리눅스는 1970-01-01 00:00:00부터 현재 시간까지의 초를 누적한 시간을 사용
        이를 읽기 쉽도록 변환해줘야함
        '''
        time = datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')
        return time

    def convertDay(self, time):
        time = timedelta(seconds=time)
        day = time.days
        year = day // 365
        day %= 365
        month = day // 30
        day %= 30
        return year, month, day

    def convertTime(self, time):
        time = timedelta(seconds=time)
        second = time.seconds
        hour = second // 3600
        second %= 3600
        minute = second // 60
        second %= 60
        return hour, minute, second

    def compareTime(self):
        print("Compare....")
        now_time = self.getNowTime()
        last_time = self.getLastUseTime()
        # print(f"Now: {self.checkTimestamp(now_time)}")
        # print(f"Last Time: {self.checkTimestamp(last_time)}")

        diff_time = now_time - last_time

        year, month, day = self.convertDay(diff_time)
        hour, minute, second = self.convertTime(diff_time)
        print(f"Compare time : {year}-{month}-{day} {hour}:{minute}:{second}")
        return year, month, day

    def getResult(self):
        y, m, d = self.compareTime()
        if y > 0 or m > 0:
            return False
        if d > 7:
            return False
        else:
            return True
