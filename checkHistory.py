import os
from datetime import datetime, timedelta
from kubernetes import client, config, stream

class CheckHistory():
    def __init__(self, api_instance, pod):
        self.file = "/home/dcuuser/.bash_history"
        self.v1 = api_instance
        self.pod = pod
        self.namespace = pod.metadata.namespace

    def run(self, filetime):
        # 사용하지않는다고 판단하면 false
        # filetime = self.getLastUseTime()
        if filetime == None:
            # file이 없는경우
            # 접속을 했으나, 사용중이거나 제대로 종료하지않으면 파일이 없음
            return True
        y, m, d = self.compareTime(filetime)
        # 7일이상 경과 시 False
        if y > 0 or m > 0:
            return False
        if d > 7:
            return False
        else:
            return True

    def getLastUseTime(self):
        # last = os.path.getmtime(self.file)
        # 유닉스
        command = ["stat", "-c", "%Y", self.file]
        try:
            exec_command = stream.stream(self.v1.connect_get_namespaced_pod_exec,
                                         self.pod.metadata.name,
                                         self.namespace,
                                         command=command,
                                         stderr=True, stdin=False,
                                         stdout=True, tty=False)
            last = int(exec_command.strip())
            return last
        except FileNotFoundError as e:
            print("File not found")
            return None
        except Exception as e:
            print(f"occured error: {e}")
            return None

    def getNowTime(self):
        # 현재 시스템은 utc기준
        now = datetime.now().timestamp()
        return now

    def compareTime(self, last_time):
        # print("Compare....")
        now_time = self.getNowTime()
        diff_time = now_time - last_time

        year, month, day = self.convertDay(diff_time)
        hour, minute, second = self.convertTime(diff_time)
        # print('-' * 50)
        #print(f"Compare time : {year}-{month}-{day} {hour}:{minute}:{second}")
        return year, month, day

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

    def checkTimestamp(self, time):
        '''
        운영제체 별로 날짜/시간을 표현하는 방식이 다르며 유닉스와 리눅스는 1970-01-01 00:00:00부터 현재 시간까지의 초를 누적한 시간을 사용
        이를 읽기 쉽도록 변환해줘야함
        '''
        time = datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')
        return time
