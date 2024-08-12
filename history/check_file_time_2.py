import os
import sys
from datetime import datetime, timedelta

class LastUseTime():
    def __init__(self, type):
        self.filepath = self.loadfile(type)

    def loadfile(self,type):
        # 파일 경로
        if type == "history":
            self.filepath = os.path.expanduser("~/.bash_history")
        elif type == "touch":
            self.filepath = os.path.expanduser("~/.profiling/.touch.dat")
        else:
            print("command: python3 check_file_time.py <history or touch>")
            sys.exit(1)
        self.compareTime()

    def nowTime(self):
        # 현재 시스템은 utc기준
        now = datetime.now().timestamp()
        return now

    def lastTime(self):
        last = os.path.getmtime(self.filepath)
        return last

    def checkTimestamp(self, time):
        '''
        운영제체 별로 날짜/시간을 표현하는 방식이 다르며 유닉스와 리눅스는 1970-01-01 00:00:00부터 현재 시간까지의 초를 누적한 시간을 사용
        이를 읽기 쉽도록 변환해줘야함
        '''
        time = datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')
        return time

    def convertTime(self, time):
        time = timedelta(seconds=time)
        second = time.seconds
        hour = second // 3600
        second %= 3600
        minute = second // 60
        second %= 60
        return hour, minute, second

    def convertDay(self, time):
        time = timedelta(seconds=time)
        day = time.days
        year = day // 365
        day %= 365
        month = day // 30
        day %= 30
        return year, month, day

    def compareTime(self):
        print("Compare....")
        nowTime = self.nowTime()
        lastTime = self.lastTime()
        print(f"Now: {self.checkTimestamp(nowTime)}")
        print(f"Last Time: {self.checkTimestamp(lastTime)}")

        diffTime = nowTime - lastTime

        year, month, day = self.convertDay(diffTime)
        hour, minute, second = self.convertTime(diffTime)
        print(f"Compare time : {year}-{month}-{day} {hour}:{minute}:{second}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("command: python3 check_file_time.py <history or touch>")
        sys.exit(1)

    lt = LastUseTime(sys.argv[1])
