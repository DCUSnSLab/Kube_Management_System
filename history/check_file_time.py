import os
from datetime import datetime, timedelta

class LastUseTime():
    def __init__(self, filepath):
        self.file = filepath

    def nowTime(self):
        # 현재 시스템은 utc기준
        now = datetime.now().timestamp()
        return now

    def lastTime(self):
        last = os.path.getmtime(self.file)
        return last

    def checkTimestamp(self, time):
        '''
        운영제체 별로 날짜/시간을 표현하는 방식이 다르며 유닉스와 리눅스는 1970-01-01 00:00:00부터 현재 시간까지의 초를 누적한 시간을 사용
        이를 읽기 쉽도록 변환해줘야함
        '''
        time = datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')
        print(f"convert time : {time}")
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
        print(f"Time : {nowTime - lastTime}")
        diffTime = nowTime - lastTime

        year, month, day = self.convertDay(diffTime)
        hour, minute, second = self.convertTime(diffTime)
        print(f"Compare time : {year}-{month}-{day} {hour}:{minute}:{second}")

if __name__ == "__main__":
    # 파일 경로
    touch_file = os.path.expanduser("~/.system/touch.dat")
    lt = LastUseTime(touch_file)
    lt.compareTime()