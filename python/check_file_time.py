import os
from datetime import datetime

#파일위치
touch_file = os.path.expanduser("~/.system/touch.dat")

def check_last_time():
    '''
    운영제체 별로 날짜/시간을 표현하는 방식이 다르며 유닉스와 리눅스는 1970-01-01 00:00:00부터 현재 시간까지의 초를 누적한 시간을 사용
    이를 읽기 쉽도록 변환해줘야함
    '''
    time = os.path.getmtime(touch_file)

    time = datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')

    print(f"Last moditied time : {time}")

if __name__ == "__main__":
    check_last_time()