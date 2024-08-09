import os
import datetime
import subprocess

history_file = os.path.expanduser("~/.bash_history")
# log_file = os.path.expanduser("~/log/history.log")

def renew_HistoryFile():
    '''
    bashrc에  PROMPT_COMMAND='history -a' 추가하면 명령어로 갱신할 필요 없음
    '''
    # os.system('history -a')
    try:
        subprocess.run(['bash', '-c', 'history -a'], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error while refreshing history: {e}")

def get_LogFile():
    # 사용자 홈 디렉토리 내의 log 폴더에 현재 시각을 포함한 이름으로 파일 저장
    log_file = MakeLogFile(index='history', extension='log', hms_option=True)
    log_file_path = os.path.expanduser(f"~/log/{log_file}")
    return log_file_path

def MakeLogFile(index = 'file', extension = '', hms_option = False):
    # True일 경우 현재 시각도 포함하여 파일명 지정
    if hms_option == True :
        timestamp = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
    else :
        timestamp = datetime.datetime.now().strftime("%y%m%d")
    fileName = '{}_{}.{}'.format(index, timestamp, extension)
    print(fileName)
    return fileName

def save_history():
    try:
        with open(history_file, "r") as h:
            lines = h.readlines()

        recent_lines = lines[-100:]
        log_file = get_LogFile()

        with open(log_file, "w") as l:
            l.writelines(recent_lines)

        print("History saved successfully.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    save_history()