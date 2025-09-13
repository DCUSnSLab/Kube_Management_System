import os
import time
import sys
import signal
import random

"""
Inactive Process - Idle Shell/Terminal Session
유휴 상태의 쉘 세션 (학생이 접속만 하고 대기 중)
"""

def signal_handler(sig, frame):
    print("\nIdle process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def simulate_shell_prompt():
    """쉘 프롬프트 대기 상태 시뮬레이션"""
    # bash/sh 프롬프트가 입력을 기다리는 상태
    # CPU를 거의 사용하지 않음
    return f"student@linux:~$ "

def simulate_terminal_check():
    """터미널 상태 체크 (가끔 터미널이 살아있는지 확인)"""
    # tty 상태 확인 시뮬레이션
    tty_status = "pts/0"
    return tty_status

def simulate_idle_session():
    """유휴 SSH/터미널 세션 시뮬레이션"""
    # 학생이 접속하고 아무 명령도 입력하지 않는 상태
    session_info = {
        "user": "student",
        "tty": "pts/0",
        "idle_time": 0,
        "command": "bash"
    }
    return session_info

def main():
    print(f"[IDLE-SHELL] Idle Shell Session Started - PID: {os.getpid()}", flush=True)
    print(f"[IDLE-SHELL] Student logged in but idle (bash waiting for input)", flush=True)
    
    # 초기 프롬프트 표시
    prompt = simulate_shell_prompt()
    print(f"{prompt}", end='', flush=True)
    
    heartbeat = 0
    idle_start = time.time()
    last_activity = time.time()
    
    while True:
        heartbeat += 1
        
        # 대부분의 시간은 아무것도 하지 않음 (S - Sleeping 상태)
        # bash가 입력을 기다리는 상태를 시뮬레이션
        
        # 매우 가끔 터미널 상태 확인 (세션이 살아있는지)
        if heartbeat % 600 == 0:  # 10분마다
            tty = simulate_terminal_check()
            current_time = time.time()
            idle_time = current_time - idle_start
            
            # 유휴 상태 정보 출력
            print(f"\n[IDLE-STATUS] Session idle for {idle_time/60:.1f} minutes", flush=True)
            print(f"[IDLE-STATUS] Process State: S (Sleeping), waiting for input", flush=True)
            print(f"[IDLE-STATUS] CPU usage: 0.0%, Memory: minimal", flush=True)
            print(f"{prompt}", end='', flush=True)
            
            # 세션 정보 업데이트
            session = simulate_idle_session()
            session["idle_time"] = idle_time
        
        # 아주 가끔 미세한 활동 (커서 깜빡임 등)
        if heartbeat % 100 == 0:
            # 커서만 깜빡임 (실제 CPU 사용 거의 없음)
            pass
        
        # Sleeping 상태로 대기 (CPU 사용률 0%에 가까움)
        # /proc/[pid]/stat에서 state = 'S', utime/stime 변화 없음
        time.sleep(10)  # 긴 대기 시간

if __name__ == "__main__":
    main()
