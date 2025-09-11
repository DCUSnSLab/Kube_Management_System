import time
import sys
import signal
import os

"""
Inactive Process - Sleeping
대부분의 시간을 sleep 상태로 보내는 비활성 프로세스
"""

def signal_handler(sig, frame):
    print("\nSleeping process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def main():
    print(f"[INACTIVE] Sleeping Process Started - PID: {os.getpid()}", flush=True)
    
    wake_count = 0
    
    while True:
        wake_count += 1
        
        # 아주 가끔 깨어나서 최소한의 작업
        if wake_count % 100 == 0:
            print(f"[INACTIVE] Still alive... (wake #{wake_count})", flush=True)
        
        # 대부분의 시간을 sleep
        time.sleep(30)  # 30초 sleep

if __name__ == "__main__":
    main()
