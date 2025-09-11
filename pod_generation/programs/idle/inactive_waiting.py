import time
import sys
import signal
import select
import socket
import os

"""
Inactive Process - Waiting
대기 상태를 유지하는 비활성 프로세스
"""

def signal_handler(sig, frame):
    print("\nWaiting process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def main():
    print(f"[INACTIVE] Waiting Process Started - PID: {os.getpid()}", flush=True)
    
    # 소켓 생성 (실제로 연결하지는 않음)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setblocking(False)
    
    wait_cycles = 0
    
    while True:
        wait_cycles += 1
        
        # select를 사용한 대기 (타임아웃 있음)
        try:
            ready = select.select([sock], [], [], 60)  # 60초 대기
        except:
            pass
        
        # 매우 드물게 상태 출력
        if wait_cycles % 50 == 0:
            print(f"[INACTIVE] Waiting... (cycle {wait_cycles})", flush=True)
        
        # 최소한의 CPU 사용
        time.sleep(10)

if __name__ == "__main__":
    main()
