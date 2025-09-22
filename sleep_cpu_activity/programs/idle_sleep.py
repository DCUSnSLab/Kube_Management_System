import os
import sys
import time
import signal

"""
Idle Process - Sleep 상태 시뮬레이션
거의 CPU 안 쓰고 time.sleep만 반복하는 프로세스
"""


def signal_handler(sig, frame):
    print("\nIdle sleep process terminated")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def main():
    print(f"[IDLE] Sleep Process Started - PID: {os.getpid()}", flush=True)

    iteration = 0
    while True:
        iteration += 1
        # CPU 거의 안 쓰고 Sleep만 반복
        print(f"[IDLE] Iteration {iteration} - State: S (Sleeping)", flush=True)
        time.sleep(5)


if __name__ == "__main__":
    main()
