import os
import sys
import time
import signal
import math
import random

"""
Active but momentarily Sleep 상태 시뮬레이션
CPU/IO 활동 후 짧게 sleep하는 프로세스
"""


def signal_handler(sig, frame):
    print("\nActive-with-sleep process terminated")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def cpu_task(size=200000):
    """짧은 CPU 연산"""
    result = 0
    for i in range(size):
        result += math.sqrt(i) * math.sin(i)
    return result


def main():
    print(f"[ACTIVE-SLEEP] Mixed Activity Process Started - PID: {os.getpid()}", flush=True)

    iteration = 0
    while True:
        iteration += 1
        # CPU Burst
        print(f"[ACTIVE-SLEEP] Iteration {iteration} - CPU burst running", flush=True)
        for _ in range(5):
            cpu_task(random.randint(50000, 150000))

        # 잠시 Sleep (여전히 활성 프로세스지만 state=S로 잡힐 수 있음)
        sleep_time = random.uniform(1, 3)
        print(f"[ACTIVE-SLEEP] Iteration {iteration} - Sleeping {sleep_time:.1f}s", flush=True)
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
