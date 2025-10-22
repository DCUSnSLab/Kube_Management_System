import os
import sys
import time
import signal
import random
import math
import tempfile

"""
Idle Periodic Activity - 간헐적 사용자 활동 시뮬레이션 (학생 명령 실행)
장시간 유휴 상태 유지 중, 주기적으로 짧은 CPU/I-O 작업 발생
"""


def signal_handler(sig, frame):
    print("\n[IDLE] Process terminated")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def simulate_user_command():
    """간단한 명령어 실행 시뮬레이션"""
    commands = ["ls", "cat report.txt", "gcc main.c", "python test.py", "tar -czf backup.tar.gz ./src"]
    cmd = random.choice(commands)
    print(f"[IDLE] User executed: {cmd}", flush=True)

    # CPU 연산 (짧은 연산)
    for _ in range(random.randint(50000, 150000)):
        _ = math.sqrt(random.random() * 1000)

    # 가끔 파일 쓰기
    if random.random() < 0.3:
        tmpfile = os.path.join(tempfile.gettempdir(), f"idle_tmp_{random.randint(1000, 9999)}.log")
        with open(tmpfile, 'w') as f:
            f.write("Short user activity log\n" * 10)
            f.flush()
            os.fsync(f.fileno())


def main():
    print(f"[IDLE] Periodic Idle Activity Started - PID: {os.getpid()}", flush=True)
    intervals = [300, 600, 900, 1200, 1500, 1800]  # 5, 10, 15, 20, 25, 30분
    iteration = 0

    while True:
        interval = random.choice(intervals)
        print(f"[IDLE] Sleeping for {interval / 60:.1f} minutes...", flush=True)
        time.sleep(interval)

        iteration += 1
        print(f"[IDLE] Activity burst #{iteration}", flush=True)
        simulate_user_command()

        # 활동 후 짧은 여유
        time.sleep(random.uniform(1.0, 3.0))
        print(f"[IDLE] Back to idle state", flush=True)


if __name__ == "__main__":
    main()
