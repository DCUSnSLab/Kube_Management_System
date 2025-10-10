import os
import sys
import time
import signal
import random
import math
import tempfile

"""
Hybrid Active Scenario - CPU + I/O Mixed (Extended I/O Duration +  Idle)
CPU 집중 연산과 파일 I/O, 그리고 여유 있는 Idle 구간을 포함한 복합 부하 시나리오
"""


def signal_handler(sig, frame):
    print("\n[HYBRID] Process terminated")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def cpu_burst(intensity=800000):
    """CPU 집중 연산"""
    result = 0
    for i in range(intensity):
        result += math.sqrt(i) * math.sin(i)
    return result


def io_activity(work_dir, iteration, duration=3.0):
    """
    I/O 활동 시뮬레이션:
    - 여러 개의 파일 생성 및 쓰기
    - 일부 파일 다시 읽기
    - 총 duration 동안 반복 수행
    """
    start = time.time()
    files_written = 0

    while time.time() - start < duration:
        filename = os.path.join(work_dir, f"hybrid_{iteration}_{files_written}.log")
        with open(filename, "w") as f:
            lines = random.randint(200, 600)
            for _ in range(lines):
                f.write("Hybrid workload active\n")
            f.flush()
            os.fsync(f.fileno())

        # 파일 읽기 시뮬레이션 (디스크 캐시 효과 반영)
        if random.random() < 0.5:
            with open(filename, "r") as f:
                _ = f.readline()

        files_written += 1
        time.sleep(random.uniform(0.2, 0.5))

    print(f"[HYBRID] I/O activity: {files_written} files written (≈ {duration:.1f}s)", flush=True)


def main():
    print(f"[HYBRID] Mixed CPU+IO Process Started - PID: {os.getpid()}", flush=True)
    work_dir = tempfile.mkdtemp(prefix="hybrid_test_")
    iteration = 0

    while True:
        iteration += 1
        print(f"[HYBRID] Iteration {iteration} - CPU burst phase", flush=True)

        # CPU 부하 단계
        for _ in range(random.randint(3, 6)):
            cpu_burst(random.randint(200000, 500000))

        # I/O 부하 단계
        if random.random() < 0.7:
            io_time = random.uniform(2.5, 5.0)
            print(f"[HYBRID] Iteration {iteration} - I/O phase (~{io_time:.1f}s)", flush=True)
            io_activity(work_dir, iteration, duration=io_time)

        # Idle 단계 (조금 더 길게)
        idle = random.uniform(3.0, 10.0)
        print(f"[HYBRID] Idle {idle:.1f}s", flush=True)
        time.sleep(idle)


if __name__ == "__main__":
    main()