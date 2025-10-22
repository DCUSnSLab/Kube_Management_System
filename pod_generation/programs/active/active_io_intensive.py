import os
import sys
import time
import signal
import tempfile
import random

"""
Active Process - I/O Intensive 
(with Random File Size and Read/Write Ratio)
"""

def signal_handler(sig, frame):
    print("\nI/O intensive process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def simulate_io_activity(work_dir, iteration):
    """파일 I/O 시뮬레이션 (크기, 읽기/쓰기 비율 난수화)"""
    file_size_kb = random.randint(100, 2000)  # 100KB ~ 2MB
    write_ratio = random.uniform(0.4, 0.9)    # 40~90% 쓰기 중심
    read_ratio = 1 - write_ratio

    filename = os.path.join(work_dir, f"io_file_{iteration}.dat")

    # 쓰기 작업
    if random.random() < write_ratio:
        with open(filename, "wb") as f:
            f.write(os.urandom(file_size_kb * 1024))
            f.flush()
            os.fsync(f.fileno())
        print(f"[I/O] Wrote {file_size_kb}KB to {filename} (write_ratio={write_ratio:.2f})", flush=True)
        time.sleep(random.uniform(0.05, 0.2))

    # 읽기 작업
    if os.path.exists(filename) and random.random() < read_ratio:
        with open(filename, "rb") as f:
            _ = f.read(random.randint(1024, 8192))
        print(f"[I/O] Read from {filename} (read_ratio={read_ratio:.2f})", flush=True)

def main():
    print(f"[ACTIVE-IO] Randomized File I/O Simulation Started - PID: {os.getpid()}", flush=True)
    temp_dir = tempfile.mkdtemp(prefix="io_random_")
    iteration = 0

    while True:
        iteration += 1
        simulate_io_activity(temp_dir, iteration)
        if iteration % 10 == 0:
            print(f"[ACTIVE-IO-STATUS] Workspace files: {len(os.listdir(temp_dir))}", flush=True)
        time.sleep(random.uniform(0.2, 0.8))

if __name__ == "__main__":
    main()
