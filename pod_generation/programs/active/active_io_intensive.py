#!/usr/bin/env python3
"""
Active Process - I/O Intensive
"""
import os, sys, time, signal, tempfile, random

def signal_handler(sig, frame):
    print("\nI/O intensive process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def main():
    print(f"[ACTIVE] I/O Intensive Process Started - PID: {os.getpid()}", flush=True)
    temp_dir = tempfile.mkdtemp(prefix="active_io_")
    print(f"[ACTIVE] Working directory: {temp_dir}", flush=True)
    iteration, file_count = 0, 0
    while True:
        iteration += 1
        filename = os.path.join(temp_dir, f"data_{file_count % 10}.txt")
        with open(filename, 'w') as f:
            data = ''.join(str(random.randint(0, 9)) for _ in range(10000))
            f.write(f"Iteration {iteration}: {data}\n")
            f.flush()
            os.fsync(f.fileno())
        if file_count > 0:
            read_file = os.path.join(temp_dir, f"data_{(file_count - 1) % 10}.txt")
            if os.path.exists(read_file):
                with open(read_file, 'r') as f:
                    _ = f.read()
        file_count += 1
        if iteration % 5 == 0:
            print(f"[ACTIVE] I/O Operations: {iteration}", flush=True)
        time.sleep(0.5)

if __name__ == "__main__":
    main()
