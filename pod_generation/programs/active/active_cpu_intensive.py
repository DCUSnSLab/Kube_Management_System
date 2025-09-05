#!/usr/bin/env python3
"""
Active Process - CPU Intensive
"""
import os, sys, time, signal

def signal_handler(sig, frame):
    print("\nCPU intensive process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def cpu_intensive_task():
    result = 0
    for i in range(1000000):
        result += i ** 2
        result = result % 1000007
    return result

def main():
    print(f"[ACTIVE] CPU Intensive Process Started - PID: {os.getpid()}", flush=True)
    iteration = 0
    while True:
        result = cpu_intensive_task()
        iteration += 1
        if iteration % 10 == 0:
            print(f"[ACTIVE] CPU Iteration {iteration}, result={result}", flush=True)

if __name__ == "__main__":
    main()
