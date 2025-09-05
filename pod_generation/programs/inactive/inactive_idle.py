#!/usr/bin/env python3
"""
Inactive Process - Idle with Minimal Activity
최소한의 활동만 하는 유휴 프로세스
"""
import time
import sys
import signal
import random

def signal_handler(sig, frame):
    print("\nIdle process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def minimal_work():
    """최소한의 작업"""
    x = random.random()
    y = x * 2
    return y

def main():
    print(f"[INACTIVE] Idle Process Started - PID: {os.getpid()}", flush=True)
    
    heartbeat = 0
    last_activity = time.time()
    
    while True:
        heartbeat += 1
        
        # 매우 가끔씩만 최소한의 작업
        if heartbeat % 200 == 0:
            result = minimal_work()
            current_time = time.time()
            idle_time = current_time - last_activity
            print(f"[INACTIVE] Heartbeat {heartbeat}: Idle for {idle_time:.1f}s", flush=True)
            last_activity = current_time
        
        # 대부분의 시간은 유휴 상태
        time.sleep(5)

if __name__ == "__main__":
    import os
    main()
