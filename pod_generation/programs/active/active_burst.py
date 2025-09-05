#!/usr/bin/env python3
"""
Active Process - Burst Mode
버스트 모드로 활동하는 프로세스
"""
import time
import sys
import signal
import random
import math

def signal_handler(sig, frame):
    print("\nBurst mode process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def burst_computation(intensity=1000000):
    """버스트 계산 작업"""
    result = 0
    for i in range(intensity):
        result += math.sqrt(i) * math.sin(i)
    return result

def main():
    print(f"[ACTIVE] Burst Mode Process Started - PID: {os.getpid()}", flush=True)
    
    burst_count = 0
    total_bursts = 0
    
    while True:
        # 버스트 활동 기간 (2-5초)
        burst_duration = random.uniform(2, 5)
        burst_start = time.time()
        
        print(f"[ACTIVE] Starting burst #{total_bursts + 1}", flush=True)
        
        while time.time() - burst_start < burst_duration:
            result = burst_computation(random.randint(100000, 500000))
            burst_count += 1
        
        total_bursts += 1
        print(f"[ACTIVE] Burst completed: {burst_count} operations", flush=True)
        burst_count = 0
        
        # 휴식 기간 (1-3초)
        idle_duration = random.uniform(1, 3)
        print(f"[ACTIVE] Idle for {idle_duration:.1f} seconds", flush=True)
        time.sleep(idle_duration)

if __name__ == "__main__":
    import os
    main()
