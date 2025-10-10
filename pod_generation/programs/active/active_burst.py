import time
import sys
import signal
import random
import math

"""
Active Process - Burst Mode
버스트 모드로 활동하는 프로세스
+ random sleep 추가 (5분~30분 간 랜덤 간격으로 휴식)
"""

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

            # 짧은 pause (micro idle) 삽입
            if random.random() < 0.2:
                pause = random.uniform(0.05, 0.2)
                time.sleep(pause)

        total_bursts += 1
        print(f"[ACTIVE] Burst completed: {burst_count} operations", flush=True)
        burst_count = 0

        # 장시간 idle 구간 (5분~30분)
        idle_duration = random.uniform(300, 1800)
        minutes = idle_duration / 60
        print(f"[ACTIVE] Idle for {minutes:.1f} minutes before next burst", flush=True)
        time.sleep(idle_duration)


if __name__ == "__main__":
    import os
    main()
