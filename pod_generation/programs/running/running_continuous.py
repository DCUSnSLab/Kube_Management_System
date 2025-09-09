#!/usr/bin/env python3
"""
Running Process - Continuous Loop
지속적으로 실행되는 일반적인 프로세스
"""
import time
import sys
import signal
import random

def signal_handler(sig, frame):
    print("\nRunning process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def process_data(data):
    """데이터 처리 시뮬레이션"""
    result = sum(data) / len(data) if data else 0
    return result

def main():
    print(f"[RUNNING] Continuous Process Started - PID: {os.getpid()}", flush=True)
    
    cycle_count = 0
    data_buffer = []
    
    while True:
        cycle_count += 1
        
        # 데이터 생성
        new_data = [random.random() * 100 for _ in range(10)]
        data_buffer.extend(new_data)
        
        # 버퍼가 일정 크기가 되면 처리
        if len(data_buffer) >= 100:
            result = process_data(data_buffer)
            data_buffer = data_buffer[-50:]  # 절반만 유지
            
            # 주기적 상태 출력
            if cycle_count % 50 == 0:
                print(f"[RUNNING] Cycle {cycle_count}: Buffer processed, Result: {result:.2f}", flush=True)
        
        # 일정한 속도로 실행
        time.sleep(0.1)

if __name__ == "__main__":
    import os
    main()
