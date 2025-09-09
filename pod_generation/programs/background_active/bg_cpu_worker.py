#!/usr/bin/env python3
"""
Background Active Process - CPU Worker
백그라운드에서 CPU 작업을 수행하는 워커 프로세스
"""
import time
import sys
import signal
import hashlib

def signal_handler(sig, frame):
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def hash_computation(data, iterations=1000):
    """해시 계산을 통한 CPU 작업"""
    result = data.encode()
    for _ in range(iterations):
        result = hashlib.sha256(result).digest()
    return result

def main():
    # 백그라운드 프로세스: 초기 메시지만 출력
    print(f"[BACKGROUND] CPU Worker Started - PID: {os.getpid()}", flush=True)
    
    # 로그 파일로 출력 전환 (백그라운드 특징)
    log_file = f"/tmp/bg_cpu_worker_{os.getpid()}.log"
    
    iteration = 0
    base_data = "background_worker_data"
    
    while True:
        iteration += 1
        
        # CPU 집중 작업 수행
        result = hash_computation(f"{base_data}_{iteration}", 500)
        
        # 백그라운드: 파일로만 로깅 (stdout으로 출력 최소화)
        if iteration % 100 == 0:
            with open(log_file, 'a') as f:
                f.write(f"Iteration {iteration}: Hash computed\n")
        
        # CPU 사용률 조절 (70-80% 목표)
        time.sleep(0.01)

if __name__ == "__main__":
    import os
    main()
