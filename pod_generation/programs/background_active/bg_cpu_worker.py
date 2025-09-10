import os
import time
import sys
import signal
import hashlib
import random

"""
Background Active Process - Daemon/Service Simulation
백그라운드 데몬 프로세스 (make 백그라운드 작업, cron job 등)
"""

def signal_handler(sig, frame):
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def simulate_make_job():
    """백그라운드 make 작업 시뮬레이션"""
    # 컴파일 작업을 백그라운드에서 수행
    result = 0
    for i in range(500000):
        result += i ** 2
        result = result % 1000007
    return result

def simulate_cron_task():
    """정기 작업 시뮬레이션 (로그 정리, 백업 등)"""
    # 파일 정리 작업 시뮬레이션
    temp_files = []
    for i in range(5):
        filename = f"/tmp/cron_temp_{os.getpid()}_{i}.tmp"
        try:
            with open(filename, 'w') as f:
                f.write(f"Temporary data {i}\n")
            temp_files.append(filename)
        except:
            pass
    
    # 정리 작업
    for filename in temp_files:
        try:
            os.remove(filename)
        except:
            pass
    
    return len(temp_files)

def simulate_background_analysis():
    """백그라운드 데이터 분석 작업"""
    data = [random.random() for _ in range(10000)]
    # 통계 계산
    avg = sum(data) / len(data)
    min_val = min(data)
    max_val = max(data)
    return avg, min_val, max_val

def main():
    print(f"[BACKGROUND-DAEMON] Background Worker Started - PID: {os.getpid()}", flush=True)
    print(f"[BACKGROUND-DAEMON] Running as daemon (make -j, cron, etc.)", flush=True)
    
    # 로그 파일로 출력 전환 (백그라운드 특징)
    log_file = f"/tmp/bg_daemon_{os.getpid()}.log"
    
    iteration = 0
    activities = ["make_job", "cron_task", "analysis"]
    
    # 백그라운드 프로세스는 초기 메시지 후 조용히 실행
    print(f"[BACKGROUND-DAEMON] Switching to background mode...", flush=True)
    
    while True:
        iteration += 1
        activity = random.choice(activities)
        
        try:
            if activity == "make_job":
                # make -j 백그라운드 컴파일
                result = simulate_make_job()
                if iteration % 50 == 0:
                    with open(log_file, 'a') as f:
                        f.write(f"[{time.time()}] Make job completed: iteration {iteration}\n")
                
            elif activity == "cron_task":
                # cron 정기 작업
                files_cleaned = simulate_cron_task()
                if iteration % 100 == 0:
                    with open(log_file, 'a') as f:
                        f.write(f"[{time.time()}] Cron task: {files_cleaned} files processed\n")
                
            else:  # analysis
                # 백그라운드 분석 작업
                avg, min_val, max_val = simulate_background_analysis()
                if iteration % 75 == 0:
                    with open(log_file, 'a') as f:
                        f.write(f"[{time.time()}] Analysis: avg={avg:.4f}\n")
            
            # 백그라운드 프로세스는 CPU를 지속적으로 사용하지만 낮은 우선순위
            # State: R (Running) 또는 S (Sleeping) 상태를 번갈아가며
            if iteration % 200 == 0:
                # 가끔씩만 상태 출력 (백그라운드 특성)
                print(f"[BACKGROUND-STATUS] Still running... (iteration {iteration})", flush=True)
            
            # CPU 사용률 조절 - 백그라운드는 더 자주 대기
            time.sleep(random.uniform(0.01, 0.05))
            
        except Exception as e:
            with open(log_file, 'a') as f:
                f.write(f"[{time.time()}] Error: {e}\n")
            time.sleep(0.1)

if __name__ == "__main__":
    main()
