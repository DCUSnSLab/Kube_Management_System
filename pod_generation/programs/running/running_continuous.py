import os
import time
import sys
import signal
import random
import math

"""
Running Process - Student Program Execution
학생이 작성한 프로그램 실행 시뮬레이션 (반복문, 이벤트 루프 등)
"""

def signal_handler(sig, frame):
    print("\nRunning process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def simulate_student_loop():
    """학생 프로그램의 일반적인 반복문 시뮬레이션"""
    # for/while 루프 실행
    result = 0
    for i in range(1000):
        result += i * math.sqrt(i + 1)
        if i % 100 == 0:
            # 중간 계산
            temp = [j**2 for j in range(100)]
    return result

def simulate_event_processing():
    """이벤트 처리 프로그램 시뮬레이션"""
    events = []
    # 이벤트 생성
    for _ in range(10):
        event = {
            "type": random.choice(["input", "timer", "signal"]),
            "value": random.randint(1, 100),
            "timestamp": time.time()
        }
        events.append(event)
    
    # 이벤트 처리
    processed = 0
    for event in events:
        if event["type"] == "input":
            processed += event["value"]
        elif event["type"] == "timer":
            processed += event["value"] * 2
        else:
            processed += event["value"] // 2
    
    return processed, len(events)

def simulate_data_processing():
    """데이터 처리 프로그램 (C 프로그래밍 실습)"""
    # 배열 처리 시뮬레이션
    array_size = 1000
    data = [random.random() * 100 for _ in range(array_size)]
    
    # 정렬 알고리즘 실습
    data.sort()
    
    # 통계 계산
    avg = sum(data) / len(data)
    min_val = min(data)
    max_val = max(data)
    
    return avg, min_val, max_val

def simulate_server_program():
    """서버 프로그램 시뮬레이션 (네트워크 프로그래밍 실습)"""
    # 클라이언트 요청 처리 시뮬레이션
    requests = []
    for _ in range(5):
        request = {
            "method": random.choice(["GET", "POST", "PUT"]),
            "path": f"/api/data/{random.randint(1, 100)}",
            "size": random.randint(100, 1000)
        }
        requests.append(request)
    
    # 요청 처리
    responses = []
    for req in requests:
        response_time = random.uniform(0.01, 0.1)
        responses.append(response_time)
    
    return len(requests), sum(responses)/len(responses)

def main():
    print(f"[RUNNING-STUDENT] Student Program Running - PID: {os.getpid()}", flush=True)
    print(f"[RUNNING-STUDENT] Executing: loops, data processing, server simulation", flush=True)
    
    iteration = 0
    program_types = ["loop", "event", "data_processing", "server"]
    
    while True:
        iteration += 1
        program_type = random.choice(program_types)
        
        try:
            if program_type == "loop":
                # 학생의 반복문 프로그램
                print(f"[RUNNING-LOOP] Executing for/while loops (iter {iteration})", flush=True)
                result = simulate_student_loop()
                time.sleep(0.05)
                
            elif program_type == "event":
                # 이벤트 처리 프로그램
                processed, count = simulate_event_processing()
                print(f"[RUNNING-EVENT] Processed {count} events (iter {iteration})", flush=True)
                time.sleep(0.1)
                
            elif program_type == "data_processing":
                # 데이터 처리 (정렬, 통계)
                avg, min_val, max_val = simulate_data_processing()
                print(f"[RUNNING-DATA] Array sorted, avg={avg:.2f} (iter {iteration})", flush=True)
                time.sleep(0.08)
                
            else:  # server
                # 서버 프로그램
                req_count, avg_time = simulate_server_program()
                print(f"[RUNNING-SERVER] Handled {req_count} requests (iter {iteration})", flush=True)
                time.sleep(0.15)
            
            # 주기적으로 프로세스 상태 정보 출력
            if iteration % 10 == 0:
                print(f"[RUNNING-STATUS] Process State: R (Running), CPU Active", flush=True)
                print(f"[RUNNING-STATUS] Continuous execution, iteration {iteration}", flush=True)
            
            # CPU 사용률을 적절히 유지 (50-70%)
            # R 상태를 유지하면서도 과도하지 않게
            time.sleep(random.uniform(0.01, 0.03))
            
        except Exception as e:
            print(f"[RUNNING-ERROR] Execution error: {e}", flush=True)
            time.sleep(0.5)

if __name__ == "__main__":
    main()
