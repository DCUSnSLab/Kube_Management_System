import os
import sys
import time
import signal
import random

"""
Active Process - CPU Intensive
"""

def signal_handler(sig, frame):
    print("\nCPU intensive process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def simulate_compilation():
    """GCC 컴파일 작업 시뮬레이션"""
    result = 0
    # 컴파일 시 발생하는 CPU 집약적 작업
    for i in range(2000000):
        result += i ** 2
        result = result % 1000007
        # Symbol table, parsing 시뮬레이션
        if i % 100000 == 0:
            temp = [j*2 for j in range(1000)]
    return result

def simulate_student_program():
    """학생 프로그램 실행 시뮬레이션 (정렬, 행렬 연산 등)"""
    # 정렬 알고리즘 시뮬레이션
    data = [random.randint(1, 10000) for _ in range(5000)]
    data.sort()
    
    # 행렬 연산 시뮬레이션
    matrix = [[random.random() for _ in range(100)] for _ in range(100)]
    result = 0
    for i in range(100):
        for j in range(100):
            result += matrix[i][j]
    return result

def main():
    print(f"[ACTIVE-COMPILE] Student Activity Process Started - PID: {os.getpid()}", flush=True)
    print(f"[ACTIVE-COMPILE] Simulating gcc/g++ compilation and program execution", flush=True)
    
    iteration = 0
    activity_type = ["compilation", "execution", "debugging"]
    
    while True:
        iteration += 1
        activity = random.choice(activity_type)
        
        if activity == "compilation":
            # GCC 컴파일 시뮬레이션
            print(f"[ACTIVE-COMPILE] gcc -o program main.c (iteration {iteration})", flush=True)
            result = simulate_compilation()
            time.sleep(0.1)  # 짧은 대기 (다음 컴파일)
            
        elif activity == "execution":
            # 학생 프로그램 실행
            print(f"[ACTIVE-EXECUTE] ./student_program (iteration {iteration})", flush=True)
            result = simulate_student_program()
            time.sleep(0.2)  # 실행 후 결과 확인
            
        else:  # debugging
            # GDB 디버깅 시뮬레이션
            print(f"[ACTIVE-DEBUG] gdb student_program (iteration {iteration})", flush=True)
            for step in range(10):
                _ = sum([i**2 for i in range(10000)])
                time.sleep(0.05)  # Step-by-step 실행
        
        if iteration % 5 == 0:
            print(f"[ACTIVE-STATUS] Process State: R (Running), CPU Usage: High", flush=True)

if __name__ == "__main__":
    main()
