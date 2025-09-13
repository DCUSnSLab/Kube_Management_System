import time
import sys
import signal
import socket
import threading
import random


"""
Background Active Process - Network Service
백그라운드에서 네트워크 서비스를 시뮬레이션
"""

def signal_handler(sig, frame):
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def simulate_network_activity():
    """네트워크 활동 시뮬레이션"""
    try:
        # DNS 조회 시뮬레이션
        hostname = f"service-{random.randint(1, 100)}.local"
        socket.gethostbyname_ex('localhost')
    except:
        pass
    
    # 데이터 처리 시뮬레이션
    data = bytes(random.randint(100, 1000))
    processed = hashlib.md5(data).hexdigest()
    return processed

def background_service():
    """백그라운드 서비스 워커"""
    log_file = f"/tmp/bg_network_{os.getpid()}_{threading.current_thread().ident}.log"
    request_count = 0
    
    while True:
        request_count += 1
        
        # 네트워크 작업 시뮬레이션
        result = simulate_network_activity()
        
        # 백그라운드: 파일 로깅만
        if request_count % 500 == 0:
            with open(log_file, 'a') as f:
                f.write(f"Thread {threading.current_thread().ident}: Processed {request_count} requests\n")
        
        time.sleep(random.uniform(0.01, 0.05))

def main():
    print(f"[BACKGROUND] Network Service Started - PID: {os.getpid()}", flush=True)
    
    # 멀티스레드 백그라운드 서비스
    num_threads = 3
    threads = []
    
    for i in range(num_threads):
        t = threading.Thread(target=background_service, daemon=True)
        t.start()
        threads.append(t)
    
    # 메인 스레드는 대기
    while True:
        time.sleep(10)
        # 최소한의 하트비트만 (백그라운드 특징)
        with open(f"/tmp/bg_network_{os.getpid()}_heartbeat.log", 'a') as f:
            f.write(f"Heartbeat: {time.time()}\n")

if __name__ == "__main__":
    import os
    import hashlib
    main()
