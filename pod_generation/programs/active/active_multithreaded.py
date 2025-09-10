import time
import sys
import signal
import threading
import random
import queue

"""
Active Process - Multi-threaded
멀티스레드로 활발히 작동하는 프로세스
"""

def signal_handler(sig, frame):
    print("\nMulti-threaded process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class Worker(threading.Thread):
    def __init__(self, thread_id, work_queue, result_queue):
        super().__init__()
        self.thread_id = thread_id
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.daemon = True
        self.processed = 0
    
    def run(self):
        while True:
            try:
                work_item = self.work_queue.get(timeout=1)
                
                # 작업 처리
                result = self.process_work(work_item)
                self.result_queue.put((self.thread_id, result))
                self.processed += 1
                
                self.work_queue.task_done()
            except queue.Empty:
                continue
    
    def process_work(self, item):
        """작업 처리"""
        result = 0
        for i in range(item['iterations']):
            result += i ** 2
        time.sleep(random.uniform(0.001, 0.01))
        return result

def main():
    print(f"[ACTIVE] Multi-threaded Process Started - PID: {os.getpid()}", flush=True)
    
    # 스레드 풀 생성
    num_threads = 4
    work_queue = queue.Queue(maxsize=100)
    result_queue = queue.Queue()
    
    workers = []
    for i in range(num_threads):
        worker = Worker(i, work_queue, result_queue)
        worker.start()
        workers.append(worker)
    
    print(f"[ACTIVE] Started {num_threads} worker threads", flush=True)
    
    work_id = 0
    total_results = 0
    
    while True:
        # 작업 생성
        for _ in range(random.randint(5, 15)):
            work_id += 1
            work_item = {
                'id': work_id,
                'iterations': random.randint(1000, 10000)
            }
            try:
                work_queue.put(work_item, timeout=0.1)
            except queue.Full:
                pass
        
        # 결과 수집
        while not result_queue.empty():
            try:
                thread_id, result = result_queue.get_nowait()
                total_results += 1
            except queue.Empty:
                break
        
        # 상태 출력
        if work_id % 100 == 0:
            print(f"[ACTIVE] Created {work_id} tasks, Completed {total_results}, Queue size: {work_queue.qsize()}", flush=True)
        
        time.sleep(0.1)

if __name__ == "__main__":
    import os
    main()
