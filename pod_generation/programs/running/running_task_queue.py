import time
import sys
import signal
import queue
import random
import threading
import os

"""
Running Process - Task Queue
작업 큐를 처리하는 실행 중 프로세스
"""

def signal_handler(sig, frame):
    print("\nTask queue process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class Task:
    def __init__(self, task_id, task_type, data):
        self.id = task_id
        self.type = task_type
        self.data = data
        self.result = None
    
    def execute(self):
        """작업 실행"""
        if self.type == "compute":
            self.result = sum(self.data) * 2
        elif self.type == "transform":
            self.result = [x ** 2 for x in self.data]
        elif self.type == "aggregate":
            self.result = max(self.data) - min(self.data)
        time.sleep(random.uniform(0.01, 0.05))  # 작업 시간
        return self.result

def task_generator(task_queue):
    """작업 생성기"""
    task_id = 0
    task_types = ["compute", "transform", "aggregate"]
    
    while True:
        task_id += 1
        task_type = random.choice(task_types)
        data = [random.random() * 100 for _ in range(random.randint(5, 20))]
        
        task = Task(task_id, task_type, data)
        task_queue.put(task)
        
        time.sleep(random.uniform(0.05, 0.2))

def main():
    print(f"[RUNNING] Task Queue Processor Started - PID: {os.getpid()}", flush=True)
    
    task_queue = queue.Queue(maxsize=100)
    
    # 작업 생성 스레드
    generator = threading.Thread(target=task_generator, args=(task_queue,), daemon=True)
    generator.start()
    
    processed_count = 0
    start_time = time.time()
    
    while True:
        try:
            # 작업 큐에서 가져오기
            task = task_queue.get(timeout=1)
            
            # 작업 실행
            result = task.execute()
            processed_count += 1
            
            # 상태 출력
            if processed_count % 20 == 0:
                elapsed = time.time() - start_time
                rate = processed_count / elapsed if elapsed > 0 else 0
                print(f"[RUNNING] Processed: {processed_count} tasks, Rate: {rate:.2f} tasks/sec, Queue size: {task_queue.qsize()}", flush=True)
        
        except queue.Empty:
            print(f"[RUNNING] Queue empty, waiting for tasks...", flush=True)

if __name__ == "__main__":
    main()
