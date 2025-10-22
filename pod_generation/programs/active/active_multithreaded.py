import time
import sys
import signal
import threading
import random
import queue
import numpy as np
import os

"""
Active Process - Multi-threaded
멀티스레드로 활발히 작동하는 프로세스
(Dynamic Thread Count 2~8)
"""


def signal_handler(sig, frame):
    print("\nMulti-threaded process terminated")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


class Worker(threading.Thread):
    def __init__(self, thread_id, work_queue, result_queue):
        super().__init__(daemon=True)
        self.thread_id = thread_id
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.processed = 0

    def run(self):
        while True:
            try:
                work_item = self.work_queue.get(timeout=1)
                result = self.process_work(work_item)
                self.result_queue.put((self.thread_id, result))
                self.processed += 1
                self.work_queue.task_done()
            except queue.Empty:
                continue

    def process_work(self, item):
        result = 0
        for i in range(item['iterations']):
            result += i ** 2
        time.sleep(random.uniform(0.001, 0.01))
        return result


def main():
    num_threads = int(np.random.randint(2, 8))  # 동적으로 스레드 개수 결정
    print(f"[ACTIVE] Multi-threaded Process Started - PID: {os.getpid()}, Threads: {num_threads}", flush=True)

    work_queue = queue.Queue(maxsize=100)
    result_queue = queue.Queue()
    workers = [Worker(i, work_queue, result_queue) for i in range(num_threads)]
    [w.start() for w in workers]

    work_id = 0
    total_results = 0

    while True:
        for _ in range(random.randint(3, 8)):
            work_id += 1
            work_item = {'id': work_id, 'iterations': random.randint(1000, 5000)}
            try:
                work_queue.put(work_item, timeout=0.1)
            except queue.Full:
                pass

        while not result_queue.empty():
            try:
                thread_id, result = result_queue.get_nowait()
                total_results += 1
            except queue.Empty:
                break

        if work_id % 100 == 0:
            print(f"[ACTIVE] Created {work_id} tasks, Completed {total_results}, Threads: {num_threads}", flush=True)
        time.sleep(0.1)


if __name__ == "__main__":
    main()
