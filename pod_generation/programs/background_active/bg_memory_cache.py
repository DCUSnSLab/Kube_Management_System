#!/usr/bin/env python3
"""
Background Active Process - Memory Cache
백그라운드에서 메모리 캐시를 관리하는 프로세스
"""
import time
import sys
import signal
import random
from collections import OrderedDict

def signal_handler(sig, frame):
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class LRUCache:
    def __init__(self, capacity_mb):
        self.capacity = capacity_mb * 1024 * 1024  # bytes
        self.cache = OrderedDict()
        self.current_size = 0
    
    def put(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
            self.current_size -= len(self.cache[key])
        
        # Evict if necessary
        while self.current_size + len(value) > self.capacity and self.cache:
            _, removed_value = self.cache.popitem(last=False)
            self.current_size -= len(removed_value)
        
        self.cache[key] = value
        self.current_size += len(value)
    
    def get(self, key):
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        return None

def main():
    print(f"[BACKGROUND] Memory Cache Started - PID: {os.getpid()}", flush=True)
    
    # 백그라운드 캐시 서비스
    cache = LRUCache(capacity_mb=50)  # 50MB 캐시
    log_file = f"/tmp/bg_cache_{os.getpid()}.log"
    
    operation_count = 0
    hit_count = 0
    miss_count = 0
    
    while True:
        operation_count += 1
        
        # 캐시 작업 시뮬레이션
        key = f"key_{random.randint(0, 1000)}"
        
        if random.random() < 0.3:  # 30% 읽기
            value = cache.get(key)
            if value:
                hit_count += 1
            else:
                miss_count += 1
        else:  # 70% 쓰기
            value_size = random.randint(1024, 10240)  # 1KB ~ 10KB
            value = bytes(value_size)
            cache.put(key, value)
        
        # 백그라운드: 주기적으로 파일에만 로깅
        if operation_count % 1000 == 0:
            with open(log_file, 'a') as f:
                hit_rate = hit_count / (hit_count + miss_count) if (hit_count + miss_count) > 0 else 0
                f.write(f"Ops: {operation_count}, Hit rate: {hit_rate:.2%}, Cache size: {cache.current_size / 1024 / 1024:.2f}MB\n")
        
        # 백그라운드 프로세스 속도
        time.sleep(0.001)

if __name__ == "__main__":
    import os
    main()
