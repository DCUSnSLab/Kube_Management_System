import os
import sys
import time
import signal
import gc
import psutil
import random

"""
Active Process - Memory Intensive
(Randomized Normal Distribution Allocation)
"""


def signal_handler(sig, frame):
    print("\nMemory intensive process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class MemoryBlock:
    def __init__(self, size_mb):
        self.data = bytearray(size_mb * 1024 * 1024)
        self.size = size_mb
        for i in range(0, len(self.data), 4096):
            self.data[i] = 1

def main():
    print(f"[ACTIVE] Memory Intensive Process Started - PID: {os.getpid()}", flush=True)
    blocks, total_alloc, max_memory = [], 0, 150
    mu, sigma = 5, 2  # 평균 5MB, 표준편차 2MB

    while True:
        try:
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > 85:
                print(f"[ACTIVE] System memory high: {memory_percent:.1f}% → releasing", flush=True)
                if blocks:
                    for _ in range(min(3, len(blocks))):
                        released = blocks.pop(0)
                        total_alloc -= released.size
                        del released
                    gc.collect()
        except:
            pass

        # 정규분포 기반 랜덤 블록 크기
        block_size = max(1, int(random.gauss(mu, sigma)))

        if total_alloc + block_size < max_memory:
            try:
                block = MemoryBlock(block_size)
                blocks.append(block)
                total_alloc += block_size
                print(f"[ACTIVE] Allocated {block_size}MB → Total {total_alloc}MB / {max_memory}MB", flush=True)
            except MemoryError:
                gc.collect()
        else:
            if len(blocks) > 10:
                released = blocks.pop(0)
                total_alloc -= released.size
                del released
                gc.collect()
                print(f"[ACTIVE] Memory cleaned → {total_alloc}MB", flush=True)

        # 블록 내 접근 (활성 상태 유지)
        for block in blocks[:5]:
            block.data[0] = (block.data[0] + 1) % 256

        time.sleep(random.uniform(1.0, 2.5))

if __name__ == "__main__":
    main()
