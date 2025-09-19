import os
import sys
import time
import signal
import gc
import psutil  # 메모리 모니터링용

"""
Active Process - Memory Intensive
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
    blocks, total_alloc, max_memory = [], 0, 120  # 200Mi 제한에서 안전하게 120MB
    block_size = 5  # 5MB (활성 유지)
    while True:
        # 시스템 메모리 사용률 확인
        try:
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > 85:
                print(f"[ACTIVE] System memory usage high: {memory_percent:.1f}%, cleaning...", flush=True)
                if blocks:
                    for _ in range(min(3, len(blocks))):
                        released = blocks.pop(0)
                        total_alloc -= released.size
                        del released
                    gc.collect()
        except:
            pass  # psutil 오류 무시
        
        if total_alloc < max_memory:
            try:
                block = MemoryBlock(block_size)
                blocks.append(block)
                total_alloc += block_size
                print(f"[ACTIVE] Memory allocated: {total_alloc}MB / {max_memory}MB", flush=True)
            except MemoryError:
                if blocks:
                    released = blocks.pop(0)
                    total_alloc -= released.size
                    del released
                    gc.collect()
        else:
            # 메모리 청리: 오래된 블록 제거
            if len(blocks) > 15:
                for _ in range(5):
                    if blocks:
                        released = blocks.pop(0)
                        total_alloc -= released.size
                        del released
                gc.collect()
                print(f"[ACTIVE] Memory cleaned: {total_alloc}MB", flush=True)
            else:
                # 더 활발한 작업
                for block in blocks[:8]:
                    block.data[0] = (block.data[0] + 1) % 256
        time.sleep(2)

if __name__ == "__main__":
    main()
