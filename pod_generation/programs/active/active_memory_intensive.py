import os
import sys
import time
import signal
import gc

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
    blocks, total_alloc, max_memory = [], 0, 500
    block_size = 10
    while True:
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
            for block in blocks[:5]:
                block.data[0] = (block.data[0] + 1) % 256
        time.sleep(2)

if __name__ == "__main__":
    main()
