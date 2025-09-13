import time
import sys
import signal
import multiprocessing
import ctypes
import array

"""
Active Process - Resource Intensive
리소스를 집중적으로 사용하는 활성 프로세스
"""

def signal_handler(sig, frame):
    print("\nResource intensive process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class ResourceIntensive:
    def __init__(self):
        self.memory_blocks = []
        self.cpu_results = []
        self.io_buffers = []
    
    def allocate_memory(self, size_mb):
        """메모리 할당 및 접근"""
        try:
            # C타입 배열로 실제 메모리 할당
            block = (ctypes.c_byte * (size_mb * 1024 * 1024))()
            # 메모리 터치 (실제 할당 강제)
            for i in range(0, len(block), 4096):
                block[i] = 1
            self.memory_blocks.append(block)
            return True
        except:
            return False
    
    def cpu_intensive_work(self, duration):
        """CPU 집중 작업"""
        start_time = time.time()
        result = 0
        while time.time() - start_time < duration:
            # 복잡한 계산
            for i in range(10000):
                result += i ** 2
                result = result % 1000007
        self.cpu_results.append(result)
        return result
    
    def io_simulation(self, size_kb):
        """I/O 시뮬레이션"""
        buffer = array.array('i', range(size_kb * 256))  # 1KB = 256 integers
        self.io_buffers.append(buffer)
        
        # 배열 조작 (I/O 시뮬레이션)
        for i in range(0, len(buffer), 100):
            buffer[i] = buffer[i] * 2
        
        return len(buffer)

def main():
    print(f"[ACTIVE] Resource Intensive Process Started - PID: {os.getpid()}", flush=True)
    print(f"[ACTIVE] CPU cores available: {multiprocessing.cpu_count()}", flush=True)
    
    resource_manager = ResourceIntensive()
    cycle = 0
    memory_allocated = 0
    max_memory = 100  # 최대 100MB
    
    while True:
        cycle += 1
        
        # 메모리 할당 (점진적)
        if memory_allocated < max_memory and cycle % 5 == 0:
            if resource_manager.allocate_memory(5):  # 5MB씩 할당
                memory_allocated += 5
                print(f"[ACTIVE] Memory allocated: {memory_allocated}MB / {max_memory}MB", flush=True)
        
        # CPU 작업
        cpu_result = resource_manager.cpu_intensive_work(0.5)  # 0.5초 CPU 작업
        
        # I/O 작업
        if cycle % 3 == 0:
            io_size = resource_manager.io_simulation(10)  # 10KB I/O
        
        # 상태 리포트
        if cycle % 10 == 0:
            print(f"[ACTIVE] Cycle {cycle}: Memory {memory_allocated}MB, CPU results: {len(resource_manager.cpu_results)}, I/O buffers: {len(resource_manager.io_buffers)}", flush=True)
        
        # 짧은 휴식
        time.sleep(0.1)

if __name__ == "__main__":
    import os
    main()
