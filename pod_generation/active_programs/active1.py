import time
import ctypes

ALLOC_SIZE = 1024 * 1024  # 1MB
MAX_ALLOC_MB = 100        # 최대 100MB

def busy_loop():
    x = 0
    for _ in range(10_000_000):
        x += 1
    return x

def main():
    print("Foreground CPU + Memory load process started")

    mem_blocks = []
    alloc_count = 0

    while True:
        # CPU 부하 발생
        busy_loop()

        # 메모리 점유 증가
        if alloc_count < MAX_ALLOC_MB:
            block = ctypes.create_string_buffer(ALLOC_SIZE)
            mem_blocks.append(block)
            alloc_count += 1

        time.sleep(1)

if __name__ == "__main__":
    main()
