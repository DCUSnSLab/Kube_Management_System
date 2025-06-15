import time
import ctypes

ALLOC_SIZE = 1024 * 1024  # 1MB
MAX_ALLOC_MB = 50  # 최대 50MB


def main():
    print("Memory-only active process started")

    mem_blocks = []
    alloc_count = 0

    while True:
        # 메모리 점유 증가 (1MB씩)
        if alloc_count < MAX_ALLOC_MB:
            block = ctypes.create_string_buffer(ALLOC_SIZE)
            mem_blocks.append(block)
            alloc_count += 1
            print(f"Allocated {alloc_count} MB")
        else:
            print("Memory allocation complete. Holding memory...")

        # 거의 CPU를 사용하지 않음 (sleep으로 idle)
        time.sleep(1)


if __name__ == "__main__":
    main()
