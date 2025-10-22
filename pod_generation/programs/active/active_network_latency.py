import os
import sys
import time
import signal
import random
import socket

"""
Active Network Latency Simulation
외부 서버 통신 시 지연 포함 → I/O bound 판정 보정 시나리오
"""

def signal_handler(sig, frame):
    print("\n[NET] Process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def simulate_network_request(host="example.com", port=80, timeout=3):
    try:
        with socket.create_connection((host, port), timeout=timeout) as s:
            s.sendall(b"HEAD / HTTP/1.1\r\nHost: example.com\r\n\r\n")
            s.recv(128)
        return True
    except Exception as e:
        print(f"[NET] Connection failed: {e}", flush=True)
        return False

def main():
    print(f"[NET] External I/O Load Process Started - PID: {os.getpid()}", flush=True)
    latency_range = [0.1, 0.3, 0.5, 1.0, 2.0]
    iteration = 0

    while True:
        iteration += 1
        latency = random.choice(latency_range)
        print(f"[NET] Iteration {iteration}: simulate network delay {latency}s", flush=True)
        simulate_network_request()
        time.sleep(latency)

        # 가끔 CPU 연산 포함 (I/O-bound 보정)
        if random.random() < 0.3:
            for _ in range(200000):
                _ = random.random() ** 2
        time.sleep(random.uniform(1, 3))

if __name__ == "__main__":
    main()
