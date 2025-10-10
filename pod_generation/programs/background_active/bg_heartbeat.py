import os
import sys
import time
import signal
import random
import datetime

"""
Background Daemon - Heartbeat & Log flush
False Positive 방지를 위한 지속적인 경량 백그라운드 활동
"""

def signal_handler(sig, frame):
    print("\n[DAEMON] Terminated gracefully")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def write_heartbeat(logfile):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(logfile, "a") as f:
        f.write(f"[{timestamp}] heartbeat alive\n")
        f.flush()
        os.fsync(f.fileno())

def main():
    print(f"[DAEMON] Background Heartbeat Process Started - PID: {os.getpid()}", flush=True)
    logfile = "/tmp/bg_heartbeat.log"
    interval = random.randint(20, 60)  # 20~60초마다
    iteration = 0

    while True:
        iteration += 1
        write_heartbeat(logfile)
        print(f"[DAEMON] Ping #{iteration} (interval={interval}s)", flush=True)
        time.sleep(interval)

if __name__ == "__main__":
    main()
