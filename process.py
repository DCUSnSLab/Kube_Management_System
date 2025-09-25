from enum import Enum
from dataclasses import dataclass
from typing import Optional

class Mode_State(Enum):
    R = 'Running'
    S = 'Sleeping'
    D = 'Uninterruptible Sleep'
    Z = 'Zombie'
    T = 'Stopped'
    X = 'Dead'

class Policy_State(Enum):
    SCHED_NORMAL = 0
    SCHED_FIFO = 1
    SCHED_RR = 2

class Process:
    def __init__(self):
        self.pid = None  # Process ID
        self.comm = None  # Process name
        self.state = None  # Process state
        self.ppid = None  # Parent Process ID
        self.pgrp = None  # Process group ID
        self.session = None  # Session ID
        self.tty_nr = None  # Terminal device number if connected
        self.tpgid = None  # Foreground process group ID
        self.flags = None  # Process flags
        self.minflt = None  # Minor page faults count

        self.cminflt = None  # Minor page faults count by children
        self.majflt = None  # Major page faults count
        self.cmajflt = None  # Major page faults count by children
        self.utime = None  # Time in user mode (in jiffies)
        self.stime = None  # Time in kernel mode (in jiffies)
        self.cutime = None  # User mode time by children
        self.cstime = None  # Kernel mode time by children
        self.priority = None  # Process priority
        self.nice = None  # Nice value (default 0)
        self.num_threads = None  # Number of threads

        self.itrealvalue = None  # Real-time timer value (usually 0)
        self.starttime = None  # Time the process started after system boot
        self.vsize = None  # Virtual memory size in bytes
        self.rss = None  # Resident Set Size: pages in real memory
        self.rsslim = None  # RSS limit
        self.startcode = None  # Address of the start of code segment
        self.endcode = None  # Address of the end of code segment
        self.startstack = None  # Start address of the stack
        self.kstkesp = None  # Current value of ESP (stack pointer)
        self.kstkeip = None  # Current value of EIP (instruction pointer)

        self.signal = None  # Bitmap of pending signals
        self.blocked = None  # Bitmap of blocked signals
        self.sigignore = None  # Bitmap of ignored signals
        self.sigcatch = None  # Bitmap of caught signals
        self.wchan = None  # Wait channel if sleeping
        self.nswap = None  # Swap pages count
        self.cnswap = None  # Swap pages count by children
        self.exit_signal = None  # Signal sent on process exit
        self.processor = None  # CPU last executed on
        self.rt_priority = None  # Real-time priority (0 for normal)

        self.policy = None  # Scheduling policy
        self.delayacct_blkio_ticks = None  # Block I/O wait time in ticks
        self.guest_time = None  # Guest time (for virtualized environments)
        self.cguest_time = None  # Guest time by children
        self.start_data = None  # Address of the start of data segment
        self.end_data = None  # Address of the end of data segment
        self.start_brk = None  # Address for memory allocation
        self.arg_start = None  # Address of start of command-line arguments
        self.arg_end = None  # Address of end of command-line arguments
        self.env_start = None  # Address of start of environment variables

        self.env_end = None  # Address of end of environment variables
        self.exit_code = None  # Process exit code

        self.metrics: Optional[ProcessMetrics] = None

@dataclass
class ProcessMetrics:
    """프로세스의 추가 메트릭 정보"""
    # Context Switch 정보 (/proc/[pid]/status)
    voluntary_ctxt_switches: Optional[int] = None
    nonvoluntary_ctxt_switches: Optional[int] = None

    # 메모리 상세 정보 (/proc/[pid]/status)
    vm_rss: Optional[int] = None  # Resident set size

    # I/O 정보 (/proc/[pid]/io), I/O workload 확인
    read_bytes: Optional[int] = None
    write_bytes: Optional[int] = None

@dataclass
class CgroupMetrics:
    """cgroup 메트릭 정보"""
    # Memory cgroup 정보
    memory_current: Optional[int] = None  # Pod 메모리 사용량 추이
    memory_limit: Optional[int] = None  # limit 대비 사용량 비율 계산에 필요

    # IO cgroup 정보
    io_read_bytes: Optional[int] = None  # Pod 전체 I/O 트래픽 분석
    io_write_bytes: Optional[int] = None  # Pod 전체 I/O 트래픽 분석