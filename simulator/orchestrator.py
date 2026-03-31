# orchestrator.py
# -----------------------------------------------------------------------------
# Run multiple Generator simulations concurrently (process pool),
# centralize stdout/stderr to avoid interleaving, ensure namespaces,
# and cleanup pods if a simulator dies unexpectedly.
#
# NOTE:
# - Your Generator class must be importable as `from generator import Generator`.
#   If your filename differs, change the import in worker_entry().
# - No changes required inside Generator itself.
# -----------------------------------------------------------------------------

from __future__ import annotations
import sys
import os
import time
import signal
import traceback
import threading
import datetime as dt
from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple
from multiprocessing import Process, Queue, Event, set_start_method, current_process

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

# ==============================
# 0) Line-safe stdout/stderr → Queue
# ==============================

class _LineBuf:
    """Collects partial writes and emits complete lines to a callback."""
    def __init__(self, emit):
        self.buf = ""
        self.emit = emit  # emit(line: str)

    def write(self, s: str):
        self.buf += s
        while "\n" in self.buf:
            line, self.buf = self.buf.split("\n", 1)
            if line != "":
                self.emit(line)

    def flush(self):
        if self.buf:
            self.emit(self.buf)
            self.buf = ""


class QueueWriter:
    """File-like object that sends lines to a multiprocessing.Queue."""
    def __init__(self, q: Queue, namespace: str, stream: str):
        self.q = q
        self.ns = namespace
        self.stream = stream  # 'stdout' or 'stderr'
        self._lb = _LineBuf(self._emit)

    def write(self, s: str):
        self._lb.write(s)

    def flush(self):
        self._lb.flush()

    def _emit(self, line: str):
        self.q.put({
            "t": time.time(),
            "ns": self.ns,
            "pid": os.getpid(),
            "proc": current_process().name,
            "stream": self.stream,
            "msg": line
        })


def log_listener(log_queue: Queue, stop_event: Event, to_file: str | None = None):
    """Central logger that prints (and optionally writes) one line at a time."""
    fh = open(to_file, "a", buffering=1, encoding="utf-8") if to_file else None
    try:
        while not stop_event.is_set() or not log_queue.empty():
            try:
                rec = log_queue.get(timeout=0.2)
            except Exception:
                continue
            ts = dt.datetime.fromtimestamp(rec["t"]).strftime("%H:%M:%S")
            line = f"[{ts}][{rec['proc']}:{rec['pid']}][{rec['stream'].upper()}][ns={rec['ns']}] {rec['msg']}"
            print(line, flush=True)
            if fh:
                fh.write(line + "\n")
    finally:
        if fh:
            fh.close()


# ==============================
# 1) Task definition
# ==============================

@dataclass
class SimTask:
    namespace: str
    # Arguments forwarded to Generator.run_poisson()
    sim_kwargs: Dict[str, Any] = field(default_factory=dict)


# ==============================
# 2) Kubernetes helpers
# ==============================

def k8s_clients() -> Tuple[client.CoreV1Api, client.AppsV1Api]:
    # Try in-cluster first; fallback to kubeconfig
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    return client.CoreV1Api(), client.AppsV1Api()

def ensure_namespace(ns: str):
    core, _ = k8s_clients()
    try:
        core.read_namespace(ns)
    except ApiException as e:
        if e.status == 404:
            core.create_namespace({"metadata": {"name": ns}})
            print(f"[NS] created namespace '{ns}'", flush=True)
        else:
            raise

def delete_all_pods_in_namespace(ns: str, wait: bool = True, timeout_s: int = 120):
    core, _ = k8s_clients()
    try:
        pods = core.list_namespaced_pod(ns).items
    except ApiException as e:
        if e.status == 404:
            print(f"[CLEANUP] namespace '{ns}' not found; skip pod deletion", flush=True)
            return
        raise

    names = [p.metadata.name for p in pods] if pods else []
    for name in names:
        try:
            core.delete_namespaced_pod(name, ns)
            print(f"[CLEANUP] delete pod {name} in {ns}", flush=True)
        except ApiException as e:
            if e.status != 404:
                print(f"[CLEANUP][WARN] delete {name} failed: {e}", flush=True)

    if wait:
        start = time.time()
        while True:
            try:
                left = core.list_namespaced_pod(ns).items
            except ApiException:
                left = []
            if not left:
                print(f"[CLEANUP] all pods gone in {ns}", flush=True)
                return
            if time.time() - start > timeout_s:
                remain = [p.metadata.name for p in left]
                print(f"[CLEANUP][TIMEOUT] still remaining: {remain}", flush=True)
                return
            time.sleep(1.0)


# ==============================
# 3) Worker process entry
# ==============================

def worker_entry(task: SimTask, log_queue: Queue) -> int:
    """Each process runs one Generator simulation."""
    # Graceful termination (SIGTERM)
    def _graceful_term(signum, frame):
        print(f"[{current_process().name}] SIGTERM received; exiting", flush=True)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, _graceful_term)

    # Redirect stdout/stderr to queue
    sys.stdout = QueueWriter(log_queue, task.namespace, "stdout")
    sys.stderr = QueueWriter(log_queue, task.namespace, "stderr")

    print(f"start task ns={task.namespace}, kwargs={task.sim_kwargs}")

    ensure_namespace(task.namespace)

    # Import user's Generator (no modifications inside Generator)
    from generator import Generator  # <- change if your file/module name differs

    gen = Generator(namespace=task.namespace, log_queue=log_queue)
    try:
        # Run Poisson simulation (adjust if you want to call run() instead)
        gen.run_poisson(**task.sim_kwargs)

        # Defensive cleanup (in case GC didn't remove the last pods)
        try:
            gen.deletePod()
        except Exception:
            pass

        print(f"finished task ns={task.namespace}")
        return 0

    except SystemExit:
        try:
            gen.deletePod()
        except Exception:
            pass
        print(f"terminated ns={task.namespace}")
        return 0

    except Exception as e:
        print(f"[ERROR] {e}\n{traceback.format_exc()}")
        try:
            gen.deletePod()
        except Exception:
            pass
        return 1


def _proc_wrapper(task: SimTask, log_queue: Queue):
    """Wrapper to convert worker_entry return code to process exit code."""
    code = worker_entry(task, log_queue)
    sys.exit(code)


# ==============================
# 4) Simple fixed-size process pool
# ==============================

class SimulatorPool:
    def __init__(self, max_workers: int, log_queue: Queue):
        self.max_workers = max_workers
        self.log_queue = log_queue
        self.queue: List[SimTask] = []
        self.active: List[Tuple[Process, SimTask]] = []

    def submit(self, task: SimTask):
        self.queue.append(task)

    def _start_next_if_possible(self):
        while len(self.active) < self.max_workers and self.queue:
            task: SimTask = self.queue.pop(0)
            p = Process(target=_proc_wrapper, args=(task, self.log_queue), name=f"sim-{task.namespace}")
            p.daemon = False
            p.start()
            self.active.append((p, task))
            print(f"[POOL] started {p.name} (ns={task.namespace}) pid={p.pid}", flush=True)

    def run(self):
        try:
            self._start_next_if_possible()
            while self.active or self.queue:
                # Reap finished workers
                still: List[Tuple[Process, SimTask]] = []
                for p, task in self.active:
                    p.join(timeout=0)
                    if p.is_alive():
                        still.append((p, task))
                        continue

                    exitcode = p.exitcode
                    print(f"[POOL] process {p.name} exitcode={exitcode}", flush=True)

                    # Non-zero or killed by signal (negative exitcode) → cleanup pods
                    if exitcode is None or exitcode != 0:
                        print(f"[POOL][ABEND] cleaning pods in ns={task.namespace}", flush=True)
                        try:
                            delete_all_pods_in_namespace(task.namespace)
                        except Exception as e:
                            print(f"[POOL][ABEND][WARN] cleanup failed for ns={task.namespace}: {e}", flush=True)

                self.active = still

                # Fill free slots
                self._start_next_if_possible()
                time.sleep(0.2)

        except KeyboardInterrupt:
            print("[POOL] KeyboardInterrupt: terminating all", flush=True)
            for p, _ in self.active:
                if p.is_alive():
                    p.terminate()
            for p, task in self.active:
                try:
                    p.join(timeout=5)
                except Exception:
                    pass
            # Best-effort cleanup
            for _, task in self.active:
                try:
                    delete_all_pods_in_namespace(task.namespace)
                except Exception:
                    pass
        finally:
            print("[POOL] all tasks done; exiting", flush=True)


# ==============================
# 5) Example main
# ==============================

if __name__ == "__main__":
    # Use 'spawn' to avoid fork-related issues with Kubernetes clients
    try:
        set_start_method("spawn", force=True)
    except RuntimeError:
        pass

    # Central log queue + listener
    log_queue: Queue = Queue()
    log_stop: Event = Event()
    log_thread = threading.Thread(
        target=log_listener,
        args=(log_queue, log_stop, "simulator.log"),  # set to None to disable file
        daemon=True
    )
    log_thread.start()

    # Define tasks (edit as needed)
    tasks = [
        # SimTask("gc-05m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 300}),
        # SimTask("gc-10m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 600}),
        # SimTask("gc-15m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 900}),
        # SimTask("gc-20m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 1200}),
        # SimTask("gc-25m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 1500}),
        # SimTask("gc-30m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 1800}),

        # SimTask("gc-05m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 300}),
        # SimTask("gc-10m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 600}),
        # SimTask("gc-15m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 900}),
        # SimTask("gc-20m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 1200}),
        # SimTask("gc-25m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 1500}),
        # SimTask("gc-30m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 1800}),

        # SimTask("gc-05m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 300}),
        # SimTask("gc-10m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 600}),
        # SimTask("gc-15m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 900}),
        # SimTask("gc-20m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 1200}),
        # SimTask("gc-25m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 1500}),
        # SimTask("gc-30m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 1800}),

        # SimTask("gc-05m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 300}),
        # SimTask("gc-10m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 600}),
        # SimTask("gc-15m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 900}),
        # SimTask("gc-20m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 1200}),
        # SimTask("gc-25m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 1500}),
        # SimTask("gc-30m", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 1800}),

        SimTask("gc-40m", {"duration_s": 4200, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 2400}),
        SimTask("gc-50m", {"duration_s": 4800, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 3000}),
        SimTask("gc-60m", {"duration_s": 5400, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 3600}),
        SimTask("gc-70m", {"duration_s": 6000, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 4200}),

        SimTask("gc-40m", {"duration_s": 4200, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 2400}),
        SimTask("gc-50m", {"duration_s": 4800, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 3000}),
        SimTask("gc-60m", {"duration_s": 5400, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 3600}),
        SimTask("gc-70m", {"duration_s": 6000, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 4200}),

        SimTask("gc-40m", {"duration_s": 4200, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 2400}),
        SimTask("gc-50m", {"duration_s": 4800, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 3000}),
        SimTask("gc-60m", {"duration_s": 5400, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 3600}),
        SimTask("gc-70m", {"duration_s": 6000, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 4200}),

        SimTask("gc-40m", {"duration_s": 4200, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 2400}),
        SimTask("gc-50m", {"duration_s": 4800, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 3000}),
        SimTask("gc-60m", {"duration_s": 5400, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 3600}),
        SimTask("gc-70m", {"duration_s": 6000, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 4200}),

        SimTask("gc-40m", {"duration_s": 4200, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 2400}),
        SimTask("gc-50m", {"duration_s": 4800, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 3000}),
        SimTask("gc-60m", {"duration_s": 5400, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 3600}),
        SimTask("gc-70m", {"duration_s": 6000, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 4200}),        
        # SimTask("gc-no", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 300, "hasGC": False}),
        # SimTask("gc-no", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 300, "hasGC": False}),
        # SimTask("gc-no", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 300, "hasGC": False}),
        # SimTask("gc-no", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 300, "hasGC": False}),
        # SimTask("gc-no", {"duration_s": 3600, "generate_until_min": 20, "rate_per_min": 1.5, "inactive_threshold_s": 300, "hasGC": False}),
    ]

    # Ensure namespaces exist up front (optional; workers also ensure)
    for t in tasks:
        try:
            ensure_namespace(t.namespace)
        except Exception as e:
            print(f"[NS][WARN] ensure '{t.namespace}' failed: {e}", flush=True)

    pool = SimulatorPool(max_workers=3, log_queue=log_queue)
    for t in tasks:
        pool.submit(t)

    pool.run()

    # Stop logger
    log_stop.set()
    log_thread.join(timeout=5)

