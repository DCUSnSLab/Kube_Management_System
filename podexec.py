import socket
import threading
from dataclasses import dataclass, field
from enum import Enum
from typing import List, Optional

import yaml
from kubernetes import stream
from kubernetes.stream.ws_client import STDOUT_CHANNEL, STDERR_CHANNEL, ERROR_CHANNEL

DEFAULT_EXEC_TIMEOUT = 10

_BINARY_MISSING_HINTS = (
    "executable file not found",
    "starting container process caused",
    "oci runtime exec failed",
)
_STORAGE_FAULT_HINTS = (
    "input/output error",
    "stale file handle",
    "transport endpoint is not connected",
)

_CONNECT_LOCK = threading.Lock()


class ExecStatus(Enum):
    OK = "ok"
    COMMAND_FAILED = "command_failed"
    BINARY_MISSING = "binary_missing"
    STORAGE_FAULT = "storage_fault"
    UNREACHABLE = "unreachable"
    POD_GONE = "pod_gone"


@dataclass
class ExecResult:
    status: ExecStatus
    stdout: str = ""
    stderr: str = ""
    exit_code: Optional[int] = None
    error: str = ""
    chain: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status is ExecStatus.OK

    @property
    def reachable(self) -> bool:
        """명령이 컨테이너 안에서 실제로 실행되었는가 (출력 유무와 별개)"""
        return self.status in (ExecStatus.OK, ExecStatus.COMMAND_FAILED)


def _exc_chain(e: BaseException, limit: int = 6) -> List[str]:
    """예외 원인 체인 복원 (ApiException이 AttributeError로 변질되는 문제 대응)"""
    out, cur = [], e
    while cur is not None and len(out) < limit:
        out.append(f"{type(cur).__name__}: {cur}")
        cur = getattr(cur, "__context__", None) or getattr(cur, "__cause__", None)
    return out


def _classify_connect_error(e: BaseException):
    chain = _exc_chain(e)
    blob = "\n".join(chain).lower()
    cur = e
    while cur is not None:
        code = getattr(cur, "status_code", None)
        if code is None:
            code = getattr(cur, "status", None)
            if code == 0:
                code = None
        if code == 404:
            return ExecStatus.POD_GONE, chain
        if code is not None:
            break
        cur = getattr(cur, "__context__", None) or getattr(cur, "__cause__", None)
    if any(h in blob for h in _STORAGE_FAULT_HINTS):
        return ExecStatus.STORAGE_FAULT, chain
    if any(h in blob for h in _BINARY_MISSING_HINTS):
        return ExecStatus.BINARY_MISSING, chain
    return ExecStatus.UNREACHABLE, chain


def _classify_status_obj(err_raw: str, stderr: str):
    """ERROR(3) 채널의 metav1.Status를 해석해 (status, exit_code, message) 반환"""
    st = None
    if err_raw:
        try:
            st = yaml.safe_load(err_raw)
        except Exception:
            st = None
    if not isinstance(st, dict):
        st = None

    stderr_l = stderr.lower()

    if st is not None:
        if st.get("status") == "Success":
            return ExecStatus.OK, 0, ""
        reason = st.get("reason", "")
        message = st.get("message") or ""
        message_l = message.lower()
        if reason == "NonZeroExitCode":
            code = None
            for cause in ((st.get("details") or {}).get("causes") or []):
                if cause.get("reason") == "ExitCode":
                    try:
                        code = int(cause.get("message"))
                    except (TypeError, ValueError):
                        pass
            if any(h in stderr_l for h in _STORAGE_FAULT_HINTS):
                return ExecStatus.STORAGE_FAULT, code, stderr.strip()
            return ExecStatus.COMMAND_FAILED, code, stderr.strip() or message
        if any(h in message_l for h in _STORAGE_FAULT_HINTS):
            return ExecStatus.STORAGE_FAULT, None, message
        if any(h in message_l for h in _BINARY_MISSING_HINTS) \
                or "no such file or directory" in message_l:
            return ExecStatus.BINARY_MISSING, None, message
        return ExecStatus.UNREACHABLE, None, message or reason

    if any(h in stderr_l for h in _STORAGE_FAULT_HINTS):
        return ExecStatus.STORAGE_FAULT, None, stderr.strip()
    if any(h in stderr_l for h in _BINARY_MISSING_HINTS):
        return ExecStatus.BINARY_MISSING, None, stderr.strip()
    return ExecStatus.UNREACHABLE, None, "no status frame from apiserver"


def pod_exec(v1, pod_name: str, namespace: str, command,
             timeout: int = DEFAULT_EXEC_TIMEOUT, container: str = None) -> ExecResult:
    """
    파드 exec을 실행하고 실패 원인을 분류해 ExecResult로 반환.
    _preload_content=False로 ERROR(3) 채널을 직접 읽어 원인을 구분하며,
    timeout으로 무기한 블로킹을 방지한다.
    """
    kwargs = dict(command=command, stderr=True, stdin=False, stdout=True, tty=False,
                  _preload_content=False, _request_timeout=timeout)
    if container:
        kwargs["container"] = container

    try:
        with _CONNECT_LOCK:
            prev_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(timeout)
            try:
                ws = stream.stream(v1.connect_get_namespaced_pod_exec,
                                   pod_name, namespace, **kwargs)
            finally:
                socket.setdefaulttimeout(prev_timeout)
    except Exception as e:
        st, chain = _classify_connect_error(e)
        return ExecResult(status=st, error=chain[-1] if chain else str(e), chain=chain)

    try:
        ws.run_forever(timeout=timeout)
        stdout = ws.read_channel(STDOUT_CHANNEL) or ""
        stderr = ws.read_channel(STDERR_CHANNEL) or ""
        err_raw = ws.read_channel(ERROR_CHANNEL) or ""
        timed_out = ws.is_open() and not err_raw
    except Exception as e:
        chain = _exc_chain(e)
        return ExecResult(status=ExecStatus.UNREACHABLE, error=chain[0], chain=chain)
    finally:
        try:
            ws.close()
        except Exception:
            pass

    if timed_out:
        return ExecResult(status=ExecStatus.UNREACHABLE, stdout=stdout, stderr=stderr,
                          error=f"exec timeout after {timeout}s")

    st, code, msg = _classify_status_obj(err_raw, stderr)
    return ExecResult(status=st, stdout=stdout, stderr=stderr, exit_code=code, error=msg)


HARD_FAILURE_STATUSES = frozenset(s.value for s in (
    ExecStatus.BINARY_MISSING,
    ExecStatus.STORAGE_FAULT,
))
