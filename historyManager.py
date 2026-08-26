import os
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional
from kubernetes import client, config

from podexec import pod_exec, ExecStatus, DEFAULT_EXEC_TIMEOUT


@dataclass
class HistoryResult:
    collected: bool
    mtime: Optional[int] = None
    file_missing: bool = False
    exec_status: str = ""
    detail: str = ""


class HistoryManager():
    def __init__(self, api_instance, pod, exec_timeout=DEFAULT_EXEC_TIMEOUT):
        self.file = "/home/dcuuser/.bash_history"
        self.v1 = api_instance
        self.pod = pod
        self.namespace = pod.metadata.namespace
        self.exec_timeout = exec_timeout

    def collect(self) -> HistoryResult:
        """
        .bash_history의 mtime을 수집하고 exec 실패 원인을 구분해 반환
            collected=True, mtime=int   : 수집 성공
            collected=True, mtime=None  : stat은 실행됐고 파일만 없음 (file_missing)
            collected=False             : exec 실패 (exec_status에 원인 분류)
        """
        r = pod_exec(self.v1, self.pod.metadata.name, self.namespace,
                     ["stat", "-c", "%Y", self.file], timeout=self.exec_timeout)

        if r.status is ExecStatus.OK:
            try:
                return HistoryResult(collected=True, mtime=int(r.stdout.strip()))
            except ValueError:
                print(f"[COLLECT-FAIL] {self.pod.metadata.name} history: "
                      f"parse_error ({r.stdout[:120]!r})")
                return HistoryResult(collected=False, exec_status="parse_error",
                                     detail=r.stdout[:200])

        if r.status is ExecStatus.COMMAND_FAILED:
            print(f"No bash_history found for pod: {self.pod.metadata.name}")
            return HistoryResult(collected=True, mtime=None, file_missing=True,
                                 exec_status="file_missing",
                                 detail=r.stderr.strip()[:200])

        print(f"[COLLECT-FAIL] {self.pod.metadata.name} history: "
              f"{r.status.value} ({(r.error or r.stderr)[:120]})")
        return HistoryResult(collected=False, exec_status=r.status.value,
                             detail=(r.error or r.stderr)[:200])

    def getLastUseTime(self):
        """하위호환: 성공 시 mtime(int), 그 외 None"""
        return self.collect().mtime

    def analyze(self, filetime):
        # 사용하지않는다고 판단하면 false
        if filetime == None:
            # file이 없는경우
            # 접속을 했으나, 사용중이거나 제대로 종료하지않으면 파일이 없음
            return True
        y, m, d = self.compareTime(filetime)
        # 7일이상 경과 시 False
        if y > 0 or m > 0:
            return False
        if d > 7:
            return False
        else:
            return True

    def getNowTime(self):
        # 현재 시스템은 utc기준
        now = datetime.now().timestamp()
        return now

    def compareTime(self, last_time):
        now_time = self.getNowTime()
        diff_time = now_time - last_time

        year, month, day = self.convertDay(diff_time)
        hour, minute, second = self.convertTime(diff_time)
        print(f"Compare time : {year}-{month}-{day} {hour}:{minute}:{second}")
        return year, month, day

    def convertDay(self, time):
        time = timedelta(seconds=time)
        day = time.days
        year = day // 365
        day %= 365
        month = day // 30
        day %= 30
        return year, month, day

    def convertTime(self, time):
        time = timedelta(seconds=time)
        second = time.seconds
        hour = second // 3600
        second %= 3600
        minute = second // 60
        second %= 60
        return hour, minute, second

    def checkTimestamp(self, time):
        '''
        운영제체 별로 날짜/시간을 표현하는 방식이 다르며 유닉스와 리눅스는 1970-01-01 00:00:00부터 현재 시간까지의 초를 누적한 시간을 사용
        이를 읽기 쉽도록 변환해줘야함
        '''
        time = datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M:%S')
        return time
