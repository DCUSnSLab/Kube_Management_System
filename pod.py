from process import Process, Mode_State, Policy_State
from poddata import Pod_Info, Pod_Lifecycle, Reason_Deletion
from checkHistory import CheckHistory
from checkProcess import CheckProcess
# from processDB import save_to_database, get_last_bash_history, save_bash_history
from DB_postgresql import (
    save_bash_history_result,
    save_pod_status,
    save_pod_lifecycle,
    save_to_process,
    get_last_bash_history,
    save_bash_history,
    save_delete_reason,
    is_deleted_in_DB, is_exist_in_DB
)

from datetime import datetime, timezone
import os

class Pod():
    def __init__(self, api, pod):
        self.api = api
        self.pod = pod
        self.pod_name = pod.metadata.name
        self.namespace = pod.metadata.namespace
        self.check_history_result = None
        self.processes = list()
        self.pod_status = None  # list -> obj
        self.pod_lifecycle = None  # list -> obj

    def get_Timestamp(self):
        """시간대를 UTC로 통일"""
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    def init_pod_data(self):
        """새로운 pod가 만들어지면, 초기 데이터 저장"""
        self.insert_Pod_lifecycle()
        self.insert_Pod_Info()
        self.save_Pod_liftcycle_to_DB()
        self.save_Pod_Info_to_DB()


    def is_deleted_in_DB(self):
        """Pod이 삭제되었는지 DB에서 확인"""
        return is_deleted_in_DB(self.pod_name, self.namespace)

    def is_exist_in_DB(self):
        """Pod이 DB에 존재하는지 확인"""
        return is_exist_in_DB(self.pod_name, self.namespace)

    def insert_Pod_Info(self):
        """pod's status save"""
        p = Pod_Info

        p.uid = self.pod.metadata.uid
        # p.labels = self.pod.metadata.labels
        # p.annotations = self.pod.metadata.annotations
        p.creation_timestamp = self.pod.metadata.creation_timestamp
        p.deletion_timestamp = self.pod.metadata.deletion_timestamp
        p.generate_name = self.pod.metadata.generate_name
        # p.owner_references = self.pod.metadata.owner_references
        # p.finalizers = self.pod.metadata.finalizers
        # p.managed_fields = self.pod.metadata.managed_fields

        # p.volumes = self.pod.spec.volumes
        # p.containers = self.pod.spec.containers
        p.node_name = self.pod.spec.node_name

        p.phase = self.pod.status.phase
        # p.conditions = self.pod.status.conditions
        p.hostIP = self.pod.status.host_ip
        p.podIP = self.pod.status.pod_ip
        p.startTime = self.pod.status.start_time

        self.pod_status = p

    def save_Pod_Info_to_DB(self):
        """pod's status save to DB"""
        save_pod_status(self.pod_name, self.namespace, self.pod_status)

    def insert_Pod_lifecycle(self):
        """Save pod's created time"""
        pl = Pod_Lifecycle()
        pl.createTime = self.pod.metadata.creation_timestamp
        self.pod_lifecycle = pl

    def save_Pod_liftcycle_to_DB(self):
        """Pod's lifecycle save to DB"""
        save_pod_lifecycle(self.pod_name, self.namespace, self.pod_lifecycle)

    def insert_DeleteReason(self, reason):
        if self.pod_lifecycle is None:
            self.pod_lifecycle = Pod_Lifecycle()
        """When pod deleted, save time and because of pod deleted"""
        self.pod_lifecycle.reason_deletion = Reason_Deletion[reason].value
        self.pod_lifecycle.deleteTime = self.get_Timestamp()

    def save_DeleteReason_to_DB(self):
        """Delete time and reason save to DB"""
        save_delete_reason(self.pod_name, self.namespace, self.pod_lifecycle)

    def getResultHistory(self):
        """run에서 검사 결과 값을 가져오고, gc로 결과 전달"""
        ch = CheckHistory(self.api, self.pod)
        lastTime_Bash_history=ch.getLastUseTime()
        self.check_history_result = ch.run(lastTime_Bash_history)
        print(self.check_history_result)

        # pod_lifecycle에 리스토리 검사 결과 저장
        save_bash_history_result(self.pod_name, self.namespace, self.check_history_result)

        if lastTime_Bash_history is not None:
            lastTimeStamp_Bash_history = ch.checkTimestamp(lastTime_Bash_history)
            self.saveBash_history_to_DB(lastTimeStamp_Bash_history)

        return self.check_history_result

    def saveBash_history_to_DB(self, last_modified_time):
        """Save bash history data to DB"""
        if last_modified_time is None:
            print(f"No bash_history found for pod: {self.pod_name}")
            return

        last_saved = get_last_bash_history(self.pod_name)

        if last_saved is None or str(last_saved).strip() != str(last_modified_time).strip():
            print(f"New bash_history detected for pod: {self.pod_name}, saving to DB.")
            save_bash_history(self.pod_name, self.namespace, last_modified_time)
        else:
            print(f"No changes in bash_history for pod: {self.pod_name}, skipping DB save.")

    def getResultProcess(self):
        """process 데이터로 판별하기 위한 함수 (미완)"""
        #/proc/[pid]/stat 값을 가져오거나 ps 명령어를 활용
        # cp = CheckProcess(self.api, self.pod)
        # cpResult = cp.run()
        # print(cpResult)
        # return cpResult
        pass

    def resetProcessList(self):
        self.processes = []

    def insertProcessData(self):
        """get /proc/stat data amd split into 52"""
        self.resetProcessList()

        cp = CheckProcess(self.api, self.pod)
        process_data = cp.getProcStat()
        # 명령어의 결과값이 None일 경우 건너뛰도록
        if process_data is None:
            print(f"Skipping Pod '{self.pod.metadata.name}': Failed to retrieve process data.")
            return

        for line in process_data.splitlines():
            fields = line.split()
            if len(fields) < 2:  # 최소 2개의 필드가 있어야 함
                continue

            p = Process()

            # Map fields to Process attributes
            try:
                p.pid = int(fields[0])
            except ValueError:
                print(f"Skipping invalid PID in line: {line}")
                continue
            p.comm = fields[1].strip('()')
            try:  # for undefined code
                p.state = Mode_State[fields[2]].value
            except KeyError:
                p.state = f"Unknown({fields[2]})"
            p.ppid = int(fields[3])
            p.pgrp = int(fields[4])
            p.session = int(fields[5])
            p.tty_nr = int(fields[6])
            p.tpgid = int(fields[7])
            p.flags = int(fields[8])
            p.minflt = int(fields[9])
            p.cminflt = int(fields[10])
            p.majflt = int(fields[11])
            p.cmajflt = int(fields[12])
            p.utime = int(fields[13])
            p.stime = int(fields[14])
            p.cutime = int(fields[15])
            p.cstime = int(fields[16])
            p.priority = int(fields[17])
            p.nice = int(fields[18])
            p.num_threads = int(fields[19])
            p.itrealvalue = int(fields[20])
            p.starttime = int(fields[21])
            p.vsize = int(fields[22])
            p.rss = int(fields[23])
            p.rsslim = int(fields[24])
            p.startcode = int(fields[25])
            p.endcode = int(fields[26])
            p.startstack = int(fields[27])
            p.kstkesp = int(fields[28])
            p.kstkeip = int(fields[29])
            p.signal = int(fields[30])
            p.blocked = int(fields[31])
            p.sigignore = int(fields[32])
            p.sigcatch = int(fields[33])
            p.wchan = int(fields[34])
            p.nswap = int(fields[35])
            p.cnswap = int(fields[36])
            p.exit_signal = int(fields[37])
            p.processor = int(fields[38])
            p.rt_priority = int(fields[39])
            p.policy = Policy_State(int(fields[40])).name
            p.delayacct_blkio_ticks = int(fields[41])
            p.guest_time = int(fields[42])
            p.cguest_time = int(fields[43])
            p.start_data = int(fields[44])
            p.end_data = int(fields[45])
            p.start_brk = int(fields[46])
            p.arg_start = int(fields[47])
            p.arg_end = int(fields[48])
            p.env_start = int(fields[49])
            p.env_end = int(fields[50])
            p.exit_code = int(fields[51])

            self.processes.append(p)

        # self.printProcList()

    def printProcList(self):
        print('-'*50)
        for p in self.processes:
            print(p.comm, p.state, p.pid, p.ppid, p.policy)
        print('-' * 50)

    def saveDataToCSV(self):
        """Save process data in csv file"""
        log_path = "/home/squirtle/Kube_Management_System/logging"
        date = datetime.now().strftime("%Y-%m-%d")
        date_dir = os.path.join(log_path, date)
        os.makedirs(date_dir, exist_ok=True)

        file_name = os.path.join(date_dir, f"{self.pod_name}.csv")

        headers = [
            "timestamp", "pid", "comm", "state", "ppid", "pgrp", "session", "tty_nr", "tpgid", "flags",
            "minflt", "cminflt", "majflt", "cmajflt", "utime", "stime", "cutime", "cstime",
            "priority", "nice", "num_threads", "itrealvalue", "starttime", "vsize", "rss",
            "rsslim", "startcode", "endcode", "startstack", "kstkesp", "kstkeip", "signal",
            "blocked", "sigignore", "sigcatch", "wchan", "nswap", "cnswap", "exit_signal",
            "processor", "rt_priority", "policy", "delayacct_blkio_ticks", "guest_time",
            "cguest_time", "start_data", "end_data", "start_brk", "arg_start", "arg_end",
            "env_start", "env_end", "exit_code"
        ]
        file_exists = os.path.exists(file_name)

        with open(file_name, mode="a", newline="", encoding="utf-8") as file:
            if not file_exists:
                file.write(",".join(headers) + "\n")  # 헤더 추가

            timestamp = self.get_Timestamp()

            for process in self.processes:
                field_values = [
                    timestamp,
                    str(process.pid), process.comm, process.state, str(process.ppid),
                    str(process.pgrp), str(process.session), str(process.tty_nr),
                    str(process.tpgid), str(process.flags), str(process.minflt),
                    str(process.cminflt), str(process.majflt), str(process.cmajflt),
                    str(process.utime), str(process.stime), str(process.cutime),
                    str(process.cstime), str(process.priority), str(process.nice),
                    str(process.num_threads), str(process.itrealvalue),
                    str(process.starttime), str(process.vsize), str(process.rss),
                    str(process.rsslim), str(process.startcode), str(process.endcode),
                    str(process.startstack), str(process.kstkesp), str(process.kstkeip),
                    str(process.signal), str(process.blocked), str(process.sigignore),
                    str(process.sigcatch), str(process.wchan), str(process.nswap),
                    str(process.cnswap), str(process.exit_signal), str(process.processor),
                    str(process.rt_priority), process.policy, str(process.delayacct_blkio_ticks),
                    str(process.guest_time), str(process.cguest_time), str(process.start_data),
                    str(process.end_data), str(process.start_brk), str(process.arg_start),
                    str(process.arg_end), str(process.env_start), str(process.env_end),
                    str(process.exit_code)
                ]
                file.write(",".join(field_values) + "\n")
            file.write("\n")

    def saveProcessDataToDB(self):
        """Save Pod's process data to DB"""
        timestamp = self.get_Timestamp()
        processes = []

        for process in self.processes:
            processes.append({
                "timestamp": timestamp,
                "pid": process.pid,
                "comm": process.comm,
                "state": process.state,
                "ppid": process.ppid,
                "pgrp": process.pgrp,
                "session": process.session,
                "tty_nr": process.tty_nr,
                "tpgid": process.tpgid,
                "flags": process.flags,
                "minflt": process.minflt,
                "cminflt": process.cminflt,
                "majflt": process.majflt,
                "cmajflt": process.cmajflt,
                "utime": process.utime,
                "stime": process.stime,
                "cutime": process.cutime,
                "cstime": process.cstime,
                "priority": process.priority,
                "nice": process.nice,
                "num_threads": process.num_threads,
                "itrealvalue": process.itrealvalue,
                "starttime": process.starttime,
                "vsize": process.vsize,
                "rss": process.rss,
                "rsslim": process.rsslim,
                "startcode": process.startcode,
                "endcode": process.endcode,
                "startstack": process.startstack,
                "kstkesp": process.kstkesp,
                "kstkeip": process.kstkeip,
                "signal": process.signal,
                "blocked": process.blocked,
                "sigignore": process.sigignore,
                "sigcatch": process.sigcatch,
                "wchan": process.wchan,
                "nswap": process.nswap,
                "cnswap": process.cnswap,
                "exit_signal": process.exit_signal,
                "processor": process.processor,
                "rt_priority": process.rt_priority,
                "policy": process.policy,
                "delayacct_blkio_ticks": process.delayacct_blkio_ticks,
                "guest_time": process.guest_time,
                "cguest_time": process.cguest_time,
                "start_data": process.start_data,
                "end_data": process.end_data,
                "start_brk": process.start_brk,
                "arg_start": process.arg_start,
                "arg_end": process.arg_end,
                "env_start": process.env_start,
                "env_end": process.env_end,
                "exit_code": process.exit_code
            })

        save_to_process(self.pod_name, self.namespace, processes)
