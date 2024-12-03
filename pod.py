from process import Process, Mode_State, Policy_State
from checkHistory import CheckHistory
from checkProcess import CheckProcess
from datetime import datetime
import os

class Pod():
    def __init__(self, api, pod):
        self.api = api
        self.pod = pod
        self.pod_name = pod.metadata.name
        self.namespace = pod.metadata.namespace
        self.check_history_result = None
        self.processes = list()

    def sendResult(self):
        pass

    def getResultHistory(self):
        # manage에서 비교결과값을 가져오도록
        ch = CheckHistory(self.api, self.pod)
        self.check_history_result = ch.run()
        print(self.check_history_result)

    def getResultProcess(self):
        #/proc/[pid]/stat 값을 가져오거나 ps 명령어를 활용
        # cp = CheckProcess(self.api, self.pod)
        # cpResult = cp.run()
        # print(cpResult)
        # return cpResult
        pass

    def resetProcessList(self):
        self.processes = []

    def insertProcessData(self):
        self.resetProcessList()

        cp = CheckProcess(self.api, self.pod)
        process_data = cp.getProcStat().splitlines()
        for line in process_data:
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
            p.state = Mode_State[fields[2]].value
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

            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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