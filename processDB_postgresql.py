import psycopg2
from psycopg2.extras import execute_values

class ProcessDatabase:
    def __init__(self):
        # 데이터베이스 설정 및 연결 초기화
        self.db_config = {
            "dbname": "process_data_postgres",
            "user": "squirtle",
            "password": "snslab1",
            "host": "localhost",
            "port": "5432"
        }
        self.conn = None
        self.cursor = None

    def connect(self):
        # 데이터베이스 연결 설정
        if self.conn is None:
            self.conn = psycopg2.connect(
                dbname=self.db_config["dbname"],
                user=self.db_config["user"],
                password=self.db_config["password"],
                host=self.db_config["host"],
                port=self.db_config["port"]
            )
            self.cursor = self.conn.cursor()

    def initialize_database(self):
        # 데이터베이스 초기화
        self.connect()
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS process_data (
            id SERIAL PRIMARY KEY,
            pod_name TEXT,
            timestamp TEXT,
            pid INTEGER,
            comm TEXT,
            state TEXT,
            ppid INTEGER,
            pgrp INTEGER,
            session INTEGER,
            tty_nr INTEGER,
            tpgid INTEGER,
            flags INTEGER,
            minflt INTEGER,
            cminflt INTEGER,
            majflt INTEGER,
            cmajflt INTEGER,
            utime INTEGER,
            stime INTEGER,
            cutime INTEGER,
            cstime INTEGER,
            priority INTEGER,
            nice INTEGER,
            num_threads INTEGER,
            itrealvalue INTEGER,
            starttime INTEGER,
            vsize INTEGER,
            rss INTEGER,
            rsslim NUMERIC,       -- NUMERIC으로 설정
            startcode NUMERIC,    -- NUMERIC으로 설정
            endcode NUMERIC,      -- NUMERIC으로 설정
            startstack NUMERIC,   -- NUMERIC으로 설정
            kstkesp INTEGER,
            kstkeip INTEGER,
            signal INTEGER,
            blocked INTEGER,
            sigignore INTEGER,
            sigcatch INTEGER,
            wchan INTEGER,
            nswap INTEGER,
            cnswap INTEGER,
            exit_signal INTEGER,
            processor INTEGER,
            rt_priority INTEGER,
            policy TEXT,
            delayacct_blkio_ticks INTEGER,
            guest_time INTEGER,
            cguest_time INTEGER,
            start_data NUMERIC,   -- NUMERIC으로 설정
            end_data NUMERIC,     -- NUMERIC으로 설정
            start_brk NUMERIC,    -- NUMERIC으로 설정
            arg_start NUMERIC,    -- NUMERIC으로 설정
            arg_end NUMERIC,      -- NUMERIC으로 설정
            env_start NUMERIC,    -- NUMERIC으로 설정
            env_end NUMERIC,      -- NUMERIC으로 설정
            exit_code INTEGER
        )
        """)

        self.conn.commit()

    def save_to_database(self, pod_name, processes):
        # Save process data to the PostgreSQL database
        self.connect()

        # 데이터 삽입
        records = [
            (
                pod_name, process['timestamp'], process['pid'], process['comm'], process['state'],
                process['ppid'], process['pgrp'], process['session'], process['tty_nr'], process['tpgid'],
                process['flags'], process['minflt'], process['cminflt'], process['majflt'], process['cmajflt'],
                process['utime'], process['stime'], process['cutime'], process['cstime'], process['priority'],
                process['nice'], process['num_threads'], process['itrealvalue'], process['starttime'],
                process['vsize'], process['rss'], process['rsslim'], process['startcode'],
                process['endcode'], process['startstack'], process['kstkesp'], process['kstkeip'],
                process['signal'], process['blocked'], process['sigignore'], process['sigcatch'], process['wchan'],
                process['nswap'], process['cnswap'], process['exit_signal'], process['processor'], process['rt_priority'],
                process['policy'], process['delayacct_blkio_ticks'], process['guest_time'], process['cguest_time'],
                process['start_data'], process['end_data'], process['start_brk'],
                process['arg_start'], process['arg_end'], process['env_start'],
                process['env_end'], process['exit_code']
            )
            for process in processes
        ]

        query = """
        INSERT INTO process_data (
            pod_name, timestamp, pid, comm, state, ppid, pgrp, session, tty_nr, tpgid, flags,
            minflt, cminflt, majflt, cmajflt, utime, stime, cutime, cstime, priority,
            nice, num_threads, itrealvalue, starttime, vsize, rss, rsslim, startcode,
            endcode, startstack, kstkesp, kstkeip, signal, blocked, sigignore, sigcatch,
            wchan, nswap, cnswap, exit_signal, processor, rt_priority, policy,
            delayacct_blkio_ticks, guest_time, cguest_time, start_data, end_data,
            start_brk, arg_start, arg_end, env_start, env_end, exit_code
        ) VALUES %s
        """

        # execute_values를 사용하여 여러 행 삽입
        execute_values(self.cursor, query, records)

        self.conn.commit()

    def close(self):
        # 데이터베이스 연결 종료
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
