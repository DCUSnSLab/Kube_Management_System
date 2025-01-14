import sqlite3

def initialize_database():
    """Initialize the SQLite database and create the table."""
    conn = sqlite3.connect("process_data.db")  # DB 파일 이름
    cursor = conn.cursor()

    # 프로세스 정보를 저장할 테이블 생성
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS process_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
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
        rsslim INTEGER,
        startcode INTEGER,
        endcode INTEGER,
        startstack INTEGER,
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
        start_data INTEGER,
        end_data INTEGER,
        start_brk INTEGER,
        arg_start INTEGER,
        arg_end INTEGER,
        env_start INTEGER,
        env_end INTEGER,
        exit_code INTEGER
    )
    """)
    conn.commit()
    conn.close()

def save_to_database(pod_name, processes):
    """Save process data to the database."""
    conn = sqlite3.connect("process_data.db")
    cursor = conn.cursor()

    # 데이터 삽입
    for process in processes:
        cursor.execute("""
        INSERT INTO process_data (
            pod_name, timestamp, pid, comm, state, ppid, pgrp, session, tty_nr, tpgid, flags,
            minflt, cminflt, majflt, cmajflt, utime, stime, cutime, cstime, priority,
            nice, num_threads, itrealvalue, starttime, vsize, rss, rsslim, startcode,
            endcode, startstack, kstkesp, kstkeip, signal, blocked, sigignore, sigcatch,
            wchan, nswap, cnswap, exit_signal, processor, rt_priority, policy,
            delayacct_blkio_ticks, guest_time, cguest_time, start_data, end_data,
            start_brk, arg_start, arg_end, env_start, env_end, exit_code
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pod_name, process["timestamp"], process["pid"], process["comm"], process["state"],
            process["ppid"], process["pgrp"], process["session"], process["tty_nr"], process["tpgid"],
            process["flags"], process["minflt"], process["cminflt"], process["majflt"], process["cmajflt"],
            process["utime"], process["stime"], process["cutime"], process["cstime"], process["priority"],
            process["nice"], process["num_threads"], process["itrealvalue"], process["starttime"],
            process["vsize"], process["rss"], process["rsslim"], process["startcode"], process["endcode"],
            process["startstack"], process["kstkesp"], process["kstkeip"], process["signal"], process["blocked"],
            process["sigignore"], process["sigcatch"], process["wchan"], process["nswap"], process["cnswap"],
            process["exit_signal"], process["processor"], process["rt_priority"], process["policy"],
            process["delayacct_blkio_ticks"], process["guest_time"], process["cguest_time"], process["start_data"],
            process["end_data"], process["start_brk"], process["arg_start"], process["arg_end"],
            process["env_start"], process["env_end"], process["exit_code"]
        ))

    conn.commit()
    conn.close()