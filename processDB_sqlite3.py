import sqlite3
import time
import logging

logging.basicConfig(filename="error.log", level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s")

def initialize_database():
    """Initialize the SQLite database and create the table."""
    conn = None
    try:

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
            rsslim TEXT,          -- TEXT로 변경
            startcode TEXT,       -- TEXT로 변경
            endcode TEXT,         -- TEXT로 변경
            startstack TEXT,      -- TEXT로 변경
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
            start_data TEXT,      -- TEXT로 변경
            end_data TEXT,        -- TEXT로 변경
            start_brk TEXT,       -- TEXT로 변경
            arg_start TEXT,       -- TEXT로 변경
            arg_end TEXT,         -- TEXT로 변경
            env_start TEXT,       -- TEXT로 변경
            env_end TEXT,         -- TEXT로 변경
            exit_code INTEGER
        )
        """)
        # Bash history 정보를 저장할 새로운 테이블 생성
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS bash_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,  -- 각 기록을 고유하게 식별
            pod_name TEXT,  
            last_modified TEXT,  -- bash_history의 마지막 수정 시간
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP  -- 데이터 저장된 시간
        )
        """)

        conn.commit()
    except sqlite3.OperationalError as e:
        logging.error(f"SQLite OperationalError: {e}")
    except sqlite3.Error as e:
        logging.error(f"SQLite Error: {e}")
    finally:
        if conn:
            conn.close()

def save_to_database(pod_name, processes, max_retries=5, retry_delay=5):
    """Save process data to the database."""
    attempt = 0
    while attempt < max_retries:
        try:
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    pod_name, process['timestamp'], process['pid'], process['comm'], process['state'],
                    process['ppid'], process['pgrp'], process['session'], process['tty_nr'], process['tpgid'],
                    process['flags'], process['minflt'], process['cminflt'], process['majflt'], process['cmajflt'],
                    process['utime'], process['stime'], process['cutime'], process['cstime'], process['priority'],
                    process['nice'], process['num_threads'], process['itrealvalue'], process['starttime'],
                    process['vsize'], process['rss'], str(process['rsslim']), str(process['startcode']),
                    str(process['endcode']), str(process['startstack']), process['kstkesp'], process['kstkeip'],
                    process['signal'], process['blocked'], process['sigignore'], process['sigcatch'], process['wchan'],
                    process['nswap'], process['cnswap'], process['exit_signal'], process['processor'], process['rt_priority'],
                    process['policy'], process['delayacct_blkio_ticks'], process['guest_time'], process['cguest_time'],
                    str(process['start_data']), str(process['end_data']), str(process['start_brk']),
                    str(process['arg_start']), str(process['arg_end']), str(process['env_start']),
                    str(process['env_end']), process['exit_code']
                ))

            conn.commit()
            conn.close()
            return

        except sqlite3.OperationalError as e:
            logging.error(f"SQLite OperationalError: {e}. Retrying {attempt + 1}/{max_retries}...")
        except sqlite3.Error as e:
            logging.error(f"SQLite Error: {e}. Retrying {attempt + 1}/{max_retries}...")

        attempt += 1
        time.sleep(retry_delay)  # 일정 시간 대기 후 재시도

    logging.error(f"Failed to insert data after {max_retries} retries.")

def save_bash_history(pod_name, last_modified):
    # Bash history의 마지막 수정 시간을 DB에 저장
    try:
        conn = sqlite3.connect("process_data.db")
        cursor = conn.cursor()

        # 가장 최신의 last_modified 값을 가져옴
        cursor.execute("""
        SELECT last_modified FROM bash_history WHERE pod_name = ? ORDER BY id DESC LIMIT 1
        """, (pod_name,))
        last_saved = cursor.fetchone()

        # 기존 값과 다를 경우에만 저장
        if last_saved is None or last_saved[0] != last_modified:
            cursor.execute("""
            INSERT INTO bash_history (pod_name, last_modified)
            VALUES (?, ?)
            """, (pod_name, last_modified))
            conn.commit()
            print(f"Saved new bash history for pod: {pod_name}")

    except sqlite3.OperationalError as e:
        logging.error(f"SQLite OperationalError: {e}")
    except sqlite3.Error as e:
        logging.error(f"SQLite Error: {e}")
    finally:
        if conn:
            conn.close()

def get_last_bash_history(pod_name):
    # 해당 pod_name에 대한 가장 최근의 bash_history 값을 반환
    try:
        conn = sqlite3.connect("process_data.db")
        cursor = conn.cursor()

        cursor.execute("""
        SELECT last_modified FROM bash_history WHERE pod_name = ? ORDER BY id DESC LIMIT 1
        """, (pod_name,))
        last_saved = cursor.fetchone()

        return last_saved[0] if last_saved else None

    except sqlite3.OperationalError as e:
        logging.error(f"SQLite OperationalError: {e}")
        return None
    except sqlite3.Error as e:
        logging.error(f"SQLite Error: {e}")
        return None
    finally:
        if conn:
            conn.close()