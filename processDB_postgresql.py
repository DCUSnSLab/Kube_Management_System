import psycopg2
import logging

logging.basicConfig(filename="error.log", level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s")

# PostgreSQL 설정
DATABASE_CONFIG = {
    "dbname": "gc-data",
    "user": "k8s_gc",
    "password": "snslab",
    "host": "localhost",  # 또는 실제 서버 주소
    "port": "5432"
}

def get_db_connection():
    """PostgreSQL 연결 생성"""
    return psycopg2.connect(**DATABASE_CONFIG)

def initialize_database():
    """PostgreSQL 데이터베이스 초기화 및 테이블 생성"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 프로세스 정보를 저장할 테이블 생성
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS process_data (
            id SERIAL PRIMARY KEY,
            pod_name VARCHAR(255),
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            pid INTEGER,
            comm VARCHAR(255),
            state VARCHAR(10),
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
            starttime BIGINT,
            vsize BIGINT,
            rss BIGINT,
            rsslim VARCHAR(50),
            startcode VARCHAR(50),
            endcode VARCHAR(50),
            startstack VARCHAR(50),
            kstkesp BIGINT,
            kstkeip BIGINT,
            signal INTEGER,
            blocked INTEGER,
            sigignore INTEGER,
            sigcatch INTEGER,
            wchan BIGINT,
            nswap INTEGER,
            cnswap INTEGER,
            exit_signal INTEGER,
            processor INTEGER,
            rt_priority INTEGER,
            policy VARCHAR(20),
            delayacct_blkio_ticks BIGINT,
            guest_time INTEGER,
            cguest_time INTEGER,
            start_data VARCHAR(50),
            end_data VARCHAR(50),
            start_brk VARCHAR(50),
            arg_start VARCHAR(50),
            arg_end VARCHAR(50),
            env_start VARCHAR(50),
            env_end VARCHAR(50),
            exit_code INTEGER
        );
        """)

        # bash_history 테이블 추가
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS bash_history (
            id SERIAL PRIMARY KEY,
            pod_name VARCHAR(255),
            last_modified TIMESTAMP,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        conn.commit()
    except psycopg2.Error as e:
        logging.error(f"PostgreSQL Error: {e}")
    finally:
        if conn:
            conn.close()

def save_to_database(pod_name, processes):
    """PostgreSQL에 프로세스 데이터 저장"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO process_data (
            pod_name, timestamp, pid, comm, state, ppid, pgrp, session, tty_nr, tpgid, flags,
            minflt, cminflt, majflt, cmajflt, utime, stime, cutime, cstime, priority,
            nice, num_threads, itrealvalue, starttime, vsize, rss, rsslim, startcode,
            endcode, startstack, kstkesp, kstkeip, signal, blocked, sigignore, sigcatch,
            wchan, nswap, cnswap, exit_signal, processor, rt_priority, policy,
            delayacct_blkio_ticks, guest_time, cguest_time, start_data, end_data,
            start_brk, arg_start, arg_end, env_start, env_end, exit_code
        ) VALUES (
            %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
        """

        for process in processes:
            cursor.execute(insert_query, (
                pod_name, process['pid'], process['comm'], process['state'],
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
    except psycopg2.Error as e:
        logging.error(f"PostgreSQL Error: {e}")
    finally:
        if conn:
            conn.close()

def save_bash_history(pod_name, last_modified):
    """PostgreSQL에 bash_history 데이터 저장"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
        INSERT INTO bash_history (pod_name, last_modified)
        VALUES (%s, %s);
        """, (pod_name, last_modified))

        conn.commit()
    except psycopg2.Error as e:
        logging.error(f"PostgreSQL Error: {e}")
    finally:
        if conn:
            conn.close()

def get_last_bash_history(pod_name):
    """PostgreSQL에서 해당 pod의 마지막 bash_history 수정 시간을 가져옴"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
        SELECT last_modified FROM bash_history WHERE pod_name = %s ORDER BY id DESC LIMIT 1;
        """, (pod_name,))
        last_saved = cursor.fetchone()

        return last_saved[0] if last_saved else None
    except psycopg2.Error as e:
        logging.error(f"PostgreSQL Error: {e}")
        return None
    finally:
        if conn:
            conn.close()
