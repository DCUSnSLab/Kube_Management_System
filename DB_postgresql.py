import psycopg2
import logging
import configparser
import os

logging.basicConfig(filename="error.log", level=logging.ERROR, format="%(asctime)s - %(levelname)s - %(message)s")

config = configparser.ConfigParser()
path = os.path.dirname(os.path.abspath(__file__))
config_path = os.path.join(path, "config.ini")
config.read(config_path)  # DB config file

# PostgreSQL setting
DATABASE_CONFIG = {
    "dbname": config["database"]["dbname"],
    "user": config["database"]["user"],
    "password": config["database"]["password"],
    "host": config["database"]["host"],  # localhost 또는 실제 서버 주소
    "port": config["database"]["port"]
}

def get_db_connection():
    """PostgreSQL connect DB"""
    try:
        return psycopg2.connect(**DATABASE_CONFIG)
    except psycopg2.Error as e:
        logging.error(f"Database connection error: {e}")
        return None

def initialize_database():
    """PostgreSQL 데이터베이스 초기화 및 테이블 생성"""
    conn = None

    try:
        conn = get_db_connection()
        if conn is None:
            logging.error("Database connection failed")
            return None

        cursor = conn.cursor()

        # pod main data(name, ns) Table (다른 테이블에서 외래키로 참조)
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pod_info (
            pod_id SERIAL PRIMARY KEY,
            pod_name VARCHAR(255) NOT NULL,
            namespace VARCHAR(255)
        );
        """)

        # Process data (/proc/stat) Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS process_data (
            id SERIAL PRIMARY KEY,
            pod_id INTEGER REFERENCES pod_info(pod_id) ON DELETE CASCADE,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            pid INTEGER,
            comm VARCHAR(255),
            state VARCHAR(30),
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

        # bash_history Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS bash_history (
            id SERIAL PRIMARY KEY,
            pod_id INTEGER REFERENCES pod_info(pod_id) ON DELETE CASCADE,
            last_modified TIMESTAMP,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        #  ️pod status Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pod_status (
            id SERIAL PRIMARY KEY,
            pod_id INTEGER UNIQUE  REFERENCES pod_info(pod_id) ON DELETE CASCADE,
            creation_timestamp TIMESTAMP,
            deletion_timestamp TIMESTAMP,
            generate_name VARCHAR(255),
            node_name VARCHAR(255),
            phase VARCHAR(50),
            host_ip VARCHAR(50),
            pod_ip VARCHAR(50),
            start_time TIMESTAMP,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # pod lifecycle Table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS pod_lifecycle (
            id SERIAL PRIMARY KEY,
            pod_id INTEGER UNIQUE REFERENCES pod_info(pod_id) ON DELETE CASCADE,
            created_at TIMESTAMP,
            deleted_at TIMESTAMP,
            delete_reason TEXT,
            history_check BOOLEAN DEFAULT FALSE,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        conn.commit()

    except psycopg2.Error as e:
        logging.error(f"PostgreSQL Error: {e}")
    finally:
        if conn:
            conn.close()

def get_or_create_pod_id(pod_name, namespace):
    """pod data check and if existed data return pod_id, else create"""
    conn = None

    try:
        conn = get_db_connection()
        if conn is None:
            logging.error("Database connection failed")
            return None

        cursor = conn.cursor()

        cursor.execute("""
            SELECT pod_id FROM pod_info WHERE pod_name = %s AND namespace = %s;
        """, (pod_name, namespace))
        pod_ids = cursor.fetchall()

        if pod_ids:
            for (pod_id,) in pod_ids:
                # all pod_id's lifecycle check
                cursor.execute("""
                    SELECT deleted_at FROM pod_lifecycle
                    WHERE pod_id = %s ORDER BY created_at DESC LIMIT 1;
                """, (pod_id,))
                lifecycle = cursor.fetchone()

                if lifecycle is None:
                    # lifecycle 정보가 없다면 살아있는 것으로 간주
                    logging.info(f"Pod {pod_name} has no lifecycle info. Using pod_id: {pod_id}")
                    print("lifecycle 데이터가 없으므로, 기존 id 반환합니다.")
                    return pod_id
                elif lifecycle[0] is None:
                    # delete time is None -> not deleted pod
                    logging.info(f"Pod {pod_name} is active. Using pod_id: {pod_id}")
                    print("기존 id 반환합니다.")
                    return pod_id

            # All pod_id have deleted time -> create pod info
            logging.info(f"All existing pods with name {pod_name} are deleted. Creating new pod entry.")

        # No exist or deleted
        print("새로 만듭니다")
        cursor.execute("""
        INSERT INTO pod_info (pod_name, namespace) 
        VALUES (%s, %s) RETURNING pod_id;
        """, (pod_name, namespace))
        new_pod_id = cursor.fetchone()[0]
        conn.commit()

        logging.info(f"New pod inserted into DB: {pod_name}, namespace: {namespace}, pod_id: {new_pod_id}")
        return new_pod_id

    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"PostgreSQL Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def save_pod_status(pod_name, namespace, pod_info_obj):
    """Save new pod's status"""
    conn = None

    try:
        conn = get_db_connection()
        if conn is None:
            logging.error("Database connection failed")
            return None

        cursor = conn.cursor()

        pod_id = get_or_create_pod_id(pod_name, namespace)

        insert_query = """
        INSERT INTO pod_status (
            pod_id, creation_timestamp, deletion_timestamp,
            generate_name, node_name, phase,
            host_ip, pod_ip, start_time
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        ) ON CONFLICT (pod_id) DO UPDATE
        SET deletion_timestamp = EXCLUDED.deletion_timestamp,
            phase = EXCLUDED.phase, host_ip = EXCLUDED.host_ip,
            pod_ip = EXCLUDED.pod_ip, start_time = EXCLUDED.start_time;
        """

        values = (
            pod_id,
            pod_info_obj.creation_timestamp,
            pod_info_obj.deletion_timestamp,
            pod_info_obj.generate_name,
            pod_info_obj.node_name,
            pod_info_obj.phase,
            pod_info_obj.hostIP,
            pod_info_obj.podIP,
            pod_info_obj.startTime
        )

        cursor.execute(insert_query, values)
        conn.commit()

    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"PostgreSQL Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def save_pod_lifecycle(pod_name, namespace, lifecycle):
    """Save new pod's lifecycle (create time)"""
    conn = None

    try:
        conn = get_db_connection()
        if conn is None:
            logging.error("Database connection failed")
            return None

        cursor = conn.cursor()

        pod_id = get_or_create_pod_id(pod_name, namespace)

        insert_query = """
        INSERT INTO pod_lifecycle (
            pod_id, created_at
        ) VALUES (
            %s, %s
        ) ON CONFLICT (pod_id) 
        DO UPDATE SET created_at = EXCLUDED.created_at;
        """

        values = (pod_id, lifecycle.createTime)

        cursor.execute(insert_query, values)
        conn.commit()

    except psycopg2.Error as e:
        conn.rollback()
        logging.error(f"PostgreSQL Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()


def save_to_process(pod_name, namespace, processes):
    """process data save to DB"""
    conn = None

    try:
        conn = get_db_connection()
        if conn is None:
            logging.error("Database connection failed")
            return None  # 연결 실패 시 None 반환

        cursor = conn.cursor()

        pod_id = get_or_create_pod_id(pod_name, namespace)

        insert_query = """
        INSERT INTO process_data (
            pod_id, timestamp, pid, comm, state, ppid, pgrp, session, tty_nr, tpgid,
            flags, minflt, cminflt, majflt, cmajflt, utime, stime, cutime, cstime, priority,
            nice, num_threads, itrealvalue, starttime, vsize, rss, rsslim, startcode, endcode, startstack,
            kstkesp, kstkeip, signal, blocked, sigignore, sigcatch, wchan, nswap, cnswap, exit_signal,
            processor, rt_priority, policy, delayacct_blkio_ticks, guest_time, cguest_time, start_data, end_data, start_brk, arg_start,
            arg_end, env_start, env_end, exit_code
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s
        )
        """

        for process in processes:
            values = (
                pod_id, process['timestamp'], process['pid'], process['comm'], process['state'],
                process['ppid'], process['pgrp'], process['session'], process['tty_nr'], process['tpgid'],
                process['flags'], process['minflt'], process['cminflt'], process['majflt'], process['cmajflt'],
                process['utime'], process['stime'], process['cutime'], process['cstime'], process['priority'],
                process['nice'], process['num_threads'], process['itrealvalue'], process['starttime'], process['vsize'],
                process['rss'], process['rsslim'], process['startcode'], process['endcode'], process['startstack'],
                process['kstkesp'], process['kstkeip'], process['signal'], process['blocked'], process['sigignore'],
                process['sigcatch'], process['wchan'], process['nswap'], process['cnswap'], process['exit_signal'],
                process['processor'], process['rt_priority'], process['policy'], process['delayacct_blkio_ticks'],
                process['guest_time'], process['cguest_time'], process['start_data'], process['end_data'],
                process['start_brk'],
                process['arg_start'], process['arg_end'], process['env_start'], process['env_end'], process['exit_code']
            )

            cursor.execute(insert_query, values)
        conn.commit()
    except psycopg2.Error as e:
        logging.error(f"PostgreSQL Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def save_bash_history(pod_name, namespace, last_modified):
    """bash_history data seve to DB"""
    conn = None

    try:
        conn = get_db_connection()
        if conn is None:
            logging.error("Database connection failed")
            return None  # 연결 실패 시 None 반환

        cursor = conn.cursor()

        pod_id = get_or_create_pod_id(pod_name, namespace)

        cursor.execute("""
        INSERT INTO bash_history (pod_id, last_modified)
        VALUES (%s, %s);
        """, (pod_id, last_modified))

        conn.commit()
    except psycopg2.Error as e:
        logging.error(f"PostgreSQL Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def save_bash_history_result(pod_name, namespace, result):
    """save result checked hisotry in pod_lifecycle """
    conn = None

    try:
        conn = get_db_connection()
        if conn is None:
            logging.error("Database connection failed")
            return None  # 연결 실패 시 None 반환

        cursor = conn.cursor()

        pod_id = get_or_create_pod_id(pod_name, namespace)

        cursor.execute("""
            UPDATE pod_lifecycle
            SET history_check = %s,
                last_updated = CURRENT_TIMESTAMP
            WHERE pod_id = %s;
        """, (result, pod_id))

        conn.commit()
    except psycopg2.Error as e:
        logging.error(f"PostgreSQL Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def save_delete_reason(pod_name, namespace, lifecycle):
    conn = None

    try:
        conn = get_db_connection()
        if conn is None:
            logging.error("Database connection failed")
            return None  # 연결 실패 시 None 반환

        cursor = conn.cursor()

        pod_id = get_or_create_pod_id(pod_name, namespace)

        insert_query = """
        INSERT INTO pod_lifecycle (
            pod_id, deleted_at, delete_reason
        ) VALUES (
            %s, %s, %s
        ) ON CONFLICT (pod_id) 
        DO UPDATE SET 
            deleted_at = EXCLUDED.deleted_at,
            delete_reason = EXCLUDED.delete_reason,
            last_updated = CURRENT_TIMESTAMP;
        """

        values = (pod_id, lifecycle.deleteTime, lifecycle.reason_deletion)

        cursor.execute(insert_query, values)

        conn.commit()
    except psycopg2.Error as e:
        logging.error(f"PostgreSQL Error: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()


def get_last_bash_history(pod_name):
    """PostgreSQL에서 해당 pod의 마지막 bash_history 수정 시간을 가져옴"""
    conn = None

    try:
        conn = get_db_connection()
        if conn is None:
            logging.error("Database connection failed")
            return None  # 연결 실패 시 None 반환

        cursor = conn.cursor()

        cursor.execute("SELECT pod_id FROM pod_info WHERE pod_name = %s;", (pod_name,))
        pod_id = cursor.fetchone()

        if not pod_id:
            return None  # pod_name이 DB에 없으면 None 반환

        pod_id = pod_id[0]  # 실제 pod_id 값 추출

        cursor.execute("""
        SELECT last_modified FROM bash_history WHERE pod_id = %s ORDER BY id DESC LIMIT 1;
        """, (pod_id,))

        last_saved = cursor.fetchone()

        if last_saved and last_saved[0]:
            return last_saved[0]
        else:
            return None

    except psycopg2.Error as e:
        logging.error(f"PostgreSQL Error: {e}")
        return None
    finally:
        if conn:
            cursor.close()
            conn.close()

def is_deleted_in_DB(pod_name, namespace):
    conn = None

    try:
        conn = get_db_connection()
        if conn is None:
            logging.error("Database connection failed")
            return None  # 연결 실패 시 None 반환

        cursor = conn.cursor()

        # all pod_id(same pod) name check
        cursor.execute("SELECT pod_id FROM pod_info WHERE pod_name = %s;", (pod_name,))
        pod_ids = cursor.fetchall()

        if not pod_ids:
            return False  # pod_name이 DB에 없다면 삭제된 것으로 간주할 필요 없음

        for (pod_id,) in pod_ids:
            cursor.execute("""
                SELECT deleted_at FROM pod_lifecycle 
                WHERE pod_id = %s LIMIT 1;
            """, (pod_id,))
            result = cursor.fetchone()

            if result is not None and result[0] is None:
                return False

        return True  # 삭제 시간이 존재하면 True, 없으면 False

    except psycopg2.Error as e:
        logging.error(f"PostgreSQL Error: {e}")
        return None
    finally:
        if conn:
            cursor.close()
            conn.close()

def is_exist_in_DB(pod_name, namespace):
    conn = None

    try:
        conn = get_db_connection()
        if conn is None:
            logging.error("Database connection failed")
            return None  # 연결 실패 시 None 반환

        cursor = conn.cursor()

        # pod_name이 DB에 존재하는지 확인
        query = "SELECT EXISTS (SELECT 1 FROM pod_info WHERE pod_name = %s);"
        cursor.execute(query, (pod_name,))
        result = cursor.fetchone()

        return result[0] if result else False  # True or False 반환

    except psycopg2.Error as e:
        logging.error(f"PostgreSQL Error: {e}")
        return None
    finally:
        if conn:
            cursor.close()
            conn.close()
