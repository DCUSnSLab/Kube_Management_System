import os
import sys
import time
import signal
import tempfile
import random

"""
Active Process - I/O Intensive 
"""

def signal_handler(sig, frame):
    print("\nI/O intensive process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def simulate_vim_save(work_dir, iteration):
    """에디터 저장 작업 시뮬레이션 (vim :w)"""
    filename = os.path.join(work_dir, f"student_code_{iteration % 5}.c")
    # C 코드 작성 시뮬레이션
    code_lines = [
        "#include <stdio.h>\n",
        "#include <stdlib.h>\n",
        f"// Modified at iteration {iteration}\n",
        "int main() {\n",
        f"    int array[{random.randint(100, 500)}];\n",
        "    for(int i = 0; i < 100; i++) {\n",
        "        array[i] = i * i;\n",
        "    }\n",
        "    return 0;\n",
        "}\n"
    ]
    
    with open(filename, 'w') as f:
        for line in code_lines:
            f.write(line)
        f.flush()
        os.fsync(f.fileno())
    return filename

def simulate_file_operations(work_dir, iteration):
    """파일 복사, 이동 작업 (cp, mv)"""
    src_file = os.path.join(work_dir, f"data_{iteration % 10}.txt")
    dst_file = os.path.join(work_dir, f"backup_{iteration % 10}.txt")
    
    # 파일 생성 및 쓰기
    with open(src_file, 'w') as f:
        data = ''.join(str(random.randint(0, 9)) for _ in range(5000))
        f.write(f"Data iteration {iteration}: {data}\n")
        f.flush()
        os.fsync(f.fileno())
    
    # 파일 복사 시뮬레이션
    if os.path.exists(src_file):
        with open(src_file, 'rb') as src:
            content = src.read()
        with open(dst_file, 'wb') as dst:
            dst.write(content)
            dst.flush()
            os.fsync(dst.fileno())
    
    return src_file, dst_file

def simulate_grep_find(work_dir):
    """검색 명령어 시뮬레이션 (grep, find)"""
    # 여러 파일 읽기 작업
    files_read = 0
    for filename in os.listdir(work_dir):
        if filename.endswith('.txt') or filename.endswith('.c'):
            filepath = os.path.join(work_dir, filename)
            if os.path.exists(filepath):
                try:
                    with open(filepath, 'r') as f:
                        content = f.read()
                        # 패턴 검색 시뮬레이션
                        if 'main' in content or 'data' in content:
                            files_read += 1
                except:
                    pass
    return files_read

def simulate_tar_operation(work_dir, iteration):
    """tar 압축 작업 시뮬레이션"""
    # 여러 파일 생성
    files = []
    for i in range(3):
        filename = os.path.join(work_dir, f"project_file_{iteration}_{i}.txt")
        with open(filename, 'w') as f:
            f.write(f"Project data {i}\n" * 500)
            f.flush()
        files.append(filename)
    
    # 아카이브 시뮬레이션 (파일들을 하나의 큰 파일로)
    archive = os.path.join(work_dir, f"archive_{iteration}.tar")
    with open(archive, 'wb') as tar:
        for file in files:
            with open(file, 'rb') as f:
                tar.write(f.read())
    
    # 원본 파일 삭제
    for file in files:
        try:
            os.remove(file)
        except:
            pass
    
    return archive

def main():
    print(f"[ACTIVE-IO] Student File Operations Started - PID: {os.getpid()}", flush=True)
    temp_dir = tempfile.mkdtemp(prefix="student_workspace_")
    print(f"[ACTIVE-IO] Working directory: {temp_dir}", flush=True)
    print(f"[ACTIVE-IO] Simulating: vim, cp, mv, grep, find, tar", flush=True)
    
    iteration = 0
    activities = ["vim_edit", "file_copy", "grep_search", "tar_archive"]
    
    while True:
        iteration += 1
        activity = random.choice(activities)
        
        try:
            if activity == "vim_edit":
                # vim 편집 시뮬레이션
                filename = simulate_vim_save(temp_dir, iteration)
                print(f"[ACTIVE-IO] vim {os.path.basename(filename)} :w (iter {iteration})", flush=True)
                time.sleep(0.1)
                
            elif activity == "file_copy":
                # 파일 복사/이동
                src, dst = simulate_file_operations(temp_dir, iteration)
                print(f"[ACTIVE-IO] cp {os.path.basename(src)} {os.path.basename(dst)} (iter {iteration})", flush=True)
                time.sleep(0.15)
                
            elif activity == "grep_search":
                # grep/find 검색
                files_found = simulate_grep_find(temp_dir)
                print(f"[ACTIVE-IO] grep -r 'main' ./ found {files_found} files (iter {iteration})", flush=True)
                time.sleep(0.05)
                
            else:  # tar_archive
                # tar 압축
                archive = simulate_tar_operation(temp_dir, iteration)
                print(f"[ACTIVE-IO] tar -czf {os.path.basename(archive)} * (iter {iteration})", flush=True)
                time.sleep(0.2)
            
            # 주기적으로 상태 정보 출력
            if iteration % 5 == 0:
                print(f"[ACTIVE-IO-STATUS] Process State: D (Disk I/O), I/O Wait Active", flush=True)
                print(f"[ACTIVE-IO-STATUS] Files in workspace: {len(os.listdir(temp_dir))}", flush=True)
                
        except Exception as e:
            print(f"[ACTIVE-IO-ERROR] Operation failed: {e}", flush=True)
            time.sleep(0.5)

if __name__ == "__main__":
    main()
