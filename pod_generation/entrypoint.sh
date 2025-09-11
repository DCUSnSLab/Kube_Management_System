#!/bin/bash

# 환경 변수 설정
PROCESS_STATE=${PROCESS_STATE:-active}  # active / idle / running / background_active
NUM_PROCS=${NUM_PROCS:-1}               # 실행할 프로세스 개수
PROCESS_MIX=${PROCESS_MIX:-single}      # single / diverse (프로세스 조합 방식)

# 유효성 검사
if [[ "$NUM_PROCS" =~ ^[0-9]+$ ]] && [ "$NUM_PROCS" -ge 1 ]; then
    PROC_COUNT=$NUM_PROCS
else
    echo "Invalid NUM_PROCS: $NUM_PROCS. Using default of 1."
    PROC_COUNT=1
fi

echo "================================================"
echo "Pod Configuration:"
echo "  - Process State: $PROCESS_STATE"
echo "  - Number of Processes: $PROC_COUNT"
echo "  - Process Mix: $PROCESS_MIX"
echo "  - Pod Start Time: $(date)"
echo "================================================"

# 상태별 디렉토리 매핑
case "$PROCESS_STATE" in
    active)
        DIR="./programs/active"
        ;;
    idle)
        DIR="./programs/idle"
        ;;
    running)
        DIR="./programs/running"
        ;;
    background_active)
        DIR="./programs/background_active"
        ;;
    *)
        echo "Error: Unknown PROCESS_STATE: $PROCESS_STATE"
        echo "Valid options: active, idle, running, background_active, foreground_active"
        exit 1
        ;;
esac

# 프로세스 시작 함수
launch_process() {
    local selected_file=$1
    local proc_num=$2

    echo "[Process $proc_num] Launching: $(basename $selected_file)"

    # 프로세스별 환경 변수 설정
    PROC_ID=$proc_num python "$selected_file" &

    # PID 저장
    echo "[Process $proc_num] Started with PID: $!"
}

# diverse 모드: 다양한 상태의 프로세스 혼합
if [ "$PROCESS_MIX" == "diverse" ]; then
    echo "Starting diverse process mix..."

    # 모든 상태 디렉토리
    ALL_DIRS=(
        "./programs/active"
        "./programs/idle"
        "./programs/running"
        "./programs/background_active"
    )

    for i in $(seq 1 $PROC_COUNT); do
        # 랜덤하게 상태 선택
        SELECTED_DIR=${ALL_DIRS[$((RANDOM % ${#ALL_DIRS[@]}))]}

        # 해당 디렉토리에서 랜덤 파일 선택
        FILES=($SELECTED_DIR/*.py)
        if [ ${#FILES[@]} -gt 0 ] && [ -f "${FILES[0]}" ]; then
            SELECTED_FILE=${FILES[$((RANDOM % ${#FILES[@]}))]}
            launch_process "$SELECTED_FILE" "$i"
        else
            echo "Warning: No Python files found in $SELECTED_DIR"
        fi

        # 프로세스 시작 간격
        sleep 0.5
    done
else
    # single 모드: 동일한 상태의 프로세스들
    echo "Starting $PROCESS_STATE processes..."

    FILES=($DIR/*.py)
    if [ ${#FILES[@]} -eq 0 ] || [ ! -f "${FILES[0]}" ]; then
        echo "Error: No Python files found in $DIR"
        exit 1
    fi

    for i in $(seq 1 $PROC_COUNT); do
        # 랜덤 파일 선택
        SELECTED_FILE=${FILES[$((RANDOM % ${#FILES[@]}))]}
        launch_process "$SELECTED_FILE" "$i"

        # 프로세스 시작 간격
        sleep 0.5
    done
fi

echo "================================================"
echo "All processes launched. Container will keep running..."
echo "================================================"

# 프로세스 모니터링 함수
monitor_processes() {
    while true; do
        # 30초마다 프로세스 상태 체크
        sleep 30

        # 실행 중인 프로세스 수 확인
        RUNNING=$(jobs -r | wc -l)

        if [ $RUNNING -gt 0 ]; then
            echo "[Monitor] $(date): $RUNNING processes running"

            # 상세 정보 출력 (디버깅용, 주석 처리 가능)
            # ps aux | grep -E "python.*\.py" | grep -v grep
        else
            echo "[Monitor] $(date): No processes running, container will exit"
            exit 0
        fi
    done
}

# 백그라운드에서 모니터링 시작
monitor_processes &

# 모든 자식 프로세스가 종료될 때까지 대기
wait
