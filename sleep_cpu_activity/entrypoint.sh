#!/bin/bash

# 환경 변수 설정
PROCESS_STATE=${PROCESS_STATE:-active}        # active / idle

echo "================================================"
echo "Sleep State Scenario Configuration:"
echo "  - Process State: $PROCESS_STATE"
echo "  - Container Start Time: $(date)"
echo "================================================"

# 시나리오별 프로그램 선택
case "$PROCESS_STATE" in
    active)
        FILES=(./programs/active*.py)
        if [ ${#FILES[@]} -eq 0 ]; then
            echo "Error: No active*.py files found in ./programs/"
            exit 1
        fi
        PROGRAM=${FILES[$((RANDOM % ${#FILES[@]}))]}
        DESCRIPTION="활성 상태 (active*) 시나리오"
        ;;
    idle)
        PROGRAM="./programs/idle_sleep.py"
        DESCRIPTION="비활성 상태이면서 Sleep(S)로 유지되는 경우"
        ;;
    *)
        echo "Error: Unknown PROCESS_STATE: $PROCESS_STATE"
        echo "Valid options: active, idle"
        exit 1
        ;;
esac

echo "Selected Scenario: $DESCRIPTION"
echo "Program: $PROGRAM"

# 프로세스 시작
echo "================================================"
echo "Starting scenario process..."

if [ ! -f "$PROGRAM" ]; then
    echo "Error: Program file not found: $PROGRAM"
    exit 1
fi

# 시나리오 프로그램 실행
python "$PROGRAM" &
MAIN_PID=$!

echo "Main process started with PID: $MAIN_PID"
echo "Program: $(basename $PROGRAM)"
echo "================================================"
echo "Container is now running. External monitoring system will check /proc/pid/stat."
echo "================================================"

# 시그널 핸들러 설정
cleanup() {
    echo "\nReceived termination signal"
    echo "Cleaning up processes..."

    if kill -0 $MAIN_PID 2>/dev/null; then
        kill -TERM $MAIN_PID 2>/dev/null
        wait $MAIN_PID 2>/dev/null
    fi
    
    echo "Cleanup completed. Exiting..."
    exit 0
}

trap cleanup SIGTERM SIGINT

# 메인 프로세스가 종료될 때까지 대기
wait $MAIN_PID
