#!/bin/bash

MODE=${MODE:-mixed}         # 실행 모드: active / idle / mixed
MAX_PROCS=${NUM_PROCS:-1}   # 기본값은 1

# 유효성 검사: 숫자이며 1 이상인지 확인
if [[ "$MAX_PROCS" =~ ^[0-9]+$ ]] && [ "$MAX_PROCS" -ge 1 ]; then
    NUM_PROCS=$MAX_PROCS
else
    echo "Invalid NUM_PROCS: $MAX_PROCS. Using default of 1."
    NUM_PROCS=1
fi

echo "Pod starting with MODE=$MODE and NUM_PROCS=$NUM_PROCS"

for i in $(seq 1 $NUM_PROCS); do
    if [ "$MODE" == "active" ]; then
        DIR="./active_programs"
    elif [ "$MODE" == "idle" ]; then
        DIR="./idle_programs"
    else  # mixed
        if (( RANDOM % 2 == 0 )); then
            DIR="./active_programs"
        else
            DIR="./idle_programs"
        fi
    fi

    FILES=($DIR/*.py)
    SELECTED=${FILES[$((RANDOM % ${#FILES[@]}))]}
    echo "Launching process $i: $SELECTED"
    python "$SELECTED" &
done

wait