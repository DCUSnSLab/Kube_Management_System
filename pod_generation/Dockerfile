FROM python:3.10-slim

# 필수 패키지 설치 (ps 명령어 포함)
RUN apt-get update && apt-get install -y procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# 작업 디렉토리 설정
WORKDIR /app

# 파이썬 코드 복사
COPY active_programs ./active_programs
COPY idle_programs ./idle_programs

# entrypoint 복사 및 실행권한 부여
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# 엔트리포인트 설정
ENTRYPOINT ["/entrypoint.sh"]
