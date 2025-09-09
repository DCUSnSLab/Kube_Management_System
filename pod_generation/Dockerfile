FROM python:3.10-slim

# 필수 패키지 설치
RUN apt-get update && apt-get install -y \
    procps \
    net-tools \
    htop \
    strace \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 작업 디렉토리 설정
WORKDIR /app

# 프로그램 디렉토리 구조 복사
COPY programs ./programs

# entrypoint 복사 및 실행권한 부여
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# 환경 변수 기본값 설정
ENV PROCESS_STATE=active
ENV NUM_PROCS=1
ENV PROCESS_MIX=single

# 헬스체크 추가 (선택적)
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD pgrep -f "python.*\.py" || exit 1

# 엔트리포인트 설정
ENTRYPOINT ["/entrypoint.sh"]
