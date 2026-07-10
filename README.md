# Kube_Management_System

Kubernetes 네임스페이스에서 **유휴(비활성) Pod 를 자동으로 탐지·회수(Garbage Collection)** 하는 시스템.
SSH 실습용 Pod 처럼 오래 방치되는 컨테이너를 프로세스 활동 + 명령어 히스토리 기준으로 판단해 정리하고,
그 과정의 프로세스/리소스 지표를 PostgreSQL 에 적재한다.

## 동작 개요

`GarbageCollector` 가 지정 네임스페이스를 **60초 주기(fixed-rate)** 로 순회하며 다음을 수행한다.

1. **Pod 목록 갱신** — 네임스페이스의 Running Pod 조회, `exclude` 목록 제외
2. **데이터 수집(병렬)** — Pod 마다 `exec` 로 `/proc/[pid]/stat`(+status/io) 와 `~/.bash_history` mtime 수집
3. **Pod 정보/프로세스 DB 저장** + **리소스 사용량(CPU/MEM) 수집·저장**
4. **GC 판단(병렬)** 및 삭제

수집·판단 단계는 `ThreadPoolExecutor`(기본 worker 10) 로 Pod 단위 병렬 처리한다.

### GC 판단 로직

Pod 는 **히스토리 기준과 프로세스 기준이 모두 비활성일 때만** 삭제된다 (AND 조건).

| 기준 | 비활성 판정 |
|---|---|
| **명령어 히스토리** | `~/.bash_history` 가 7일 이상 미변경 **그리고** Pod 생성 후 7일 경과 |
| **프로세스** | 활성 프로세스 0개 상태가 `Inactive_Threshold_s`(기본 배포값 20분) 이상 지속 |

프로세스 활성 판정은 상태(Running/Uninterruptible)와 사이클 간 delta(CPU time, context switch, RSS, minor fault, I/O)를 종합해 결정한다 (`processManager.py`).

## 구성 요소

| 파일 | 역할 |
|---|---|
| `garbagecollector.py` | 메인 루프, Pod 목록 관리, 병렬 수집/판단/삭제 오케스트레이션 |
| `pod.py` | Pod 단위 상태·수집·분석·DB 저장 래퍼 |
| `processManager.py` | `/proc` 기반 프로세스 수집 및 활성/비활성 분류 |
| `historyManager.py` | `~/.bash_history` mtime 기반 히스토리 활성 판정 |
| `resourceCollector.py` | Metrics API 로 CPU/MEM 사용량 + requests/limits 수집 |
| `DB_postgresql.py` | PostgreSQL 연결 및 테이블 초기화/저장 함수 |
| `poddata.py`, `process.py` | 데이터 모델(dataclass/enum) |

## 데이터 모델 (PostgreSQL)

`initialize_database()` 가 다음 테이블을 생성한다.

- `pod_info` — Pod 기본 정보(name/namespace), 나머지 테이블의 FK 대상
- `pod_status` / `pod_lifecycle` — Pod 상태 스냅샷 / 생성·삭제 이력 및 사유
- `process_data` + `process_metrics` — 프로세스별 `/proc/stat` 및 ctxt/mem/io 지표
- `pod_analysis` + `process_classification` — 사이클별 활성/비활성 분석 요약 및 프로세스 분류
- `bash_history` — 명령어 히스토리 최종 사용 시각
- `namespace_resource_usage` / `pod_resource_usage` — CPU/MEM 사용량 시계열

## 설정

DB 접속 정보는 `config.ini` 로 관리한다. 아래 형식으로 **각 환경에 맞는 값을 채워** 저장한다.

```ini
[database]
dbname = <database name>
user     = <username>
password = <password>
host     = <db host>
port     = <port>
```

> 실제 자격증명은 저장소에 커밋하지 말 것. 배포 환경에서는 Kubernetes Secret 이나 환경변수로 주입하는 것을 권장한다.

주요 실행 파라미터는 `garbagecollector.py` 하단에서 지정한다.

```python
gc = GarbageCollector(namespace='swlabpods', isDev=False, Inactive_Threshold_s=20 * 60)
gc.manage()          # interval=60, worker=10
```

- `namespace`: 관리(회수) 대상 네임스페이스
- `exclude`: 회수 대상에서 제외할 Pod 이름 목록 (`GarbageCollector.__init__` 에서 지정)
- `Inactive_Threshold_s`: 비활성 지속 시간 임계값(초)

## 실행

### 로컬 실행

```bash
pip install -r requirements.txt   # kubernetes, psycopg2-binary
# ~/.kube/config 로 클러스터 접근 가능해야 함
python garbagecollector.py
```

클러스터 내부(Pod)에서 실행 시 `load_incluster_config()` 가 우선 적용되고, 실패하면 `~/.kube/config` 로 폴백한다.

### 컨테이너 빌드

```bash
docker build -t harbor.cu.ac.kr/kube_management_system/gc:latest .
docker push harbor.cu.ac.kr/kube_management_system/gc:latest
```

## 배포 (in-cluster)

`deploy/gc-deployment.yaml` 로 배포한다. GC 는 `k8s-gc` 네임스페이스에서 구동되며, 대상(`swlabpods`) 네임스페이스에
대한 권한을 크로스 네임스페이스 RoleBinding 으로 부여받는다.

```bash
kubectl create namespace k8s-gc      # 매니페스트에 Namespace 오브젝트 없음 — 사전 생성 필요
kubectl apply -f deploy/gc-deployment.yaml
kubectl -n k8s-gc rollout status deployment/garbage-collector
kubectl -n k8s-gc logs deploy/garbage-collector -f
```

부여되는 RBAC (대상 네임스페이스 `swlabpods`):
- `pods`: get/list/**delete**
- `pods/exec`: create/get (프로세스·히스토리 수집)
- `persistentvolumeclaims`: list
- `metrics.k8s.io/pods`: get/list (리소스 사용량)

> 전제: 클러스터에 **metrics-server** 가 설치되어 있어야 리소스 수집이 동작한다.

## CI/CD (Jenkins)

`Jenkinsfile` — 멀티브랜치 파이프라인, `main` 브랜치에서만 빌드·배포한다.

1. **Build & Push** — `docker.build` 로 이미지 빌드 후 Harbor 에 `${BUILD_NUMBER}-${GIT_SHA}` + `latest` 태그로 푸시
2. **Deploy** — `kubectl apply -f deploy/gc-deployment.yaml` → `set image`(불변 태그) → `rollout status`

필요 사항: Harbor 크리덴셜 ID `harbor`, Docker/kubectl 이 설치된 에이전트(`agent any`).

## 알려진 제약

- `~/.bash_history` 가 없는 Pod 는 `exec` 의 `stat` 오류가 로그에 남으며 히스토리상 활성으로 간주된다.
- Pod 재시작 시 비활성 지속시간(`podInactiveSince`) 등 인메모리 상태가 초기화된다.
- 병렬 수집 로그가 스레드 간 인터리브되어 출력될 수 있다.
