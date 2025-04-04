from enum import Enum

class Reason_Deletion(Enum):
    GC_h = 'GarbageCollector - No usage history for more than a week'
    UNKNOWN = "Pod deleted"

class Pod_Info:
    def __init__(self):
        """pod.metadata"""
        # json 형식의 값들은 모두 주석 처리
        self.uid = None  # pod uid
        # self.labels = None
        # self.annotations = None
        self.creation_timestamp = None
        self.deletion_timestamp = None  # from metadata
        self.generate_name = None  # pod 이름 자동 생성 시 사용되는 접두사
        # self.owner_references = None
        # self.finalizers = None  # pod 삭제 시 특정 동작을 수행힉 위한 리스트
        # self.managed_fields = None  # API 서버가 리소스 변경 이력을 추적하는 정보

        """pod.spec"""
        # self.volumes = None  # mounted volumes list
        # self.containers = None  # Pod's containers list
        self.node_name = None

        """pod.status"""
        self.phase = None  # pod status
        # self.conditions = None  # pod의 상태 조건 리스트
        self.hostIP = None  # node ip
        self.podIP = None
        self.startTime = None  # pod start time

class Pod_Lifecycle:
    def __init__(self):
        self.createTime = None  # pod 생성 시간
        self.deleteTime = None  # pod 삭제 시간
        self.reason_deletion = None  # Reason for pod deletion
