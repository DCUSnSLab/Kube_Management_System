import time
import sys
import signal
import random
from collections import deque
import os

"""
Running Process - Event Loop
이벤트 루프 기반 실행 프로세스
"""

def signal_handler(sig, frame):
    print("\nEvent loop process terminated")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

class Event:
    def __init__(self, event_type, data, timestamp):
        self.type = event_type
        self.data = data
        self.timestamp = timestamp
        self.processed = False

class EventLoop:
    def __init__(self):
        self.events = deque()
        self.handlers = {
            'timer': self.handle_timer,
            'data': self.handle_data,
            'signal': self.handle_signal,
            'state': self.handle_state
        }
        self.state = {'counter': 0, 'total': 0}
    
    def handle_timer(self, event):
        """타이머 이벤트 처리"""
        self.state['counter'] += 1
        return f"Timer tick {self.state['counter']}"
    
    def handle_data(self, event):
        """데이터 이벤트 처리"""
        value = sum(event.data) / len(event.data) if event.data else 0
        self.state['total'] += value
        return f"Data processed: {value:.2f}"
    
    def handle_signal(self, event):
        """시그널 이벤트 처리"""
        return f"Signal received: {event.data}"
    
    def handle_state(self, event):
        """상태 이벤트 처리"""
        return f"State: counter={self.state['counter']}, total={self.state['total']:.2f}"
    
    def add_event(self, event):
        self.events.append(event)
    
    def process_events(self):
        processed = 0
        while self.events and processed < 10:  # 한 번에 최대 10개 처리
            event = self.events.popleft()
            
            if event.type in self.handlers:
                result = self.handlers[event.type](event)
                event.processed = True
                processed += 1
        
        return processed

def generate_events(event_loop):
    """이벤트 생성"""
    event_types = ['timer', 'data', 'signal', 'state']
    
    for event_type in random.choices(event_types, weights=[4, 3, 2, 1], k=random.randint(1, 3)):
        if event_type == 'data':
            data = [random.random() * 100 for _ in range(5)]
        else:
            data = f"{event_type}_data_{random.randint(1, 100)}"
        
        event = Event(event_type, data, time.time())
        event_loop.add_event(event)

def main():
    print(f"[RUNNING] Event Loop Started - PID: {os.getpid()}", flush=True)
    
    event_loop = EventLoop()
    cycle_count = 0
    total_processed = 0
    
    while True:
        cycle_count += 1
        
        # 이벤트 생성
        generate_events(event_loop)
        
        # 이벤트 처리
        processed = event_loop.process_events()
        total_processed += processed
        
        # 상태 출력
        if cycle_count % 30 == 0:
            pending = len(event_loop.events)
            print(f"[RUNNING] Cycle {cycle_count}: Processed {total_processed} events, Pending: {pending}", flush=True)
        
        # 이벤트 루프 속도
        time.sleep(0.05)

if __name__ == "__main__":
    main()
