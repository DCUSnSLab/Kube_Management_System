from enum import Enum


class Mode_State(Enum):
    R = 'Running'
    S = 'Sleeping'
    D = 'Uninterruptible Sleep'
    Z = 'Zombie'
    T = 'Stopped'
class Process:
    def __init__(self):
        self.PID = None
        self.comm = None
        self.state = Mode_State.R
        self.ppid = None
