from threading import Lock

class LamportClock:

    def __init__(self):
        self.clock = 0
        self.mutex = Lock()

    def increment(self):
        self.clock += 1

    def update(self, event):
        self.clock = max(self.clock, event.getStamp()) + 1

    def lock_clock(self):
        self.mutex.acquire()

    def unlock_clock(self):
        self.mutex.release()

    def __repr__(self):
        return f"⏱ {self.clock}"
