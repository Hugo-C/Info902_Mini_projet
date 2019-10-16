class LamportClock:

    def __init__(self):
        self.clock = 0

    def increment(self):
        self.clock += 1

    def update(self, event):
        self.clock = max(self.clock, event.getEstampile()) + 1

    def __repr__(self):
        return f"⏱ {self.clock}"
