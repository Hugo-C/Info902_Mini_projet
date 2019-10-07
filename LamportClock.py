class LamportClock:

    def __init__(self):
        self.clock = 0

    def increment(self):
        self.clock += 1

    def update(self, received_clock):
        self.clock = max(self.clock, received_clock) + 1

    def __repr__(self):
        return "clock : " + str(self.clock)
