class Event:
    def __init__(self, data, *, lamport_clock):
        self.data = data
        self.estampile = lamport_clock.clock

    def getData(self):
        return self.data

    def getEstampile(self):
        return self.estampile

class BroadcastMessage(Event):
    def __init__(self, data, *, lamport_clock, author):
        super().__init__(data, lamport_clock=lamport_clock)
        self.author = author

    def getAuthor(self):
        return self.author

