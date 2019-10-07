class Event:
    def __init__(self, data, *, topic="default", lamport_clock):
        self.data = data
        self.topic = topic
        self.estampile = lamport_clock.clock

    def getData(self):
        return self.data

    def getTopic(self):
        return self.topic

    def getEstampile(self):
        return self.estampile
