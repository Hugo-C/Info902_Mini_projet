from pyeventbus3.pyeventbus3 import PyBus


# Base class, used by other Message class
class Event:
    def __init__(self, data, *, lamport_clock):
        self.data = data
        self.estampile = lamport_clock.clock

    def getData(self):
        return self.data

    def getEstampile(self):
        """Return the estampile or lamport clock value of the event"""
        return self.estampile

    def post(self):
        """Post this message on the bus"""
        PyBus.Instance().post(self)


class BroadcastMessage(Event):
    def __init__(self, data, *, lamport_clock, author):
        super().__init__(data, lamport_clock=lamport_clock)
        self.author = author


class Synchronize(BroadcastMessage):
    def __init__(self, *, lamport_clock, author):
        super().__init__("", lamport_clock=lamport_clock, author=author)


class DedicatedMessage(Event):
    def __init__(self, data, *, lamport_clock, author, recipient):
        super().__init__(data, lamport_clock=lamport_clock)
        self.author = author
        self.recipient = recipient


class SynchronizeAck(DedicatedMessage):
    def __init__(self, *, lamport_clock, author, recipient):
        super().__init__("", lamport_clock=lamport_clock, author=author, recipient=recipient)


class Token(DedicatedMessage):
    def __init__(self, *, lamport_clock, author, recipient, min_wait):
        super().__init__("", lamport_clock=lamport_clock, author=author, recipient=recipient)
        self.min_wait = min_wait

    def update_lamport_clock(self, lamport_clock):
        """Update the lamport clock since a token will be send several times"""
        self.estampile = lamport_clock.clock


