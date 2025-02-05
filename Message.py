from pyeventbus3.pyeventbus3 import PyBus


class Message:
    """Generic Message class"""
    def __init__(self, payload, *, lamport_clock, tag=None):
        self.payload = payload
        self.stamp = lamport_clock.clock
        self.tag = tag

    def post(self):
        """Post this message on the bus"""
        PyBus.Instance().post(self)

    def __repr__(self):
        return f"<Message: {self.payload}>"


class BroadcastMessage(Message):
    def __init__(self, data, *, lamport_clock, author, tag=None):
        super().__init__(data, lamport_clock=lamport_clock, tag=tag)
        self.author = author


class BroadcastMessageSync(BroadcastMessage):
    pass


class JoinMessage(BroadcastMessageSync):
    pass


class Synchronize(BroadcastMessage):
    def __init__(self, *, lamport_clock, author):
        super().__init__("", lamport_clock=lamport_clock, author=author)


class DedicatedMessage(Message):
    def __init__(self, data, *, lamport_clock, author, recipient, tag=None):
        super().__init__(data, lamport_clock=lamport_clock, tag=tag)
        self.author = author
        self.recipient = recipient


class DedicatedMessageSync(DedicatedMessage):
    pass


class SynchronizeAck(DedicatedMessage):
    def __init__(self, *, lamport_clock, author, recipient, tag=None):
        super().__init__("", lamport_clock=lamport_clock, author=author, recipient=recipient, tag=tag)


class DedicatedMessageSyncAck(SynchronizeAck):
    pass


class BroadcastSyncAck(SynchronizeAck):
    pass


class Heartbit(BroadcastMessage):
    pass


class Token(DedicatedMessage):
    def __init__(self, *, lamport_clock, author, recipient, min_wait):
        super().__init__("", lamport_clock=lamport_clock, author=author, recipient=recipient)
        self.min_wait = min_wait
