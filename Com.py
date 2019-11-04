from collections import deque

from pyeventbus3.pyeventbus3 import PyBus
from pyeventbus3.pyeventbus3 import subscribe

from LamportClock import LamportClock
from Message import BroadcastMessage, DedicatedMessage


class Com:
    def __init__(self, process):
        self.process = process
        self.letterbox = deque()
        self.lamport_clock = LamportClock()
        PyBus.Instance().register(self, self)

    def __repr__(self):
        return f"[⚙ {self.process.getName()}]"

    def broadcast(self, data):
        self.lamport_clock.lock_clock()
        self.lamport_clock.increment()
        bm = BroadcastMessage(data=data, lamport_clock=self.lamport_clock, author=self.process.getName())
        print(f"{self} Broadcast => send: {data} {self.lamport_clock}")
        self.lamport_clock.unlock_clock()
        bm.post()

    @subscribe(onEvent=BroadcastMessage)
    def on_broadcast(self, m):
        if not isinstance(m, BroadcastMessage):
            print(f"{self} ONBroadcast => Invalid object type is passed.")
            return
        if m.author == self.process.getName():
            return
        data = m.getData()
        self.lamport_clock.lock_clock()
        self.lamport_clock.update(m)
        print(f"{self} ONBroadcast from {m.author} => received : {data} + {self.lamport_clock}")
        self.lamport_clock.unlock_clock()

    def send_to(self, data, id):
        self.lamport_clock.lock_clock()
        self.lamport_clock.increment()
        dm = DedicatedMessage(data=data, lamport_clock=self.lamport_clock, author=self.process.getName(), recipient=id)
        print(f"{self} DedicatedMessage => send: {data} to {id} {self.lamport_clock}")
        self.lamport_clock.unlock_clock()
        dm.post()

    @subscribe(onEvent=DedicatedMessage)
    def on_receive(self, m):
        if not isinstance(m, DedicatedMessage):
            print(f"{self} ONDedicatedMessage => Invalid object type is passed.")
            return
        if m.recipient != self.process.getName():
            # print(f"{self} ONDedicatedMessage => This message is not for me.")
            return
        data = m.getData()
        self.lamport_clock.lock_clock()
        self.lamport_clock.update(m)
        print(f"{self} ONDedicatedMessage from {m.author} => received: {data} {self.lamport_clock}")
        self.lamport_clock.unlock_clock()
