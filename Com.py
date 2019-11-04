from collections import deque
from enum import Enum
from time import sleep

from pyeventbus3.pyeventbus3 import Mode
from pyeventbus3.pyeventbus3 import PyBus
from pyeventbus3.pyeventbus3 import subscribe

from LamportClock import LamportClock
from Message import BroadcastMessage, DedicatedMessage, Token, Synchronize, SynchronizeAck, BroadcastMessageSync, \
    BroadcastSyncAck

State = Enum("State", "REQUEST SC RELEASE")
PROCESS_NUMBER = 3


class Com:
    def __init__(self, process):
        self.process = process
        self.letterbox = deque()
        self.lamport_clock = LamportClock()
        self.state = None
        self.answered_process = set()
        self.answered_process_broadcast_sync = set()

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
        data = m.get_payload()
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
        data = m.get_payload()
        self.lamport_clock.lock_clock()
        self.lamport_clock.update(m)
        print(f"{self} ONDedicatedMessage from {m.author} => received: {data} {self.lamport_clock}")
        self.lamport_clock.unlock_clock()

    def send_token(self, t):  # TODO  test with a custom thread
        process_position = int(self.process.getName())
        t.recipient = str((process_position + 1) % PROCESS_NUMBER)
        t.author = self.process.getName()
        t.update_lamport_clock(self.lamport_clock)
        print(f"{self} Token => send token to {t.recipient} {self.lamport_clock}")
        t.post()

    @subscribe(threadMode=Mode.PARALLEL, onEvent=Token)
    def on_token(self, token):
        if not self.process.alive:
            return
        if token.recipient != self.process.getName():
            return

        assert self.state != State.SC, "Error : unstable state ! " + self.process.getName()
        assert self.state != State.RELEASE, "Error : unstable state ! " + self.process.getName()
        print(f"{self} ONToken => received token from {token.author} {self.lamport_clock}")
        if self.state is None:
            sleep(token.min_wait)
        elif self.state == State.REQUEST:
            self.state = State.SC
            while self.state != State.RELEASE:
                sleep(token.min_wait)
        self.state = None
        self.send_token(token)

    def request_sc(self):
        self.state = State.REQUEST
        print(f"{self} REQUEST => state : {self.state}")
        while self.state != self.state.SC and self.process.alive:
            sleep(1)
        print(f"{self} REQUEST => state : {self.state}")

    def release_sc(self):
        assert self.state == State.SC, "Error : unstable state !"
        self.state = State.RELEASE
        print(f"{self} RELEASE => state : {self.state}")

    def synchronize(self):
        self.answered_process = set()
        self.lamport_clock.lock_clock()
        self.lamport_clock.increment()
        m = Synchronize(lamport_clock=self.lamport_clock, author=self.process.getName())
        print(f"{self} Synchronize => {self.lamport_clock}")
        self.lamport_clock.unlock_clock()
        m.post()
        while len(self.answered_process) < PROCESS_NUMBER - 1 and self.process.alive:
            sleep(1)

    @subscribe(onEvent=Synchronize)
    def on_synchronize(self, m):
        if not isinstance(m, Synchronize):
            print(f"{self} Synchronize => {self.process.getName()} Invalid object type is passed.")
            return
        if m.author == self.process.getName():
            return
        self.lamport_clock.lock_clock()
        self.lamport_clock.update(m)
        print(f"{self} Synchronize from {m.author} => {self.lamport_clock}")
        self.lamport_clock.unlock_clock()
        self.synchronize_ack(m.author)

    def synchronize_ack(self, recipient):
        self.lamport_clock.lock_clock()
        self.lamport_clock.increment()
        m = SynchronizeAck(lamport_clock=self.lamport_clock, author=self.process.getName(), recipient=recipient)
        print(f"{self} SynchronizeAck => respond to {recipient} {self.lamport_clock}")
        self.lamport_clock.unlock_clock()
        m.post()

    @subscribe(threadMode=Mode.PARALLEL, onEvent=SynchronizeAck)
    def on_synchronize_ack(self, m):
        if not isinstance(m, SynchronizeAck):
            print(f"{self} SynchronizeAck => {self.process.getName()} Invalid object type is passed.")
            return
        if m.recipient != self.process.getName():
            return
        self.lamport_clock.lock_clock()
        self.lamport_clock.update(m)
        print(f"{self} SynchronizeAck from {m.author} => {self.lamport_clock}")
        self.lamport_clock.unlock_clock()
        self.answered_process.add(m.author)

    def broadcast_sync(self, data, from_id):
        if from_id == self.process.getName():
            self.answered_process_broadcast_sync = set()
            self.lamport_clock.lock_clock()
            self.lamport_clock.increment()
            bm = BroadcastMessageSync(data=data, lamport_clock=self.lamport_clock, author=self.process.getName())
            print(f"{self} Broadcast => send: {data} {self.lamport_clock}")
            self.lamport_clock.unlock_clock()
            bm.post()

            while len(self.answered_process_broadcast_sync) != PROCESS_NUMBER - 1 and self.process.is_alive():
                sleep(1)
        else:  # wait for a broadcast message from from_id
            while len(self.letterbox) == 0 and self.process.is_alive():
                sleep(1)
            m: BroadcastMessageSync = self.letterbox.popleft()
            print(f"{self} BroadcastMessageSync => received: {m.get_payload()} {self.lamport_clock}")
            return m

    @subscribe(threadMode=Mode.PARALLEL, onEvent=BroadcastMessageSync)
    def on_broadcast_sync(self, m):
        if not isinstance(m, BroadcastMessageSync):
            print(f"{self} BroadcastMessageSync => {self.process.getName()} Invalid object type is passed.")
            return
        if m.author == self.process.getName():
            return
        self.lamport_clock.lock_clock()
        self.lamport_clock.update(m)
        self.letterbox.append(m)
        print(f"{self} BroadcastMessageSync from {m.author} => {self.lamport_clock}")
        self.lamport_clock.unlock_clock()
        self.broadcast_sync_ack(m.author)

    def broadcast_sync_ack(self, recipient):
        self.lamport_clock.lock_clock()
        self.lamport_clock.increment()
        m = BroadcastSyncAck(lamport_clock=self.lamport_clock, author=self.process.getName(), recipient=recipient)
        print(f"{self} BroadcastSyncAck => respond to {recipient} {self.lamport_clock}")
        self.lamport_clock.unlock_clock()
        m.post()

    @subscribe(threadMode=Mode.PARALLEL, onEvent=BroadcastSyncAck)
    def on_broadcast_sync_ack(self, m):
        if not isinstance(m, BroadcastSyncAck):
            print(f"{self} BroadcastSyncAck => {self.process.getName()} Invalid object type is passed.")
            return
        if m.recipient != self.process.getName():
            return
        self.lamport_clock.lock_clock()
        self.lamport_clock.update(m)
        print(f"{self} BroadcastSyncAck from {m.author} => {self.lamport_clock}")
        self.lamport_clock.unlock_clock()
        self.answered_process_broadcast_sync.add(m.author)
