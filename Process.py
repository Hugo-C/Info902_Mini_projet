from threading import Lock, Thread

from time import sleep
from enum import Enum

from Event import Event, Synchronize, SynchronizeAck
from Event import BroadcastMessage
from Event import DedicatedMessage
from Event import Token
from LamportClock import LamportClock

from pyeventbus3.pyeventbus3 import *

State = Enum("State", "REQUEST SC RELEASE")
PROCESS_NUMBER = 3


class Process(Thread):
    def __init__(self, name):
        Thread.__init__(self)

        self.setName(name)
        self.lamport_clock = LamportClock()
        self.state = None

        PyBus.Instance().register(self, self)

        self.alive = True
        self.start()
        self.answered_process = set()

    def __repr__(self):
        return f"[⚙ {self.getName()}]"

    @subscribe(onEvent=Event)
    def process(self, event):
        self.lamport_clock.update(event)
        print(f" data : {event.getData()}  {self.lamport_clock}")

    def run(self):
        if self.getName() == "2":
            sleep(1)
            self.critical_work()
        if self.getName() == "0":
            sleep(1)
            self.synchronize()
        loop = 0
        while self.alive:
            sleep(1)
            # self.sendTo("ga", "2")
            # if self.getName() == "0":
            #     self.broadcast("bu")

            loop += 1
        print(f"{self} stopped")

    def stop(self):
        self.alive = False
        self.join()

    def broadcast(self, data):
        self.lamport_clock.increment()
        bm = BroadcastMessage(data=data, lamport_clock=self.lamport_clock, author=self.getName())
        print(f"{self} Broadcast => send: {data} {self.lamport_clock}")
        bm.post()

    @subscribe(onEvent=BroadcastMessage)
    def onBroadcast(self, m):
        if not isinstance(m, BroadcastMessage):
            print(f"{self} ONBroadcast => {self.getName()} Invalid object type is passed.")
            return
        if m.getAuthor() == self.getName():
            return
        data = m.getData()
        self.lamport_clock.update(m)
        print(f"{self} ONBroadcast from {m.getAuthor()} => received : {data} + {self.lamport_clock}")

    def sendTo(self, data, id):
        self.lamport_clock.increment()
        dm = DedicatedMessage(data=data, lamport_clock=self.lamport_clock, author=self.getName(), recipient=id)
        print(f"{self} DedicatedMessage => send: {data} to {id} {self.lamport_clock}")
        dm.post()

    @subscribe(onEvent=DedicatedMessage)
    def onReceive(self, m):
        if not isinstance(m, DedicatedMessage):
            print(f"{self} ONDedicatedMessage => Invalid object type is passed.")
            return
        if m.recipient != self.getName():
            return
        data = m.getData()
        self.lamport_clock.update(m)
        print("DedicatedMessage " + m.author + " => " + self.getName() + ' data : ' + data + " " + repr(self.lamport_clock))
        print(f"{self} ONDedicatedMessage from {m.author} => received: {data} {self.lamport_clock}")

    def request(self):
        self.state = State.REQUEST
        print(f"{self} REQUEST => state : {self.state}")
    
    def release(self):
        assert self.state == State.SC, "Error : unstable state !"
        self.state = State.RELEASE
        print(f"{self} RELEASE => state : {self.state}")

    def critical_work(self):
        self.request()
        while self.state != State.SC:
            sleep(1)
        print(f"{self} SC => state : {self.state}")
        sleep(1)
        self.release()

    def sendToken(self, t):
        process_position = int(self.getName())
        t.recipient = str((process_position + 1) % PROCESS_NUMBER)
        t.author = self.getName()
        self.lamport_clock.increment()
        t.update_lamport_clock(self.lamport_clock)
        print(f"{self} Token => send token to {t.recipient} {self.lamport_clock}")
        t.post()

    @subscribe(onEvent=Token)
    def onToken(self, token):
        if not self.alive:
            return
        if token.recipient != self.getName():
            return

        assert self.state != State.SC, "Error : unstable state ! " + self.getName()
        assert self.state != State.RELEASE, "Error : unstable state ! " + self.getName()
        self.lamport_clock.update(token)
        print(f"{self} ONToken => received token from {token.author} {self.lamport_clock}")
        if self.state == None:
            sleep(token.min_wait)
        elif self.state == State.REQUEST:
            self.state = State.SC
            while self.state != State.RELEASE:
                sleep(token.min_wait)
        self.state = None
        self.sendToken(token)

    def synchronize(self):
        self.answered_process = set()
        self.lamport_clock.increment()
        m = Synchronize(lamport_clock=self.lamport_clock, author=self.getName())
        print(f"{self} Synchronize => {self.lamport_clock}")
        m.post()
        while len(self.answered_process) < PROCESS_NUMBER - 1:
            sleep(1)

    @subscribe(onEvent=Synchronize)
    def onSynchronize(self, m):
        if not isinstance(m, Synchronize):
            print(f"{self} Synchronize => {self.getName()} Invalid object type is passed.")
            return
        if m.getAuthor() == self.getName():
            return
        self.lamport_clock.update(m)
        print(f"{self} Synchronize from {m.getAuthor()} => {self.lamport_clock}")
        self.synchronizeAck(m.getAuthor())

    def synchronizeAck(self, recipient):
        self.lamport_clock.increment()
        m = SynchronizeAck(lamport_clock=self.lamport_clock, author=self.getName(), recipient=recipient)
        print(f"{self} SynchronizeAck => respond to {recipient} {self.lamport_clock}")
        m.post()

    @subscribe(onEvent=SynchronizeAck)
    def onSynchronizeAck(self, m):
        if not isinstance(m, SynchronizeAck):
            print(f"{self} SynchronizeAck => {self.getName()} Invalid object type is passed.")
            return
        if m.recipient != self.getName():
            return
        self.lamport_clock.update(m)
        print(f"{self} SynchronizeAck from {m.author} => {self.lamport_clock}")
        self.answered_process.add(m.author)
