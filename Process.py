from threading import Lock, Thread

from time import sleep
from enum import Enum

from Event import Event
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

        PyBus.Instance().register(self, self.__class__.__name__)

        self.alive = True
        self.start()

    def __repr__(self):
        return f"[⚙ {self.getName()}]"

    @subscribe(onEvent=Event)
    def process(self, event):
        self.lamport_clock.update(event)
        print(f" data : {event.getData()}  {self.lamport_clock}")

    def run(self):
        # elif self.getName() == "2":
        #     sleep(1)
        #     self.critical_work()
        loop = 0
        while self.alive:
            sleep(1)
            self.sendTo("ga", "2")
            if self.getName() == "2":
                self.broadcast("bu")

            loop += 1
        print(f"{self} stopped")

    def stop(self):
        self.alive = False
        self.join()

    def broadcast(self, data):
        bm = BroadcastMessage(data=data, lamport_clock=self.lamport_clock, author=self.getName())
        self.lamport_clock.increment()
        print(f"{self} Broadcast => send: {data} {self.lamport_clock}")
        PyBus.Instance().post(bm)

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
        dm = DedicatedMessage(data=data, lamport_clock=self.lamport_clock, author=self.getName(), recipient=id)
        self.lamport_clock.increment()
        print(f"{self} DedicatedMessage => send: {data} to {id} {self.lamport_clock}")
        PyBus.Instance().post(dm)

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
        PyBus.Instance().post(t)
    
    @subscribe(onEvent=Token)
    def onToken(self, token):
        if not self.alive and token.recipient == self.getName():
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
