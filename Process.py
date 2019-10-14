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

    @subscribe(onEvent=Event)
    def process(self, event):
        lamport_clock_value = event.getEstampile()
        self.lamport_clock.update(lamport_clock_value)
        print(self.getName() + ' data : ' + event.getData() + repr(self.lamport_clock))

    def run(self):
        t = Token(lamport_clock=LamportClock() , author="0", recipient="1", min_wait=1)
        if self.getName() == "0":
            self.sendToken(t)
        elif self.getName() == "2":
            sleep(1)
            self.critical_work()
        loop = 0
        while self.alive:
            print(self.getName() + " Loop: " + str(loop))
            sleep(1)
            self.sendTo("ga", "2")
            if self.getName() == "2":
                self.broadcast("bu")

            loop += 1
        print(self.getName() + " stopped")

    def stop(self):
        self.alive = False
        self.join()

    def broadcast(self, data):
        bm = BroadcastMessage(data=data, lamport_clock=self.lamport_clock, author=self.getName())
        self.lamport_clock.increment()
        print("Broadcast " + bm.getAuthor() + " => " + self.getName() + " send: " + bm.getData() + repr(self.lamport_clock))
        PyBus.Instance().post(bm)

    @subscribe(onEvent=BroadcastMessage)
    def onBroadcast(self, m):
        if not isinstance(m, BroadcastMessage):
            print("Broadcast => " + self.getName() + ' Invalid object type is passed.')
            return
        if m.getAuthor() == self.getName():
            return
        data = m.getData()
        lamport_clock_value = m.getEstampile()
        self.lamport_clock.update(lamport_clock_value)
        print("Broadcast " + m.getAuthor() + " => " + self.getName() + ' data : ' + data + repr(self.lamport_clock))

    def sendTo(self, data, id):
        dm = DedicatedMessage(data=data, lamport_clock=self.lamport_clock, author=self.getName(), recipient=id)
        self.lamport_clock.increment()
        print("DedicatedMessage " + dm.author + " => " + self.getName() + " send: " + dm.getData() + repr(self.lamport_clock))
        PyBus.Instance().post(dm)

    @subscribe(onEvent=DedicatedMessage)
    def onReceive(self, m):
        if not isinstance(m, DedicatedMessage):
            print("DedicatedMessage => " + self.getName() + ' Invalid object type is passed.')
            return
        if m.recipient != self.getName():
            return
        data = m.getData()
        lamport_clock_value = m.getEstampile()
        self.lamport_clock.update(lamport_clock_value)
        print("DedicatedMessage " + m.author + " => " + self.getName() + ' data : ' + data + repr(self.lamport_clock))

    def request(self):
        self.state = State.REQUEST
        print(self.getName() + " => " + str(self.state))
    
    def release(self):
        assert self.state == State.SC, "Error : unstable state !"
        self.state = State.RELEASE
        print(self.getName() + " => " + str(self.state))

    def critical_work(self):
        self.request()
        while self.state != State.SC:
            sleep(1)
        print(self.getName() + " critical section /!\\")
        sleep(1)
        self.release()

    def sendToken(self, t):
        t.recipient = str((int(t.recipient) + 1) % PROCESS_NUMBER)
        t.author = self.getName()
        self.lamport_clock.increment()
        print("Token " + t.author + " => " + self.getName() + " send: " + t.getData() + repr(self.lamport_clock))
        PyBus.Instance().post(t)
    
    @subscribe(onEvent=Token)
    def onToken(self, token):
        if not self.alive:
            return
        assert self.state != State.SC, "Error : unstable state ! " + self.getName()
        assert self.state != State.RELEASE, "Error : unstable state ! " + self.getName()
        print(self.getName() + " receive token")
        if self.state == None:
            sleep(token.min_wait)
        elif self.state == State.REQUEST:
            self.state = State.SC
            while self.state != State.RELEASE:
                sleep(token.min_wait)
        self.state = None
        self.sendToken(token)
        
        

        
