from threading import Lock, Thread

from time import sleep

from Event import Event
from Event import BroadcastMessage
from LamportClock import LamportClock

from pyeventbus3.pyeventbus3 import *


class Process(Thread):
    def __init__(self, name):
        Thread.__init__(self)

        self.setName(name)
        self.lamport_clock = LamportClock()

        PyBus.Instance().register(self, self.__class__.__name__)

        self.alive = True
        self.start()

    @subscribe(onEvent=Event)
    def process(self, event):
        lamport_clock_value = event.getEstampile()
        self.lamport_clock.update(lamport_clock_value)
        print(self.getName() + ' Processes event : ' + event.getData())
        print("[MY_LOG] self.lamport_clock : " + repr(self.lamport_clock))

    def run(self):
        loop = 0
        while self.alive:
            print(self.getName() + " Loop: " + str(loop))
            sleep(1)

            self.lamport_clock.increment()
            b1 = BroadcastMessage("ga", lamport_clock=self.lamport_clock, author=self.getName())
            print(self.getName() + " send: " + b1.getData())
            PyBus.Instance().post(b1)
            if self.getName() == "P2":
                self.lamport_clock.increment()
                b2 = Event("bu", lamport_clock=self.lamport_clock)
                print(self.getName() + " send: " + b2.getData())
                PyBus.Instance().post(b2)

            loop += 1
        print(self.getName() + " stopped")

    def stop(self):
        self.alive = False
        self.join()

    def broadcast(self, data):
        bm = BroadcastMessage(data=data, lamport_clock=self.lamport_clock, author=self.getName())
        self.lamport_clock.increment()
        print("Broadcast " + bm.getAuthor() + " => " + self.getName() + " send: " + bm.getData())
        print("Broadcast => " + "[MY_LOG] self.lamport_clock : " + repr(self.lamport_clock))
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
        print("Broadcast " + m.getAuthor() + " => " + self.getName() + ' Processes event : ' + data)
        print("Broadcast => " + "[MY_LOG] self.lamport_clock : " + repr(self.lamport_clock))
