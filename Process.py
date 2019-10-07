from threading import Lock, Thread

from time import sleep

# from geeteventbus.subscriber import subscriber
# from geeteventbus.eventbus import eventbus
# from geeteventbus.event import event
from LamportClock import LamportClock
from EventBus import EventBus
from Event import Event
from Event import BroadcastMessage


class Process(Thread):
    def __init__(self, name):
        Thread.__init__(self)

        self.setName(name)
        self.lamport_clock = LamportClock()

        self.bus = EventBus.getInstance()
        self.bus.register(self, 'Bidule')
        if self.getName() == "P3":
            self.bus.register(self, 'Machin')

        self.alive = True
        self.start()

    def process(self, event):
        if not isinstance(event, Event):
            print(self.getName() + ' Invalid object type is passed.')
            return
        topic = event.getTopic()
        data = event.getData()
        lamport_clock_value = event.getEstampile()
        self.lamport_clock.update(lamport_clock_value)
        print(self.getName() + ' Processes event from TOPIC: ' + topic + ' with DATA: ' + data)
        print("[MY_LOG] self.lamport_clock : " + repr(self.lamport_clock))

    def run(self):
        loop = 0
        while self.alive:
            print(self.getName() + " Loop: " + str(loop))
            sleep(1)

            self.lamport_clock.increment()
            b1 = Event(data="ga", lamport_clock=self.lamport_clock)
            print(self.getName() + " send: " + b1.getData())
            print("[MY_LOG] self.lamport_clock : " + repr(self.lamport_clock))
            self.bus.post(b1)
            if self.getName() == "P2":
                self.lamport_clock.increment()
                b2 = Event(data="bu", lamport_clock=self.lamport_clock)
                self.bus.post(b2)
                print(self.getName() + " send: " + b2.getData())
                print("[MY_LOG] self.lamport_clock : " + repr(self.lamport_clock))

            loop += 1
        print(self.getName() + " stopped")

    def stop(self):
        self.alive = False
        self.join()

    def broadcast(self, data):
        bm = BroadcastMessage(data=data, lamport_clock=self.lamport_clock, author=self.getName())
        self.lamport_clock.increment()
        print(self.getName() + " send: " + bm.getData())
        print("[MY_LOG] self.lamport_clock : " + repr(self.lamport_clock))
        self.bus.post(bm)

    def onBroadcast(self, m):
        if not isinstance(m, BroadcastMessage):
            print(self.getName() + ' Invalid object type is passed.')
            return
        if m.getAuthor() == self.getName():
            return
        topic = m.getTopic()
        data = m.getData()
        lamport_clock_value = m.getEstampile()
        self.lamport_clock.update(lamport_clock_value)
        print(self.getName() + ' Processes event from TOPIC: ' + topic + ' with DATA: ' + data)
        print("[MY_LOG] self.lamport_clock : " + repr(self.lamport_clock))