from threading import Thread

from LamportClock import LamportClock


class BaseProcess(Thread):

    def __init__(self):
        Thread.__init__(self)
        self.alive = True
        self.lamport_clock = LamportClock()
