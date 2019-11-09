from threading import Thread

from LamportClock import LamportClock


class BaseProcess(Thread):
    """Super class of all process that implement a lamportClock"""

    def __init__(self):
        Thread.__init__(self)
        self.alive = True
        self.lamport_clock = LamportClock()
