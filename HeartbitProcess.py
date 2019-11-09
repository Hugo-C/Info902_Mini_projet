from threading import Thread
from time import sleep

HEARTBIT_LAPSE_TIME = 1


class HeartbitProcess(Thread):

    def __init__(self, process, com):
        Thread.__init__(self)
        self.process = process
        self.com = com
        self.start()

    def run(self):
        while self.process.is_alive():
            self.com._send_heartbit()
            sleep(HEARTBIT_LAPSE_TIME)
