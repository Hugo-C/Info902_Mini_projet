import random
from enum import Enum
from time import sleep

from pyeventbus3.pyeventbus3 import *

from Com import Com
from Message import *
from LamportClock import LamportClock

PROCESS_NUMBER = 3
DICE_FACE = 6
RESULT_FILENAME = "results.txt"

def who_is_winner(dict_of_result):
    """ return the winner of the last dice roll """
    winner = "error"
    max_res = -1
    for key, value in dict_of_result.items():
        if value >= max_res:
            max_res = value
            winner = key
    return winner, max_res


class Process(Thread):
    def __init__(self, name):
        Thread.__init__(self)

        self.setName(name)
       
        self.alive = True
        self.start()
        self.answered_process = set()
        self.dice_result = {}

        self.com = Com(self)

    def __repr__(self):
        return f"[⚙ {self.getName()}]"

    @subscribe(threadMode=Mode.PARALLEL, onEvent=Message)
    def process(self, event):
        self.lamport_clock.update(event)
        print(f" data : {event.get_payload()}  {self.lamport_clock}")

    def run(self):
        """ method run for the roll dice """
        sleep(1)
        # if self.getName() == "1":
        #     t = Token(lamport_clock=LamportClock(), author="", recipient="", min_wait=1)
        #     self.com.send_token(t)
        # sleep(int(self.getName()))
        # self.com.synchronize()
        # print("Synchronize !!!")

        if self.getName() == "0":
            self.com.send_to_sync("wow", "0")

        loop = 0
        # self.critical_work()
        while self.alive:

            # # roll dice
            # dice_value = self.roll_dice()
            # self.dice_result = {self.getName(): dice_value}

            # # wait that all players have play
            # while len(self.dice_result) < PROCESS_NUMBER and self.alive:
            #     sleep(0.5)

            # if self.alive:
            #     # look at who is the winner and write his result in a file
            #     process, res = who_is_winner(self.dice_result)
            #     if self.getName() == process:
            #         self.write_result(process, res)
            #     self.synchronize()
            loop += 1
        print(f"{self} stopped")

    def write_result(self, process, result):
        """ write in a file the winner's result of the last roll dice """
        print(f"{self} writing result {self.lamport_clock}")
        self.request()
        while self.state != State.SC and self.alive:
            sleep(1)
        if self.alive:
            self.lamport_clock.increment()
            with open(RESULT_FILENAME, "a+") as f:
                f.write(f"{process} : {result}\n")
            self.release()
            print(f"{self} result writed {self.lamport_clock}")

    def stop(self):
        print(f"{self} RECEIVED stop message {self.com.lamport_clock}")
        self.alive = False
        self.join()

    def critical_work(self):
        self.com.request_sc()
        print(f"{self} SC => state : {self.com.state}")
        sleep(1)
        self.com.release_sc()

    def roll_dice(self):
        return random.randint(1, DICE_FACE)