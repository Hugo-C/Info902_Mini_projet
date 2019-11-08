import random
from time import sleep

from pyeventbus3.pyeventbus3 import *

from BaseProcess import BaseProcess
from Com import Com
from Message import *

PROCESS_NUMBER = 3
DICE_FACE = 6
RESULT_FILENAME = "results.txt"
ACTIVE_WAIT_TIME = 0.2


def who_is_winner(dict_of_result):
    """ return the winner of the last dice roll """
    winner = "error"
    max_res = -1
    for key, value in dict_of_result.items():
        if value >= max_res:
            max_res = value
            winner = key
    return winner, max_res


def roll_dice():
    return random.randint(1, DICE_FACE)


class Process(BaseProcess):

    def __init__(self, name):
        super().__init__()
        self.setName(name)

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

        sleep(0.1)
        self.com.join_sync()
        sleep(0.1)
        self.com.register_function(self.receive_dice_value, tag="dice_value")

        if self.getName() == "1":
            t = Token(lamport_clock=self.lamport_clock, author="", recipient="", min_wait=1)
            self.com.send_token(t)
        sleep(int(self.getName()))
        self.com.synchronize()
        print("Synchronize !!!")

        loop = 0
        while self.alive:

            # roll dice
            dice_value = roll_dice()
            self.dice_result = {self.getName(): dice_value}
            self.com.broadcast(f"dice_value:{dice_value}", tag="dice_value")

            # wait that all players have play
            while len(self.dice_result) < PROCESS_NUMBER and self.alive:
                sleep(0.5)

            if self.alive:
                # look at who is the winner and write his result in a file
                process, res = who_is_winner(self.dice_result)
                if self.getName() == process:
                    self.write_result(process, res)
                self.com.synchronize()
            loop += 1
        print(f"{self} stopped")

    def receive_dice_value(self, m):
        data = m.payload
        print(f"{self} Receive_dice_value from {m.author} => received : {data} + {self.lamport_clock}")
        if "dice_value" in data:
            self.dice_result[m.author] = int(data.split(":")[1])

    def write_result(self, process, result):
        """ write in a file the winner's result of the last roll dice """
        print(f"{self} writing result {self.lamport_clock}")
        self.com.request_sc()
        if self.alive:
            self.lamport_clock.increment()
            with open(RESULT_FILENAME, "a+") as f:
                f.write(f"{process} : {result}\n")
            self.com.release_sc()
            print(f"{self} result writed {self.lamport_clock}")

    def stop(self):
        print(f"{self} RECEIVED stop message {self.lamport_clock}")
        self.alive = False
        self.join()

    def critical_work(self):
        self.com.request_sc()
        print(f"{self} SC => state : {self.com.state}")
        sleep(1)
        self.com.release_sc()
