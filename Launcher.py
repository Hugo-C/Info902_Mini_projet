from time import sleep
from Process import Process
from Event import Token
from LamportClock import LamportClock

if __name__ == '__main__':
    
    p1 = Process("0")
    p2 = Process("1")
    p3 = Process("2")

    t = Token(lamport_clock=LamportClock(), author="", recipient="", min_wait=1)
    sleep(1)
    p1.sendToken(t)

    sleep(5)

    p1.stop()
    p2.stop()
    p3.stop()
