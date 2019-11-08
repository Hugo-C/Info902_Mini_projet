from time import sleep

from Process import Process

if __name__ == '__main__':
    
    p1 = Process("78")
    sleep(1)
    p2 = Process("24")
    sleep(1)
    p3 = Process("8")

    sleep(20)

    p1.stop()
    p2.stop()
    p3.stop()
