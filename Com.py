from collections import deque

class Com:
     def __init__(self, process):
         self.process = process
         self.letterbox = deque()
          self.lamport_clock = LamportClock()

    def __repr__(self):
        return f"[⚙ {self.process.getName()}]"

     def broadcast(self, data):
        self.lamport_clock.lock_clock()
        self.lamport_clock.increment()
        bm = BroadcastMessage(data=data, lamport_clock=self.lamport_clock, author=self.process.getName())
        print(f"{self} Broadcast => send: {data} {self.lamport_clock}")
        self.lamport_clock.unlock_clock()
        bm.post()

    @subscribe(onEvent=BroadcastMessage)
    def onBroadcast(self, m):
        if not isinstance(m, BroadcastMessage):
            print(f"{self} ONBroadcast => Invalid object type is passed.")
            return
        if m.author == self.process.getName():
            return
        data = m.getData()
        self.lamport_clock.lock_clock()
        self.lamport_clock.update(m)
        print(f"{self} ONBroadcast from {m.author} => received : {data} + {self.lamport_clock}")
        self.lamport_clock.unlock_clock()