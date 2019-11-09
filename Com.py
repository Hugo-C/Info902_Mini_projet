from collections import deque
from enum import Enum
from time import sleep
from typing import Dict, Callable, List, Set, Any

from pyeventbus3.pyeventbus3 import Mode
from pyeventbus3.pyeventbus3 import PyBus
from pyeventbus3.pyeventbus3 import subscribe

from BaseProcess import BaseProcess
from HeartbitProcess import HeartbitProcess
from Message import BroadcastMessage, DedicatedMessage, Token, Synchronize, SynchronizeAck, BroadcastMessageSync, \
    BroadcastSyncAck, DedicatedMessageSync, DedicatedMessageSyncAck, Message, JoinMessage, Heartbit

State = Enum("State", "REQUEST SC RELEASE")

ACTIVE_WAIT_TIME = 0.2
RESPONSE_WAIT_TIME = 0.5


class Com:
    """Communication class, that allow a process to communicate with others"""

    def __init__(self, process):
        self._callbacks: Dict[str, List[Callable[[Message], None]]] = {}
        self.process: BaseProcess = process
        self.broadcast_sync_message = None
        self.letterbox = deque()
        self.state = None
        self.answered_process = set()
        self.answered_process_broadcast_sync = set()
        self.answered_process_heartbit = set()
        self.have_process_sync_responded = False
        self.process_number = 1

        self.heartbit_process = HeartbitProcess(self.process, self)

        PyBus.Instance().register(self, self)

    def __repr__(self):
        return f"[⚙ {self.process.name}]"

    def _get_nodes_down(self, nodes_up: Set[str]) -> Set[str]:
        """Return a set of nodes down from a set of nodes up"""
        list_nodes_down = set()
        for i in range(0, self.process_number):
            if str(i) != self.process.name and str(i) not in nodes_up:
                list_nodes_down.add(str(i))
        return list_nodes_down

    def register_function(self, f: Callable[[Message], None], *, tag: str):
        """If com received a message with the given tag, trigger the function f"""
        functions = self._callbacks.get(tag, [])
        functions.append(f)
        self._callbacks[tag] = functions

    def _trigger_function(self, m: Message):
        """Trigger all function registered for this message (based on tag)"""
        if m.tag is None or m.tag not in self._callbacks:
            return
        functions = self._callbacks[m.tag]
        for f in functions:
            f(m)

    def broadcast(self, data: Any, tag=None):
        """Broadcast the data to all other process"""
        self.process.lamport_clock.lock_clock()
        self.process.lamport_clock.increment()
        bm = BroadcastMessage(data=data, lamport_clock=self.process.lamport_clock, author=self.process.name, tag=tag)
        print(f"{self} Broadcast => send: {data} {self.process.lamport_clock}")
        self.process.lamport_clock.unlock_clock()
        bm.post()

    @subscribe(onEvent=BroadcastMessage)
    def _on_broadcast(self, m: BroadcastMessage):
        """Called when receiving a BroadcastMessage from another process"""
        if not isinstance(m, BroadcastMessage):
            print(f"{self} ONBroadcast => Invalid object type is passed.")
            return
        if m.author == self.process.name:
            return
        data = m.payload
        self.process.lamport_clock.lock_clock()
        self.process.lamport_clock.update(m)
        print(f"{self} ONBroadcast from {m.author} => received : {data} + {self.process.lamport_clock}")
        self.process.lamport_clock.unlock_clock()
        self.letterbox.append(m)
        self._trigger_function(m)

    def send_to(self, data: Any, id: str, tag=None):
        """Send the data to the specified process"""
        self.process.lamport_clock.lock_clock()
        self.process.lamport_clock.increment()
        dm = DedicatedMessage(data=data, lamport_clock=self.process.lamport_clock, author=self.process.name, recipient=id, tag=tag)
        print(f"{self} DedicatedMessage => send: {data} to {id} {self.process.lamport_clock}")
        self.process.lamport_clock.unlock_clock()
        dm.post()

    @subscribe(onEvent=DedicatedMessage)
    def _on_receive(self, m: DedicatedMessage):
        """Called when receiving a DedicatedMessage from another process"""
        if not isinstance(m, DedicatedMessage):
            print(f"{self} ONDedicatedMessage => Invalid object type is passed.")
            return
        if m.recipient != self.process.name:
            # print(f"{self} ONDedicatedMessage => This message is not for me.")
            return
        data = m.payload
        self.process.lamport_clock.lock_clock()
        self.process.lamport_clock.update(m)
        print(f"{self} ONDedicatedMessage from {m.author} => received: {data} {self.process.lamport_clock}")
        self.process.lamport_clock.unlock_clock()
        self.letterbox.append(m)
        self._trigger_function(m)

    def send_token(self, t: Token):
        """Send a token to the next process in the ring"""
        process_position = int(self.process.name)
        t.recipient = str((process_position + 1) % self.process_number)
        t.author = self.process.name
        print(f"{self} Token => send token to {t.recipient} {self.process.lamport_clock}")
        t.post()

    @subscribe(threadMode=Mode.PARALLEL, onEvent=Token)
    def _on_token(self, token: Token):
        """
        Called when receiving a Token from another process.
        The token can be used to perform a work on shared resource, but need to be send to the next process at the end.
        """
        if not self.process.alive:
            return
        if token.recipient != self.process.name:
            return

        assert self.state != State.SC, "Error : unstable state ! " + self.process.name
        assert self.state != State.RELEASE, "Error : unstable state ! " + self.process.name
        print(f"{self} ONToken => received token from {token.author} {self.process.lamport_clock}")
        if self.state is None:
            sleep(token.min_wait)
        elif self.state == State.REQUEST:
            self.state = State.SC
            while self.state != State.RELEASE:
                sleep(token.min_wait)
        self.state = None
        self.send_token(token)

    def request_sc(self):
        """
        Request to perform a work on shared resource.
        If the process is not alive after this function, the critical work SHOULD NOT be performed.
        """
        self.state = State.REQUEST
        print(f"{self} REQUEST => state : {self.state}")
        while self.state != State.SC and self.process.alive:
            sleep(ACTIVE_WAIT_TIME)
        print(f"{self} REQUEST => state : {self.state}")

    def release_sc(self):
        """Have to be called once the work on shared resource is done"""
        assert self.state == State.SC, "Error : unstable state !"
        self.state = State.RELEASE
        print(f"{self} RELEASE => state : {self.state}")

    def synchronize(self):
        """Synchronize all process"""
        self.answered_process = set()
        self.process.lamport_clock.lock_clock()
        self.process.lamport_clock.increment()
        m = Synchronize(lamport_clock=self.process.lamport_clock, author=self.process.name)
        print(f"{self} Synchronize => {self.process.lamport_clock}")
        self.process.lamport_clock.unlock_clock()
        m.post()
        while len(self.answered_process) < self.process_number - 1 and self.process.alive:
            sleep(ACTIVE_WAIT_TIME)

    @subscribe(onEvent=Synchronize)
    def _on_synchronize(self, m: Synchronize):
        """Called when receiving a synchronize function"""
        if not isinstance(m, Synchronize):
            print(f"{self} Synchronize => {self.process.name} Invalid object type is passed.")
            return
        if m.author == self.process.name:
            return
        self.process.lamport_clock.lock_clock()
        self.process.lamport_clock.update(m)
        print(f"{self} Synchronize from {m.author} => {self.process.lamport_clock}")
        self.process.lamport_clock.unlock_clock()
        self._synchronize_ack(m.author)

    def _synchronize_ack(self, recipient: str):
        """Send a synchronize acknowledgement"""
        self.process.lamport_clock.lock_clock()
        self.process.lamport_clock.increment()
        m = SynchronizeAck(lamport_clock=self.process.lamport_clock, author=self.process.name, recipient=recipient)
        print(f"{self} SynchronizeAck => respond to {recipient} {self.process.lamport_clock}")
        self.process.lamport_clock.unlock_clock()
        m.post()

    @subscribe(threadMode=Mode.PARALLEL, onEvent=SynchronizeAck)
    def _on_synchronize_ack(self, m: SynchronizeAck):
        """Called when receiving a synchronize acknowledgement"""
        if not isinstance(m, SynchronizeAck):
            print(f"{self} SynchronizeAck => {self.process.name} Invalid object type is passed.")
            return
        if m.recipient != self.process.name:
            return
        self.process.lamport_clock.lock_clock()
        self.process.lamport_clock.update(m)
        print(f"{self} SynchronizeAck from {m.author} => {self.process.lamport_clock}")
        self.process.lamport_clock.unlock_clock()
        self.answered_process.add(m.author)

    def broadcast_sync(self, data: Any, from_id: str, tag=None):
        """Send a blocking broadcast if from_id is process.name else receive a blocking broadcast"""
        if from_id == self.process.name:
            self.answered_process_broadcast_sync = set()
            self.process.lamport_clock.lock_clock()
            self.process.lamport_clock.increment()
            bm = BroadcastMessageSync(data=data, lamport_clock=self.process.lamport_clock, author=self.process.name, tag=tag)
            print(f"{self} Broadcast => send: {data} {self.process.lamport_clock}")
            self.process.lamport_clock.unlock_clock()
            bm.post()

            while len(self.answered_process_broadcast_sync) != self.process_number - 1 and self.process.is_alive():
                sleep(ACTIVE_WAIT_TIME)
        else:  # wait for a broadcast message from from_id
            while self.broadcast_sync_message is not None and self.process.alive:
                sleep(ACTIVE_WAIT_TIME)
            m: BroadcastMessageSync = self.broadcast_sync_message
            self.broadcast_sync_message = None
            print(f"{self} BroadcastMessageSync => received: {m.payload} {self.process.lamport_clock}")
            return m

    @subscribe(threadMode=Mode.PARALLEL, onEvent=BroadcastMessageSync)
    def _on_broadcast_sync(self, m: BroadcastMessageSync):
        """Receive a broadcast asynchronously"""
        if not isinstance(m, BroadcastMessageSync):
            print(f"{self} BroadcastMessageSync => {self.process.name} Invalid object type is passed.")
            return
        if m.author == self.process.name:
            return
        self.process.lamport_clock.lock_clock()
        self.process.lamport_clock.update(m)
        self.broadcast_sync_message = m
        print(f"{self} BroadcastMessageSync from {m.author} => {self.process.lamport_clock}")
        self.process.lamport_clock.unlock_clock()
        self._trigger_function(m)
        self._broadcast_sync_ack(m.author)

    def _broadcast_sync_ack(self, recipient: str):
        """Send a broadcast acknowledgement"""
        self.process.lamport_clock.lock_clock()
        self.process.lamport_clock.increment()
        m = BroadcastSyncAck(lamport_clock=self.process.lamport_clock, author=self.process.name, recipient=recipient)
        print(f"{self} BroadcastSyncAck => respond to {recipient} {self.process.lamport_clock}")
        self.process.lamport_clock.unlock_clock()
        m.post()

    @subscribe(threadMode=Mode.PARALLEL, onEvent=BroadcastSyncAck)
    def _on_broadcast_sync_ack(self, m: BroadcastSyncAck):
        """Called when receiving a blocking broadcast acknowledgement"""
        if not isinstance(m, BroadcastSyncAck):
            print(f"{self} BroadcastSyncAck => {self.process.name} Invalid object type is passed.")
            return
        if m.recipient != self.process.name:
            return
        self.process.lamport_clock.lock_clock()
        self.process.lamport_clock.update(m)
        print(f"{self} BroadcastSyncAck from {m.author} => {self.process.lamport_clock}")
        self.process.lamport_clock.unlock_clock()
        self.answered_process_broadcast_sync.add(m.author)

    def send_to_sync(self, data: Any, dest: str, tag=None):
        """Send the data to the specified process and wait until it respond"""
        self.have_process_sync_responded = False
        self.process.lamport_clock.lock_clock()
        self.process.lamport_clock.increment()
        dm = DedicatedMessageSync(
            data=data,
            lamport_clock=self.process.lamport_clock,
            author=self.process.name,
            recipient=dest,
            tag=tag,
        )
        print(f"{self} DedicatedMessageSync => send: {data} to {dest} {self.process.lamport_clock}")
        self.process.lamport_clock.unlock_clock()
        dm.post()
        while not self.have_process_sync_responded:
            sleep(ACTIVE_WAIT_TIME)

    @subscribe(onEvent=DedicatedMessageSync)
    def _receive_from_sync(self, m: DedicatedMessageSync):
        """Called when receiving a DedicatedMessageSync from another process"""
        if not isinstance(m, DedicatedMessageSync):
            print(f"{self} ONDedicatedMessageSync => Invalid object type is passed.")
            return
        if m.recipient != self.process.name:
            return
        data = m.payload
        self.process.lamport_clock.lock_clock()
        self.process.lamport_clock.update(m)
        print(f"{self} ONDedicatedMessageSync from {m.author} => received: {data} {self.process.lamport_clock}")
        self.process.lamport_clock.unlock_clock()
        self._send_to_sync_ack(m.author)
        self._trigger_function(m)

    def _send_to_sync_ack(self, recipient: str):
        """Send a dedicated message acknowledgement"""
        self.process.lamport_clock.lock_clock()
        self.process.lamport_clock.increment()
        m = DedicatedMessageSyncAck(lamport_clock=self.process.lamport_clock, author=self.process.name, recipient=recipient)
        print(f"{self} DedicatedMessageSyncAck => respond to {recipient} {self.process.lamport_clock}")
        self.process.lamport_clock.unlock_clock()
        m.post()

    @subscribe(threadMode=Mode.PARALLEL, onEvent=DedicatedMessageSyncAck)
    def _receive_from_sync_ack(self, m: DedicatedMessageSyncAck):
        """Called when receiving a DedicatedMessageSync acknowledgement from another process"""
        if not isinstance(m, DedicatedMessageSyncAck):
            print(f"{self} DedicatedMessageSyncAck => {self.process.name} Invalid object type is passed.")
            return
        if m.recipient != self.process.name:
            return
        self.process.lamport_clock.lock_clock()
        self.process.lamport_clock.update(m)
        print(f"{self} DedicatedMessageSyncAck from {m.author} => {self.process.lamport_clock}")
        self.process.lamport_clock.unlock_clock()
        self.have_process_sync_responded = True

    def join_sync(self):
        """Set up the communication to have an id and be able to communicate with other """
        self.answered_process_broadcast_sync = set()
        self.process.lamport_clock.lock_clock()
        self.process.lamport_clock.increment()
        bm = JoinMessage(data="", lamport_clock=self.process.lamport_clock, author=self.process.name)
        print(f"{self} JoinMessage => send: {self.process.lamport_clock}")
        self.process.lamport_clock.unlock_clock()
        bm.post()
        sleep(RESPONSE_WAIT_TIME)
        self.process.name = str(len(self.answered_process_broadcast_sync))
        self.process_number = len(self.answered_process_broadcast_sync) + 1
        print(f"{self.process} init")

    @subscribe(threadMode=Mode.PARALLEL, onEvent=JoinMessage)
    def _on_join_sync(self, m: JoinMessage):
        """Called when receiving a JoinMessage from another process"""
        if not isinstance(m, JoinMessage):
            print(f"{self} JoinMessage => {self.process.name} Invalid object type is passed.")
            return
        if m.author == self.process.name:
            return
        self.process.lamport_clock.lock_clock()
        self.process.lamport_clock.update(m)
        print(f"{self} JoinMessage from {m.author} => {self.process.lamport_clock}")
        self.process.lamport_clock.unlock_clock()
        self._broadcast_sync_ack(m.author)
        self.process_number += 1

    def _send_heartbit(self):
        """Send a heartbit to all process to see if they are alive"""
        self.answered_process_broadcast_sync = set()
        m = Heartbit(data="", lamport_clock=self.process.lamport_clock, author=self.process.name)
        print(f"{self} Heartbit => send: {self.process.lamport_clock}")
        m.post()
        sleep(RESPONSE_WAIT_TIME)
        if len(self.answered_process_broadcast_sync) < self.process_number - 1:
            nodes_down = self._get_nodes_down(self.answered_process_broadcast_sync)
            nodes_down_below_myself = len([node for node in nodes_down if int(node) < int(self.process.name)])
            self.process.name = str(int(self.process.name) - int(nodes_down_below_myself))

            print(f"{self.process} /!\\ one node is down, only {len(self.answered_process_broadcast_sync)} nodes are up, "
                  f"(nodes down: {nodes_down}, nodes_up: {self.answered_process_broadcast_sync}, expected: {self.process_number})")
            self.process_number -= 1

    @subscribe(threadMode=Mode.PARALLEL, onEvent=Heartbit)
    def _on_heartbit(self, m: Heartbit):
        """Called when receiving a Heartbit from another process"""
        if not isinstance(m, Heartbit):
            print(f"{self} Heartbit => {self.process.name} Invalid object type is passed.")
            return
        if m.author == self.process.name:
            return
        if self.process.is_alive():
            print(f"{self} Heartbit from {m.author} => {self.process.lamport_clock}")
            self._broadcast_sync_ack(m.author)
