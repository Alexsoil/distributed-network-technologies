import hashlib
import time
from bisect import bisect
from collections import deque
from dataclasses import dataclass
from enum import Enum

from random import randint
from typing import Self, ClassVar


class Id(int):
    """Chord id -- implements consistent hashing.

    Pass a string to the constructor to hash it.

    (Basically just an integer modulo 2^(hash bits) with a convenient constructor.)
    """

    hash_func = hashlib.md5
    bits = hash_func().digest_size * 8
    mod = 2 ** (hash_func().digest_size * 8)

    def __new__(cls, value: str | int | None = None):
        if isinstance(value, str):
            digest = Id.hash_func(value.encode()).hexdigest()
            return int.__new__(cls, digest, 16)
        if isinstance(value, int):
            if value >= Id.mod:
                raise ValueError("Value should be less than modulus")
            return int.__new__(cls, value)

    def __repr__(self):
        return hex(self)

    def __add__(self, other):
        return Id((int(self) + int(other)) % Id.mod)

    @classmethod
    def random(cls) -> 'Id':
        x = randint(0, Id.mod - 1)
        return int.__new__(cls, x)

    def between(self, a: Self, b: Self) -> bool:
        if a >= b:
            return a < self or b > self
        return a < self < b

    def between_right_incl(self, a: Self, b: Self) -> bool:
        if a >= b:
            return a < self or b >= self
        return a < self <= b


class TransactionType(Enum):
    join = 0
    lookup = 1
    stabilize = 2
    fix_finger = 3
    read = 4
    write = 5


class MsgType(Enum):
    find_successor = 0
    get_predecessor = 1
    notify = 2
    response = 3


@dataclass
class Message:
    type: MsgType
    args: dict
    src: Id
    dst: Id
    transaction: int = 0
    next_transaction: ClassVar[int] = 1

    @staticmethod
    def get_next_transaction():
        Message.next_transaction += 1
        return Message.next_transaction - 1


@dataclass
class Scientist:
    name: str
    awards: list[str]
    alma_mater: list[str]


class Simulator:
    def __init__(self, node_count):
        self._nodes: dict[Id, Node] = {}
        self._queue = deque[Message]()

        nodes = [Node(f"node{i}", self) for i in range(node_count)]

        self._nodes[nodes[0].id] = nodes[0]

        for n in nodes[1:]:
            self._nodes[n.id] = n
            n.join(nodes[0].id)

        self.process()

    def stabilize(self, iters: int):
        for i in range(iters):
            for n in self._nodes.values():
                n.stabilize()
                self.process()

    def optimize(self, iters: int):
        for i in range(iters):
            for n in self._nodes.values():
                n.fix_finger()
                self.process()

    def test_ring(self) -> tuple[float, int, int]:
        tests = 1000
        successes = 0
        t0 = time.time()
        nodes = list(self._nodes.values())
        for i in range(1000):
            # Random node and id
            node = nodes[randint(0, len(self._nodes) - 1)]
            _id = Id.random()

            node.lookup(_id)
            res = self.process()[0][1]
            if res == self.global_lookup(_id):
                successes += 1

        elapsed = time.time() - t0
        return elapsed, successes, tests

    def is_fully_stable(self) -> bool:
        succs = {}
        for node in self._nodes.values():
            succs[node.name] = self.get_name(node.successor)

        chain = []
        node = "node0"
        while node not in chain:
            chain.append(node)
            node = succs[node]

        return node == chain[0] and len(chain) == len(self._nodes.keys())

    def send(self, msg: Message) -> int:
        if msg.transaction == 0:
            raise ValueError()
        self._queue.append(msg)
        return msg.transaction

    def process(self):
        results = []
        while len(self._queue):
            msg = self._queue.popleft()
            response = self._nodes[msg.dst].receive(msg)
            if response is not None:
                self.send(response)
                # print(response)
            if self._nodes[msg.dst].result is not None:
                results.append(self._nodes[msg.dst].result)
                self._nodes[msg.dst].result = None
        return results

    def get_name(self, node: Id):
        return self._nodes[node].name

    def global_lookup(self, _id: Id):
        """Do an id lookup with perfect knowledge of the ring.

        Can be used to check if a local lookup (from a node) returned the correct result."""
        ids = sorted([n.id for _, n in self._nodes.items()])
        succ_idx = bisect(ids, _id) % len(ids)
        return self._nodes[ids[succ_idx]].id

    def random_node(self):
        a = list(self._nodes.values())
        return a[randint(0, len(a) - 1)]


class Node:
    def __init__(self, name: str, transport):
        self.name = name
        self.id = Id(name)
        self._fingers: list[Id] = [self.id] * Id.bits
        self._next_finger = 0
        self._predecessor: Id | None = None

        # A class with a send(Message) function
        self.transport = transport

        self.pending: dict[int, TransactionType] = {}
        self.result = None

        self._store: dict[Id, list[Scientist]] = {}

    @property
    def successor(self) -> Id:
        return self._fingers[0]

    @successor.setter
    def successor(self, value: Id):
        self._fingers[0] = value

    def receive(self, msg: Message) -> Message | None:
        """Receive a message and send one out in return (not necessarily to the same node)."""
        fragment = self.handle(msg)
        if fragment is None:
            return None
        dst, msg_type, args = fragment

        return Message(
            dst=dst,
            type=msg_type,
            args=args,
            src=msg.src,
            transaction=msg.transaction
        )

    def send(self, msg: Message) -> int:
        msg.transaction = Message.get_next_transaction()
        self.transport.send(msg)
        return msg.transaction

    def handle(self, msg: Message) -> tuple[Id, MsgType, dict] | None:
        args = msg.args
        match msg.type:
            case MsgType.find_successor:
                # If we're the successor, send a response to the source of the message
                if self.is_successor_of(args["id"]):
                    if "op" in args:
                        if args["op"] == "read":
                            return msg.src, MsgType.response, {
                                "id": args["id"],
                                "successor": self.successor,
                                "scientists": self._read_local(args["id"], args["min_awards"])
                            }

                        if args["op"] == "write":
                            self._write_local(args["id"], args["scientists"])
                            return msg.src, MsgType.response, {"id": args["id"], "successor": self.successor}

                    return msg.src, MsgType.response, {"id": args["id"], "successor": self.successor}
                # Otherwise, just pass it on
                closest = self.closest_preceding_node(args["id"])
                return closest, msg.type, args

            case MsgType.get_predecessor:
                return msg.src, MsgType.response, {"predecessor": self._predecessor}

            case MsgType.notify:
                # Update our predecessor, potentially
                if self._predecessor is None or msg.src.between(self._predecessor, self.id):
                    self._predecessor = msg.src

            case MsgType.response:
                context = self.pending.pop(msg.transaction)
                match context:
                    case TransactionType.read:
                        self.result = args["id"], args["successor"], args["scientists"]
                    case TransactionType.lookup:
                        self.result = args["id"], args["successor"]

                    case TransactionType.stabilize:
                        node = args["predecessor"]  # Successor's predecessor
                        if node is not None and node.between(self.id, self.successor):
                            self.successor = node
                        return self.successor, MsgType.notify, {}

                    case TransactionType.join:
                        self.successor = args["successor"]

                    case TransactionType.fix_finger:
                        self._fingers[self._next_finger] = args["successor"]
                        self._next_finger += 1
                        self._next_finger %= Id.bits

    # ========

    def is_successor_of(self, _id: Id) -> bool:
        return _id.between_right_incl(self.id, self.successor)

    def closest_preceding_node(self, _id: Id) -> Id:
        for i in range(Id.bits - 1, -1, -1):
            if self._fingers[i].between(self.id, _id):
                return self._fingers[i]
        return self.id

    def _read_local(self, _id: Id, min_awards: int) -> list[Scientist]:
        if not self.is_successor_of(_id):
            raise ValueError()
        scientists: list[Scientist] = self._store.get(_id, [])
        return [s for s in scientists if len(s.awards) >= min_awards]

    def _write_local(self, _id: Id, scientists: list[Scientist]):
        self._store[_id] = scientists

    # =========

    def join(self, node_id: Id):
        transaction = self.send(Message(
            type=MsgType.find_successor,
            src=self.id, dst=node_id,
            args={"id": self.id}
        ))
        self.pending[transaction] = TransactionType.join

    def read(self, _id: Id, min_awards: int = 0):
        transaction = self.send(Message(
            type=MsgType.find_successor,
            src=self.id,
            dst=self.id,
            args={"id": _id, "op": "read", "min_awards": min_awards},
        ))
        self.pending[transaction] = TransactionType.read

    def write(self, scientists: list[Scientist]):
        _id = Id(scientists[0].alma_mater[0])
        transaction = self.send(Message(
            type=MsgType.find_successor,
            src=self.id,
            dst=self.id,
            args={"id": _id, "op": "write", "scientists": scientists},
        ))
        self.pending[transaction] = TransactionType.write

    def lookup(self, _id: Id):
        transaction = self.send(Message(
            type=MsgType.find_successor,
            src=self.id,
            dst=self.id,
            args={"id": _id},
        ))
        self.pending[transaction] = TransactionType.lookup

    def stabilize(self):
        """Attempt to improve the ring's local connectivity.

        Meant to be process periodically. Improves correctness and performance.
        Running queries on a ring that hasn't fully stabilized will lead to some failing."""
        transaction = self.send(
            Message(
                type=MsgType.get_predecessor,
                src=self.id,
                dst=self.successor,
                args={},
            )
        )
        self.pending[transaction] = TransactionType.stabilize

    def fix_finger(self):
        """Do a lookup to fix a single finger in the table.

        Meant to be process periodically. Improves performance, but not correctness."""
        _id = self.id + 2 ** self._next_finger
        transaction = self.send(Message(
            type=MsgType.find_successor,
            src=self.id,
            dst=self.id,
            args={"id": _id},
        ))
        self.pending[transaction] = TransactionType.fix_finger
