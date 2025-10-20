import hashlib
import time
import asyncio

from random import randint
from typing import Self
from bisect import bisect
from enum import Enum


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


class Transport:
    def __init__(self):
        self._nodes: dict[Id, Node] = {}

    def register(self, node: "Node"):
        self._nodes[node.id] = node

    # == Testing and debugging ==

    def get_name(self, node: Id):
        return self._nodes[node].name

    def global_lookup(self, _id: Id):
        """Do an id lookup with perfect knowledge of the ring.

        Can be used to check if a local lookup (i.e. from a node) returned the correct result."""
        ids = sorted([n.id for _, n in self._nodes.items()])
        succ_idx = bisect(ids, _id) % len(ids)
        return self._nodes[ids[succ_idx]].id

    # == RPC interface ==

    # Inter-node communication can happen either through RPC(), which is slower
    # since it uses reflection, or through the individual methods that wrap
    # the node's methods (e.g. transport.notify(), which calls node.notify()).

    def RPC(self, target: Id, method: str, **kwargs):
        target = self._nodes[target]
        return getattr(target, method)(**kwargs)

    def find_successor(self, target: Id, _id: Id) -> Id:
        return self._nodes[target].find_successor(_id)

    def notify(self, target: Id, notifier: Id):
        self._nodes[target].notify(notifier)

    def get_predecessor(self, target: Id) -> Id | None:
        return self._nodes[target].get_predecessor()



class Node:
    def __init__(self, name: str, transport: Transport):
        self.name = name
        self.id = Id(name)
        self._fingers: list[Id] = [self.id] * Id.bits
        self._next_finger = 0
        self._predecessor: Id | None = None
        self._transport: Transport = transport
        self._transport.register(self)

    @property
    def successor(self) -> Id:
        return self._fingers[0]

    @property
    def transport(self):
        return self._transport

    @successor.setter
    def successor(self, value: Id):
        self._fingers[0] = value

    def RPC(self, target: Id, method: str, **kwargs):
        return self._transport.RPC(target, method, **kwargs)

    def join(self, node_id: Id):
        self.successor = self.transport.find_successor(node_id, self.id)

    # === RPC interface ===

    def get_predecessor(self):
        return self._predecessor

    def find_successor(self, _id: Id) -> Id:
        if _id.between_right_incl(self.id, self.successor):
            return self.successor
        closest = self.closest_preceding_node(_id)
        return self.transport.find_successor(closest, _id)

    def closest_preceding_node(self, _id: Id) -> Id:
        for i in range(Id.bits - 1, -1, -1):
            if self._fingers[i].between(self.id, _id):
                return self._fingers[i]
        return self.id

    def notify(self, candidate: Id):
        """Notify self that another node thinks might be our predecessor."""
        if self._predecessor is None or candidate.between(self._predecessor, self.id):
            self._predecessor = candidate

    def stabilize(self):
        """Attempt to improve the ring's local connectivity.

        Meant to be process periodically. Improves correctness and performance.
        Running queries on a ring that hasn't fully stabilized will lead to some of them failing."""
        x: Id = self.transport.get_predecessor(self.successor)
        if x is not None and x.between(self.id, self.successor):
            self.successor = x
        self.transport.notify(self.successor, self.id)

    def fix_finger(self):
        """Do a lookup to fix a single finger in the table.

        Meant to be process periodically. Improves performance, but not correctness."""
        _id = self.id + 2 ** self._next_finger
        self._fingers[self._next_finger] = self.find_successor(_id)
        self._next_finger += 1
        self._next_finger %= Id.bits

    # ==

    def lookup(self, key: str):
        t0 = time.time()
        node_id = self.find_successor(Id(key))
        elapsed = time.time() - t0
        return {"node_id": node_id, "elapsed": elapsed}


def create_ring(node_count: int) -> list[Node]:
    """Create a Chord ring and return its nodes."""
    t = Transport()
    nodes = [Node("node0", t)]
    for i in range(1, node_count):
        n = Node(f'node{i}', t)
        n.join(nodes[0].id)
        nodes.append(n)

    return nodes
