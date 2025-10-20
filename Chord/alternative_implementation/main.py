import time
import json

from random import randint
from dataclasses import dataclass
from collections import defaultdict

import chord


@dataclass
class Scientist:
    name: str
    awards: list[str]
    alma_mater: list[str]


class StorageNode(chord.Node):
    def __init__(self, name: str, transport: chord.Transport):
        self._store: dict[str, list[Scientist]] = {}
        super().__init__(name, transport)

    def read(self, key: str, min_award_count: int = 0):
        node_id = self.lookup(key)['node_id']
        if node_id == self.id:
            scientists: list[Scientist] = self._store.get(chord.Id(key), [])
            return {
                "scientists": [s for s in scientists if len(s.awards) >= min_award_count],
                "node_name": self.name,
                "node_id": self.id
            }
        return self.RPC(node_id, 'read', key=key, min_award_count=min_award_count)

    def write(self, key: str, scientists: list[Scientist]) -> chord.Id:
        node_id = self.lookup(key)['node_id']
        if node_id == self.id:
            self._store[chord.Id(key)] = scientists
            return self.id
        return self.RPC(node_id, 'write', key=key, scientists=scientists)


def create_dht(node_count: int) -> list[StorageNode]:
    """Create a DHT and return its nodes."""
    t = chord.Transport()
    nodes = [StorageNode("node0", t)]
    for i in range(1, node_count):
        n = StorageNode(f'node{i}', t)
        n.join(nodes[0].id)
        nodes.append(n)
        for j in range(i, -1, -1):
            nodes[j].stabilize()

    return nodes


def test_ring(nodes):
    print('Testing ring...')
    tests = 1000
    successes = 0
    t0 = time.time()
    for i in range(1000):
        # Random node and id
        node = nodes[randint(0, len(nodes) - 1)]
        id = chord.Id.random()

        # Do a local lookup, then check result
        res = node.find_successor(id)

        if res == node.transport.global_lookup(id):
            successes += 1

    elapsed = time.time() - t0
    print(elapsed)
    print(f'{successes}/{tests} correct lookups ({round(100 * successes / tests)}%) in {format_ms(elapsed)}')
    # print(f'(mean: {format_us(elapsed / tests)})')


def format_ms(seconds):
    return f'{round(1e3 * seconds)} ms'


def format_us(seconds):
    return f'{round(1e6 * seconds)} μs'


if __name__ == '__main__':
    node_count = int(input('Node count: '))
    performance = float(input('Performance factor: '))

    t0 = time.time()
    nodes = create_dht(node_count)
    elapsed = time.time() - t0
    print(f'Ring created in {format_ms(elapsed)}')

    t0 = time.time()
    for i in range(round(performance * chord.Id.bits)):
        for node in nodes:
            node.fix_finger()
            # node.stabilize()
    elapsed = time.time() - t0
    print(f'Stabilized in {format_ms(elapsed)}')

    test_ring(nodes)

    with open('scientists.json', encoding='utf-8') as f:
        scientists: list[dict] = json.load(f)

    print(f'Loaded {len(scientists)} scientists')

    # Group by first alma mater
    groups = defaultdict(lambda: [])
    unknown = []
    for s in scientists:
        almas = s.get('alma_mater', [])
        if len(almas) == 0:
            unknown.append(s['name'])
            continue
        groups[almas[0]].append(Scientist(**s))

    if len(unknown) > 0:
        print(f'Ignoring {len(unknown)} scientists with no alma mater')

    t0 = time.time()
    for alma, group in groups.items():
        nodes[0].write(alma, group)
    elapsed = time.time() - t0
    print(f'Wrote {len(scientists) - len(unknown)} scientists to DHT'
          f' in {len(groups.keys())} transactions and {format_ms(elapsed)}')

    print()
    succs = {}
    for node in nodes:
        succs[node.name] = node.transport.get_name(node.successor)

    chain = []
    node = "node0"
    while node not in chain:
        chain.append(node)
        node = succs[node]
    chain.append(node)
    print(chain, len(chain))

    while True:
        alma = input("\nAlma mater: ")
        min_award_count = int(input("Minimum award count: "))
        client = nodes[randint(0, len(nodes) - 1)]

        t0 = time.time()
        result = client.read(alma, min_award_count)
        elapsed = time.time() - t0

        print(f'\nFound {len(result["scientists"])} scientists in {format_us(elapsed)}.')
        print('-' * 60)
        print('Award count | Name')
        print('-' * 60)
        for s in result['scientists']:
            print(f'{len(s.awards) :11} | {s.name}')
