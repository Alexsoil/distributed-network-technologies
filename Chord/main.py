import json
import time
from collections import defaultdict

from chord import Scientist, Simulator, Id


def format_ms(seconds):
    return f'{round(1e3 * seconds)} ms'


def format_us(seconds):
    return f'{round(1e6 * seconds)} μs'


if __name__ == "__main__":
    # === Create ring ===
    node_count = int(input('Node count: '))
    stabilize_iters = int(input('Stabilization passes: '))
    optimize_iters = int(input('Optimization passes: '))

    print()
    t0 = time.monotonic_ns()
    sim = Simulator(node_count)
    create_time = time.monotonic_ns() - t0
    print(f'Ring created in {create_time / 1e6:.0f} ms')

    t0 = time.monotonic_ns()
    sim.stabilize(stabilize_iters)
    stab_time = time.monotonic_ns() - t0
    print(f'Stabilized in {stab_time / 1e6:.0f} ms')

    t0 = time.monotonic_ns()
    sim.optimize(optimize_iters)
    opt_time = time.monotonic_ns() - t0
    print(f'Optimized in {opt_time / 1e6:.0f} ms')

    print()
    print('Ring fully stable:', sim.is_fully_stable())
    print('Testing ring...')
    elapsed, correct, total = sim.test_ring()
    print(f'{correct}/{total} correct lookups ({correct / total:.0%}) in {format_ms(elapsed)}')
    print()

    #
    # === Load scientists ===
    #

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

    client = sim.random_node()

    t0 = time.time()
    for alma, group in groups.items():
        client.write(group)
    sim.process()
    elapsed = time.time() - t0

    print(f'Wrote {len(scientists) - len(unknown)} scientists to DHT'
          f' in {len(groups.keys())} transactions and {format_ms(elapsed)}')

    #
    # === User input ===
    #

    while True:
        alma = input("\nAlma mater: ")
        min_award_count = int(input("Minimum award count: "))
        client = sim.random_node()

        t0 = time.time()
        client.read(Id(alma), min_award_count)
        _, host, results = sim.process()[0]
        elapsed = time.time() - t0

        print(f'\nFound {len(results)} scientists on host {sim.get_name(host)} in {format_us(elapsed)}.')
        print('-' * 60)
        print('Award count | Name')
        print('-' * 60)
        for s in results:
            print(f'{len(s.awards) :11} | {s.name}')