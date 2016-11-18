import argparse
import operator
import re
import json
from collections import defaultdict

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt  # noqa

NAME_RE = re.compile(r'([a-z_]+)\[(\d+)\]')


def parse_name(name):
    test_name, concurrency = NAME_RE.match(name).groups()
    method, driver = test_name.rsplit('_', 1)
    return method, driver, int(concurrency)


def plot(bench_path):
    with open(bench_path, 'r') as f:
        data = json.loads(f.read())

    aiomongo_results = defaultdict(list)
    motor_results = defaultdict(list)
    methods = set([])

    for item in data['benchmarks']:
        method, driver, concurrency = parse_name(item['name'])
        methods.add(method)
        stats = item['stats']
        stats_mean = stats['mean']
        if driver == 'motor':
            motor_results[method].append((concurrency, stats_mean))
        else:
            aiomongo_results[method].append((concurrency, stats_mean))

    for method in methods:
        aiomongo_method_results = sorted(aiomongo_results[method], key=operator.itemgetter(0))
        motor_method_results = sorted(motor_results[method], key=operator.itemgetter(0))

        fig = plt.figure()
        aiomongo_plot, = plt.plot(
            [x[0] for x in aiomongo_method_results], [x[1] for x in aiomongo_method_results], 'ro', label='aiomongo'
        )
        motor_plot, = plt.plot(
            [x[0] for x in motor_method_results], [x[1] for x in motor_method_results], 'bs', label='motor'
        )
        plt.xlabel('concurrency')
        plt.ylabel('execution time')
        plt.legend(handles=[aiomongo_plot, motor_plot], loc=2)
        fig.savefig('{}.png'.format(method))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--benchmark', help='path to json with benchmark results', default='benchmark.json')
    args = parser.parse_args()

    plot(args.benchmark)

