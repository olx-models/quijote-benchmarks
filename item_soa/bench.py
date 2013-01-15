#!/usr/bin/env python

import sys
import argparse
import random
import time
import itertools
import requests
import threading
from Queue import Queue, Empty


ENVS = {
    'dev': {
        'ids_module': 'ids_dev',
        'log_file': 'result_dev.log',
        'urls_base': ['http://dev-models.olx.com.ar:8000/quijote'],
    },
    'dev-cluster': {
        'ids_module': 'ids_dev',
        'log_file': 'result_dev_cluster.log',
        'urls_base': ['http://dev-models.olx.com.ar:5001/quijote-cluster',
                      'http://dev-models.olx.com.ar:5002/quijote-cluster',
                      'http://dev-models.olx.com.ar:5003/quijote-cluster',
                      'http://dev-models.olx.com.ar:5004/quijote-cluster',
                      ],
    },
    'qa1': {
        'ids_module': 'ids_qa1',
        'log_file': 'result_qa1.log',
        'urls_base': ['http://models-quijote-qa1.olx.com.ar'],
    },
    'qa2': {
        'ids_module': 'ids_qa2',
        'log_file': 'result_qa2.log',
        'urls_base': ['http://models-quijote-qa2.olx.com.ar'],
    },
    'live': {
        'ids_module': 'ids_live',
        'log_file': 'result_live.log',
        'urls_base': ['http://204.232.252.178'],
    },
}


class Counter(object):

    def __init__(self, name):
        self.workers = 1
        self.name = name
        self.total_items = 0
        self.total_time = 0
        self.max_time = 0
        self.min_time = 99999999999999999

    def count_item(self, t):
        self.total_items += 1
        self.total_time += t
        if t > self.max_time:
            self.max_time = t
        if t < self.min_time:
            self.min_time = t

    def count_counter(self, counter):
        self.total_items += counter.total_items
        self.total_time += counter.total_time
        if counter.max_time > self.max_time:
            self.max_time = counter.max_time
        if counter.min_time < self.min_time:
            self.min_time = counter.min_time

    def stats(self):
        if self.total_items > 0:
            avg_time = self.total_time / float(self.total_items)
            avg_items = float(self.total_items) / (self.total_time / 1000.0)
        else:
            avg_time = 0
            avg_items = 0

        workers = float(self.workers)
        return {'name': self.name,
                'items': self.total_items,
                'time': self.total_time / workers,
                'time_avg': avg_time / workers,
                'time_max': self.max_time / workers,
                'time_min': self.min_time / workers,
                'items_avg': avg_items * workers,
                }

    def __str__(self):
        stats = self.stats()
        fmt = {'name': '%s',
               'items': '\tItems: %s',
               'time': '\tTime: %0.2f ms',
               'time_avg': '\tTime avg: %0.2f ms',
               'time_max': '\tTime max: %0.2f ms',
               'time_min': '\tTime min: %0.2f ms',
               'items_avg': '\tItems Avg: %0.2f/s',
               }
        keys = ('name', 'items', 'items_avg', 'time', 'time_avg',
                'time_max', 'time_min',)
        return '\n'.join(fmt[k] % stats[k] for k in keys)


def fetch(url, counter, verbose=False):
    if verbose:
        sys.stdout.write('.')
        sys.stdout.flush()
    begin_time = time.time()
    response = requests.get(url)
    delta = (time.time() - begin_time) * 1000  # ms
    counter.count_item(delta)
    return response


def fetch_subresource(name, url, counter, verbose=False):
    response = fetch(url, counter, verbose)
    if response.status_code == 200 and name in ('category', 'location', 'seo'):
        data = response.json()
        if name in ('category', 'location'):
            url_parent = data['response']['resources']['parent']
            if url_parent:
                fetch_subresource(name, url_parent, counter, verbose)
        if name == 'seo':
            for direction in ('next', 'prev'):
                url_seo = data['response']['resources'][direction]
                if url_seo:
                    fetch(url_seo, counter, verbose)


def worker(id, queue, counters, verbose=False):
    item_counter, reqs_counter = counters
    if verbose:
        sys.stdout.write("Worker %s started\n" % id)
        sys.stdout.flush()
    time.sleep(2)

    while True:
        try:
            url = queue.get_nowait()
        except Empty:
            break
        item_reqs_counter = Counter('Item Request')
        response = fetch(url, item_reqs_counter)
        if response.status_code == 200:
            data = response.json()
            for name, url in data['response']['resources'].items():
                if url:
                    fetch_subresource(name, url, item_reqs_counter, verbose)
        reqs_counter.count_counter(item_reqs_counter)
        item_counter.count_item(item_reqs_counter.total_time)


def bench(workers, env, items, verbose=False):
    # Randomize IDs and construct the urls
    random.shuffle(ids.ids)
    urls_base = itertools.cycle(ENVS[env]['urls_base'])
    queue = Queue()
    for id in ids.ids[:items]:
        url = '%s/v1/site/1/item/%s' % (urls_base.next(), id)
        queue.put(url)

    # Start threads
    jobs = []
    counters = []
    for id in range(workers):
        c = ([Counter('Item Pages'), Counter('Total Requests')])
        t = threading.Thread(target=worker, args=(id, queue, c, verbose))
        jobs.append(t)
        counters.append(c)
        t.start()

    # End threads
    for t in jobs:
        t.join()

    # Show result
    if verbose:
        print
    print_stats(counters, workers)


def print_stats(counters, workers):
    final_item_counter = Counter('Item Pages')
    final_item_counter.workers = workers
    final_reqs_counter = Counter('Total Requests')
    final_reqs_counter.workers = workers

    for c in counters:
        final_item_counter.count_counter(c[0])
        final_reqs_counter.count_counter(c[1])

    print "-" * 40
    print workers, "Workers"
    print "-" * 40
    print final_item_counter
    print final_reqs_counter


if __name__ == '__main__':
    # Parse arguments
    parser = argparse.ArgumentParser(description='Quijote benchmarks.')
    parser.add_argument('--env', type=str, required=True,
                        help='Environment: dev, qa1, qa2, live.')
    parser.add_argument('--items', type=int, required=True,
                        help='Amount of items.')
    parser.add_argument('--workers', type=int, required=True, action='append',
                        help='Threads. Can be used multiple times.')
    parser.add_argument('--sleep', type=int, default=10,
                        help='Sleep time in seconds between benchmarks.')
    parser.add_argument('--verbose', action='store_true')
    args = parser.parse_args()

    # Load module with IDs
    try:
        module = ENVS[args.env]['ids_module']
        ids = __import__(module)
    except KeyError:
        sys.stdout.write('Env %s does not exist\n' % args.env)
        sys.exit(1)
    except ImportError:
        sys.stdout.write('Module %s does not exist\n' % module)
        sys.exit(1)

    # Run benchmarks
    for workers in args.workers:
        bench(workers, args.env, args.items, args.verbose)
        if workers != args.workers[-1]:
            time.sleep(args.sleep)


