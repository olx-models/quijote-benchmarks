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
        self.name = name
        self.items = 0
        self.time = 0
        self.max = 0
        self.min = 99999999999999999

    def count_item(self, t):
        self.items += 1
        self.time += t
        if t > self.max:
            self.max = t
        if t < self.min:
            self.min = t

    def count_counter(self, counter):
        self.items += counter.items
        self.time += counter.time
        if counter.max > self.max:
            self.max = counter.max
        if counter.min < self.min:
            self.min = counter.min

    @property
    def avg(self):
        if self.items > 0:
            return self.time / float(self.items)
        else:
            return 0

    def __str__(self):
        chunks = (self.name,
                  'Items: %s' % self.items,
                  'Time %0.2f ms' % self.time,
                  'Avg: %0.2f ms' % self.avg,
                  'Max: %0.2f ms' % self.max,
                  'Min: %0.2f ms' % self.min,
                  )
        return ' - '.join(chunks)


def fetch(url, reqs_counter):
    begin_time = time.time()
    response = requests.get(url)
    req_time = (time.time() - begin_time) * 1000  # ms
    reqs_counter.count_item(req_time)
    return response


def fetch_subresource(name, url, reqs_counter):
    response = fetch(url, reqs_counter)
    if response.status_code == 200 and name in ('category', 'location', 'seo'):
        data = response.json()
        if name in ('category', 'location'):
            url_parent = data['response']['resources']['parent']
            if url_parent:
                fetch_subresource(name, url_parent, reqs_counter)
        if name == 'seo':
            for direction in ('next', 'prev'):
                url_seo = data['response']['resources'][direction]
                if url_seo:
                    fetch(url_seo, reqs_counter)


def worker(id, queue, counters):
    items_counter, reqs_counter = counters
    while True:
        try:
            url = queue.get_nowait()
        except Empty:
            break
        begin_time = time.time()
        response = fetch(url, reqs_counter)
        if response.status_code == 200:
            data = response.json()
            for name, url in data['response']['resources'].items():
                if url:
                    fetch_subresource(name, url, reqs_counter)
        item_time = (time.time() - begin_time) * 1000  # ms
        items_counter.count_item(item_time)


def bench(workers, env, items):
    # Randomize IDs and construct the urls
    random.shuffle(ids.ids)
    urls_base = itertools.cycle(ENVS[env]['urls_base'])
    queue = Queue()
    for id in ids.ids[:items]:
        url = '%s/v1/site/1/item/%s' % (urls_base.next(), id)
        queue.put(url)

    # Create threads and counters
    jobs = []
    counters = []
    for id in range(workers):
        items_counter = Counter('Items')
        reqs_counter = Counter('Requests')
        c = (items_counter, reqs_counter)
        t = threading.Thread(target=worker, args=(id, queue, c))
        jobs.append(t)
        counters.append(c)

    # Start timer
    begin_time = time.time()

    # Start threads
    for t in jobs:
        t.start()

    # Wait for end all threads
    for t in jobs:
        t.join()

    # Stop timer
    bench_time = (time.time() - begin_time) * 1000  # ms

    # Show result
    print_stats(bench_time, counters, workers)


def print_stats(bench_time, counters, workers):
    items_counter = Counter('Items')
    reqs_counter = Counter('Requests')

    for c in counters:
        items_counter.count_counter(c[0])
        reqs_counter.count_counter(c[1])

    print "=" * 40
    print workers, "Threads"
    print "\tTime: %0.2f s" % (bench_time / 1000.0)
    print "\tTotal Items:", items_counter.items
    item_avg = float(items_counter.items) / (bench_time / 1000.0)
    print "\tItems per second: %0.2f/s" % item_avg
    print "\tTotal requests:", reqs_counter.items
    req_avg = float(reqs_counter.items) / (bench_time / 1000.0)
    print "\tRequests per second: %0.2f/s" % req_avg
    reqs_per_item = (float(reqs_counter.items) / float(items_counter.items))
    print "\tRequests per item: %0.2f" % reqs_per_item
    print "\tMax item time: %0.2f ms" % items_counter.max
    print "\tMin item time: %0.2f ms" % items_counter.min
    print "\tAvg item time: %0.2f ms" % items_counter.avg
    print "\tMax request time: %0.2f ms" % reqs_counter.max
    print "\tMin request time: %0.2f ms" % reqs_counter.min
    print "\tAvg request time: %0.2f ms" % reqs_counter.avg


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
        bench(workers, args.env, args.items)
        if workers != args.workers[-1]:
            time.sleep(args.sleep)
