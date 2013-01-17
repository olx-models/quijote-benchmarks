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
        'log_file': 'bench_dev_%s.log',
        'csv_file': 'bench_dev_%s.csv',
        'urls_base': ['http://dev-models.olx.com.ar:8000/quijote'],
    },
    'dev-cluster': {
        'ids_module': 'ids_dev',
        'log_file': 'bench_dev_cluster_%s.log',
        'csv_file': 'bench_dev_cluster_%s.csv',
        'urls_base': ['http://dev-models.olx.com.ar:5001/quijote-cluster',
                      'http://dev-models.olx.com.ar:5002/quijote-cluster',
                      'http://dev-models.olx.com.ar:5003/quijote-cluster',
                      'http://dev-models.olx.com.ar:5004/quijote-cluster',
                      ],
    },
    'qa1': {
        'ids_module': 'ids_qa1',
        'log_file': 'bench_qa1_%s.log',
        'csv_file': 'bench_qa1_%s.csv',
        'urls_base': ['http://models-quijote-qa1.olx.com.ar'],
    },
    'qa2': {
        'ids_module': 'ids_qa2',
        'log_file': 'bench_qa2_%s.log',
        'csv_file': 'bench_qa2_%s.csv',
        'urls_base': ['http://models-quijote-qa2.olx.com.ar'],
    },
    'live': {
        'ids_module': 'ids_live',
        'log_file': 'bench_live_%s.log',
        'csv_file': 'bench_live_%s.csv',
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

    def avg(self):
        if self.items > 0:
            return self.time / float(self.items)
        else:
            return 0

    def __str__(self):
        chunks = (self.name,
                  'Items: %s' % self.items,
                  'Time %0.2fms' % self.time,
                  'Avg: %0.2fms' % self.avg(),
                  'Max: %0.2fms' % self.max,
                  'Min: %0.2fms' % self.min,
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

    begin_time = time.time()

    # Start threads
    for t in jobs:
        t.start()

    # Wait for end all threads
    for t in jobs:
        t.join()

    bench_time_sec = time.time() - begin_time  # s
    return get_result(bench_time_sec, counters, workers)


def get_result(bench_time_sec, counters, workers):
    items_counter = Counter('Items')
    reqs_counter = Counter('Requests')

    for c in counters:
        items_counter.count_counter(c[0])
        reqs_counter.count_counter(c[1])

    item_avg = float(items_counter.items) / bench_time_sec
    req_avg = float(reqs_counter.items) / bench_time_sec
    reqs_per_item = (float(reqs_counter.items) / float(items_counter.items))

    return {'workers': workers,
            'time': round(bench_time_sec, 2),
            'items': items_counter.items,
            'items_per_second': round(item_avg, 2),
            'requests': reqs_counter.items,
            'requests_per_second': round(req_avg, 2),
            'requests_per_item': round(reqs_per_item, 2),
            'max_item_time': round(items_counter.max, 2),
            'min_item_time': round(items_counter.min, 2),
            'avg_item_time': round(items_counter.avg(), 2),
            'max_request_time': round(reqs_counter.max, 2),
            'min_request_time': round(reqs_counter.min, 2),
            'avg_request_time': round(reqs_counter.avg(), 2),
            }


def save_to_log_file(result, filename):
    chunks = ("=" * 40,
              "Threads %s" % result['workers'],
              "\tTime: %ss" % result['time'],
              "\tTotal items: %s" % result['items'],
              "\tItems per second: %ss" % result['items_per_second'],
              "\tTotal requests: %s" % result['requests'],
              "\tRequests per second: %s/s" % result['requests_per_second'],
              "\tRequests per item: %s" % result['requests_per_item'],
              "\tMax item time: %sms" % result['max_item_time'],
              "\tMin item time: %sms" % result['min_item_time'],
              "\tAvg item time: %sms" % result['avg_item_time'],
              "\tMax request time: %sms" % result['max_request_time'],
              "\tMin request time: %sms" % result['min_request_time'],
              "\tAvg request time: %sms" % result['avg_request_time'],
             )
    out = '\n'.join(chunks)
    print out
    with open(filename, 'a') as f:
        f.write('%s\n' % out)


def save_to_csv_file(results, filename):
    rows = {}
    for key in results[0]:
        rows[key] = []
    for result in results:
        for key in results[0]:
            rows[key].append(str(result[key]))

    titles = [('workers', 'Threads'),
              ('items', 'Items'), 
              ('time', 'Time (s)'),
              ('requests', 'Total requests'),
              ('requests_per_second', 'Requests per second'),
              ('requests_per_item', 'Requests per item'),
              ('max_item_time', 'Max item time (ms)'),
              ('min_item_time', 'Min item time (ms)'),
              ('avg_item_time', 'Avg item time (ms)'),
              ('max_request_time', 'Max request time (ms)'),
              ('min_request_time', 'Min request time (ms)'),
              ('avg_request_time', 'Avg request time (ms)'),
             ]
    with open(filename, 'w') as f:
        for key, title in titles:
            f.write("%s,%s\n" % (title, ','.join(rows[key])))


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

    time_id = time.strftime('%Y%m%d-%H%M%S')
    log_file = ENVS[args.env]['log_file'] % time_id
    csv_file = ENVS[args.env]['csv_file'] % time_id

    # Run benchmarks
    results = []
    for workers in args.workers:
        result = bench(workers, args.env, args.items)
        save_to_log_file(result, log_file)
        results.append(result)
        if workers != args.workers[-1]:
            time.sleep(args.sleep)

    save_to_csv_file(results, csv_file)
