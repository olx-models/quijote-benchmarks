"""
Quijote Item-page benchmark

Usage:

    First get a lot of item ids. Then run the bench.

    $ ./get_ids.py --env=qa2 --items=10000
    $ ./bench.py --env=qa2 --items=500 \
                 --threads=10 --threads=20 --threads=50 --sleep=20 \
                 --service=http://dev-models.olx.com.ar/quijote

"""

import sys
from optparse import OptionParser
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
        'urls_service': ['http://dev-models.olx.com.ar:8000/quijote'],
    },
    'dev-cluster': {
        'ids_module': 'ids_dev',
        'log_file': 'bench_dev_cluster_%s.log',
        'csv_file': 'bench_dev_cluster_%s.csv',
        'urls_service': ['http://dev-models.olx.com.ar:5001/quijote-cluster',
                         'http://dev-models.olx.com.ar:5002/quijote-cluster',
                         'http://dev-models.olx.com.ar:5003/quijote-cluster',
                         'http://dev-models.olx.com.ar:5004/quijote-cluster',
                        ],
    },
    'qa1': {
        'ids_module': 'ids_qa1',
        'log_file': 'bench_qa1_%s.log',
        'csv_file': 'bench_qa1_%s.csv',
        'urls_service': ['http://models-quijote-qa1.olx.com.ar'],
    },
    'qa2': {
        'ids_module': 'ids_qa2',
        'log_file': 'bench_qa2_%s.log',
        'csv_file': 'bench_qa2_%s.csv',
        'urls_service': ['http://models-quijote-qa2.olx.com.ar'],
    },
    'live': {
        'ids_module': 'ids_live',
        'log_file': 'bench_live_%s.log',
        'csv_file': 'bench_live_%s.csv',
        'urls_service': ['http://204.232.252.178'],
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
            for name, url_ in data['response']['resources'].items():
                if url_:
                    fetch_subresource(name, url_, reqs_counter)
        item_time = (time.time() - begin_time) * 1000  # ms
        items_counter.count_item(item_time)


def bench(threads, options):
    # Randomize IDs and construct the urls
    random.shuffle(ids.ids)
    urls_service = itertools.cycle(options.service)
    queue = Queue()
    for id in ids.ids[:options.items]:
        url = '%s/v1/site/1/item/%s' % (urls_service.next(), id)
        queue.put(url)

    # Create threads and counters
    jobs = []
    counters = []
    for id in range(threads):
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
    return get_result(bench_time_sec, counters, threads)


def get_result(bench_time_sec, counters, threads):
    items_counter = Counter('Items')
    reqs_counter = Counter('Requests')

    for c in counters:
        items_counter.count_counter(c[0])
        reqs_counter.count_counter(c[1])

    item_avg = float(items_counter.items) / bench_time_sec
    req_avg = float(reqs_counter.items) / bench_time_sec
    reqs_per_item = (float(reqs_counter.items) / float(items_counter.items))

    return {'threads': threads,
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
              "Threads %s" % result['threads'],
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

    titles = [('threads', 'Threads'),
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


def test_connection(services):
    for url in services:
        requests.get(url)


if __name__ == '__main__':
    # Parse arguments
    parser = OptionParser()
    parser.add_option('--env', type=str, default='dev',
                      help='Environment: dev, qa1, qa2, live.')
    parser.add_option('--items', type=int, default=10,
                      help='Amount of items.')
    parser.add_option('--threads', type=int, action='append', default=[],
                      help='Can be used multiple times for multiple benchs.')
    parser.add_option('--sleep', type=int, default=10,
                      help='Sleep time in seconds between benchmarks.')
    parser.add_option('--service', type=str, action='append', default=[],
                      help='Can be used multiple times for round robin.')
    (options, args) = parser.parse_args()

    # Load module with IDs
    try:
        module = ENVS[options.env]['ids_module']
        ids = __import__(module)
    except KeyError:
        sys.stdout.write('Environment %s does not exist\n' % options.env)
        sys.exit(1)
    except ImportError:
        sys.stdout.write('Module %s does not exist\n' % module)
        sys.exit(1)

    if len(options.threads) == 0:
        options.threads = [1]

    # URLs service
    if options.service:
        urls_service = []
        for url in options.service:
            while url.endswith('/'):
                url = url[:-1]
            urls_service.append(url)
        options.service = urls_service
    else:
        options.service = ENVS[options.env]['urls_service']

    time_id = time.strftime('%Y%m%d-%H%M%S')
    log_file = ENVS[options.env]['log_file'] % time_id
    csv_file = ENVS[options.env]['csv_file'] % time_id

    # Show benchmark settings before run benchmarks
    lines = ['Benchmark settings']
    for k in ('env', 'items', 'service', 'threads', 'sleep'):
        line = '\t%s: %s' % (k, getattr(options, k))
        lines.append(line)
    out = '\n'.join(lines)
    print out
    with open(log_file, 'a') as f:
        f.write('%s\n' % out)

    # Run benchmarks
    test_connection(options.service)
    results = []
    for threads in options.threads:
        result = bench(threads, options)
        save_to_log_file(result, log_file)
        results.append(result)
        if threads != options.threads[-1]:
            time.sleep(options.sleep)

    save_to_csv_file(results, csv_file)
