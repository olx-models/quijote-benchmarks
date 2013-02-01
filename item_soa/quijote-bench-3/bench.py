#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
import json
import random
import time
import itertools
import threading
from threading import RLock
import httplib

from optparse import OptionParser
from urlparse import urlparse
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


class Response:

    def __init__(self, code, body):
        self.status_code = code
        self.body = body

    def json(self):
        return json.loads(self.body)


class KeepAliveClient(object):

    def __init__(self):
        self._connections = {}

    def get(self, url, counter=None):
        url = urlparse(url)
        conn = self.get_connection(url.hostname, url.port)
        if counter:
            begin_time = time.time()
        conn.request('GET', url.path)
        response = conn.getresponse()
        if counter:
            req_time = (time.time() - begin_time) * 1000  # ms
            counter.count_item(req_time)
        return Response(response.status, response.read())

    def get_connection(self, host, port=80):
        key = host + ':' + str(port)
        conn = self._connections.get(key, None)
        if conn is None:
            conn = httplib.HTTPConnection(host, port)
            self._connections[key] = conn
        return conn

    def close_connection(self, host, port):
        key = host + ':' + str(port)
        if key in self._connections:
            self._connections[key].close()
            del(self._connections[key])

    def close_all_connections(self):
        for conn in self._connections.values():
            conn.close()
        self._connections = {}


class Counter(object):

    def __init__(self, name, glogger=None):
        self.name = name
        self.items = 0
        self.time = 0
        self.max = 0
        self.min = 99999999999999999
        self.errors = {}
        self.glogger = glogger

    def count_item(self, t):
        self.items += 1
        self.time += t
        if t > self.max:
            self.max = t
        if t < self.min:
            self.min = t

        if self.glogger is not None:
            self.glogger.add(int(t))

    def count_counter(self, counter):
        self.items += counter.items
        self.time += counter.time
        if counter.max > self.max:
            self.max = counter.max
        if counter.min < self.min:
            self.min = counter.min
        for code, urls in counter.errors.items():
            for url in urls:
                self.count_error(code, url)

    def count_error(self, code, url, referer=None):
        if code not in self.errors:
            self.errors[code] = []
        error = url
        if referer:
            error += ' - ' + referer
        self.errors[code].append(error)

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


class GaussLogger(object):
    def __init__(self):
        self._data = {}
        self._raw_data = []
        self._lock = RLock()

    def add(self, val):
        with self._lock:
            self._raw_data.append(val)

    def sumarize(self):
        for val in self._raw_data:
            if val not in self._data:
                self._data[val] = 0
            self._data[val] += 1

    def get_data(self):
        self.sumarize()
        max_key = max(self._data.keys())
        data = []
        for i in xrange(max_key + 1):
            data.append('%d;%d' % (i, self._data.get(i, 0)))
        return data


def fetch_subresource(name, url, http, reqs_counter, url_referer=None):
    response = http.get(url, reqs_counter)
    if response.status_code == 200:
        if name in ('category', 'location', 'seo'):
            data = response.json()
            if name in ('category', 'location'):
                url_parent = data['response']['resources']['parent']
                if url_parent:
                    fetch_subresource(name, url_parent, http, reqs_counter, url)
            if name == 'seo':
                for direction in ('next', 'prev'):
                    url_seo = data['response']['resources'][direction]
                    if url_seo:
                        fetch_subresource(direction, url_seo, http, reqs_counter, url)
    else:
        reqs_counter.count_error(response.status_code, url, url_referer)


def worker(id, queue, counters):
    items_counter, reqs_counter = counters
    http = KeepAliveClient()
    while True:
        try:
            url = queue.get_nowait()
        except Empty:
            break
        begin_time = time.time()
        response = http.get(url, reqs_counter)
        if response.status_code == 200:
            data = response.json()
            for name, url_ in data['response']['resources'].items():
                if url_:
                    fetch_subresource(name, url_, http, reqs_counter, url)
        else:
            reqs_counter.count_error(response.status_code, url)
        item_time = (time.time() - begin_time) * 1000  # ms
        items_counter.count_item(item_time)
    http.close_all_connections()


def bench(threads, options):
    # Randomize IDs and construct the urls
    random.shuffle(ids.ids)
    urls_service = itertools.cycle(options.service)
    queue = Queue()
    for id in ids.ids[:options.items]:
        url = '%s/v1/site/1/item/%s' % (urls_service.next(), id)
        queue.put(url)

    # Create threads, https and counters
    jobs = []
    counters = []
    glogger = GaussLogger()
    for id in range(threads):
        items_counter = Counter('Items')
        reqs_counter = Counter('Requests', glogger)
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

    with open('gauss.csv', 'w') as gfile:
        for l in glogger.get_data():
            gfile.write(l + '\n')

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
            'errors': reqs_counter.errors,
            }


def save_to_log_file(result, filename):
    errors = []
    for code, urls in result['errors'].items():
        errors.append("%s (%s)" % (code, len(urls)))
        for url in urls:
            errors.append(url)
    chunks = ("=" * 40,
              "Threads %s" % result['threads'],
              "\tTime: %ss" % result['time'],
              "\tTotal items: %s" % result['items'],
              "\tItems per second: %s/s" % result['items_per_second'],
              "\tTotal requests: %s" % result['requests'],
              "\tRequests per second: %s/s" % result['requests_per_second'],
              "\tRequests per item: %s" % result['requests_per_item'],
              "\tMax item time: %sms" % result['max_item_time'],
              "\tMin item time: %sms" % result['min_item_time'],
              "\tAvg item time: %sms" % result['avg_item_time'],
              "\tMax request time: %sms" % result['max_request_time'],
              "\tMin request time: %sms" % result['min_request_time'],
              "\tAvg request time: %sms" % result['avg_request_time'],
              "\n" + "\n".join(errors),
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
    http = KeepAliveClient()
    for url in services:
        http.get(url)
    http.close_all_connections()


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

    # URLs service and test connection
    if options.service:
        urls_service = []
        for url in options.service:
            while url.endswith('/'):
                url = url[:-1]
            urls_service.append(url)
        options.service = urls_service
    else:
        options.service = ENVS[options.env]['urls_service']
    test_connection(options.service)

    # Files
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
    with open(log_file, 'w') as f:
        f.write('%s\n' % out)

    # Run benchmarks
    results = []
    for threads in options.threads:
        result = bench(threads, options)
        save_to_log_file(result, log_file)
        results.append(result)
        if threads != options.threads[-1]:
            time.sleep(options.sleep)

    save_to_csv_file(results, csv_file)
