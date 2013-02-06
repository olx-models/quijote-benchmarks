#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Quijote Item-page freq benchmark

Usage:

    First get a lot of item ids. Then run the bench.

    $ ./get_ids.py --env=qa2 --items=10000
    $ ./bench.py --env=qa2 --time=60 \
                 --freq=100 --freq=200 --freq=250 --sleep=20 \
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

from datetime import datetime
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
                         'http://dev-models.olx.com.ar:5005/quijote-cluster',
                         'http://dev-models.olx.com.ar:5006/quijote-cluster',
                         'http://dev-models.olx.com.ar:5007/quijote-cluster',
                         'http://dev-models.olx.com.ar:5008/quijote-cluster',
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
            time_begin = time.time()
        conn.request('GET', url.path)
        response = conn.getresponse()
        if counter:
            req_time = (time.time() - time_begin) * 1000  # ms
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


class Counter2:

    def __init__(self, time_limit):
        self.time_limit = time_limit
        self._lock = RLock()
        self.items = []
        self.last_time = 0

    def count(self, delta, end_time):
        with self._lock:
            in_time = end_time <= self.time_limit
            self.items.append((delta, end_time, in_time))
            if end_time > self.last_time:
                self.last_time = end_time

    def get_in_time(self):
        return [item for item in self.items if item[2]]

    def get_max_time(self):
        return max(self.items, key=lambda x: x[0])[0]

    def get_min_time(self):
        return min(self.items, key=lambda x: x[0])[0]

    def get_avg_time(self):
        sum = reduce(lambda x, y: x + y, [t[0] for t in self.items])
        return sum / float(len(self.items))

    #def get_avg_time(self):
    #    in_time = self.get_in_time()
    #    sum = reduce(lambda x, y: x + y, [t[0] for t in in_time])
    #    return sum / float(len(in_time))


CACHE = {}
def cached(fn):
    global CACHE
    def inner(*args, **kwargs):
        url = args[0]
        data = CACHE.get(url, None)
        if data is None:
            data = fn(*args, **kwargs)
            if data.status_code == 200:
                CACHE[url] = data
        return data
    return inner


@cached
def requester(url, time_limit, counter, http=None):
    if http is None:
        http = KeepAliveClient()
    time_begin = time.time()
    response = http.get(url)
    end_time = time.time()
    delta = (end_time - time_begin) * 1000 #ms
    counter.count(delta, end_time)
    return response


def item_page_requester(url, time_limit, counter):
    http = KeepAliveClient()
    item = requester(url, time_limit, counter, http)
    if item.status_code != 200:
        return
    itemdoc = item.json()

    for subres in ('category', 'location', 'seo', 'dynamic_data', 'currency', 'images', 'user'):
        subresurl = itemdoc['response']['resources'][subres]
        if subresurl:
            subdoc = requester(subresurl, time_limit, counter, http)
            if subdoc.status_code == 200:
                subdoc = subdoc.json()
                if subres in ('category', 'location'):
                    parenturl = subdoc['response']['resources']['parent']
                    if parenturl:
                        parentdoc = requester(parenturl, time_limit, counter, http)
                if subres == 'seo':
                    for np in ('next', 'prev'):
                        npurl = subdoc['response']['resources'][np]
                        if npurl:
                            npdoc = requester(npurl, time_limit, counter, http)
    http.close_all_connections()
    return item


def bench(freq, options):
    # Randomize IDs and construct the urls
    random.shuffle(ids.ids)
    urls_service = itertools.cycle(options.service)
    queue = Queue()
    for id in ids.ids:
        url = '%s/v1/site/1/item/%s' % (urls_service.next(), id)
        queue.put(url)

    time_begin = time.time()
    time_limit = time_begin + options.time
    counter = Counter2(time_limit)

    i = 0
    threads = []
    time_between_threads = 1.0 / freq
    while time.time() < time_limit:
        i += 1
        url = queue.get_nowait()
        sys.stdout.write('.')
        sys.stdout.flush()
        t = threading.Thread(target=item_page_requester, args=(url, time_limit, counter))
        t.start()
        threads.append(t)
        time.sleep(time_between_threads)

    for t in threads:
        t.join()

    return get_result(freq, time_begin, time_limit, counter)


def get_result(freq, time_begin, time_limit, counter):
    time_between_threads = 1.0 / freq
    time_fmt = '%H:%M:%S.%f'
    time_begin_str = datetime.fromtimestamp(time_begin).strftime(time_fmt)
    time_limit_str = datetime.fromtimestamp(time_limit).strftime(time_fmt)
    time_elapsed = counter.last_time - time_begin
    real_freq = round(len(counter.items) / time_elapsed, 2)

    return {'freq': freq,
            'time_between_threads': time_between_threads, 
            'real_freq': real_freq,
            'time_begin': time_begin_str,
            'time_limit': time_limit_str,
            'time_elapsed': round(time_elapsed, 2),
            'requests_total': len(counter.items),
            'requests_finished_in_time': len(counter.get_in_time()),
            'max_request_time': round(counter.get_max_time(), 2),
            'min_request_time': round(counter.get_min_time(), 2),
            'avg_request_time': round(counter.get_avg_time(), 2),
            }


def save_to_log_file(result, filename):
    reqs = result['requests_total']
    reqs_in_time = result['requests_finished_in_time']
    rit_perc = reqs_in_time * 100.0 / reqs
    chunks = ("=" * 40,
              "Freq %s/s" % result['freq'],
              "\tResponse freq: %s/s" % result['real_freq'],
              "\tTime begin: %s" % result['time_begin'],
              "\tTime limit: %s" % result['time_limit'],
              "\tTime elapsed: %ss" % result['time_elapsed'],
              "\tRequests launched: %s" % reqs,
              "\tRequests finished in time: %s (%.2f%%)" % (reqs_in_time, rit_perc),
              "\tMax request time: %sms" % result['max_request_time'],
              "\tMin request time: %sms" % result['min_request_time'],
              "\tAvg request time: %sms" % result['avg_request_time'],
              )
    out = '\n'.join(chunks)
    print '\n%s' % out
    with open(filename, 'a') as f:
        f.write('%s\n' % out)


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
    parser.add_option('--time', type=float, default=60,
                      help='Benchmark duration.')
    parser.add_option('--freq', type=int, action='append', default=[],
                      help='Amount of request by second.')
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

    if len(options.freq) == 0:
        options.freq = [10]

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
    #csv_file = ENVS[options.env]['csv_file'] % time_id

    # Show benchmark settings before run benchmarks
    lines = ['Benchmark settings']
    for k in ('env', 'time', 'service', 'freq', 'sleep'):
        line = '\t%s: %s' % (k, getattr(options, k))
        lines.append(line)
    out = '\n'.join(lines)
    print out
    with open(log_file, 'w') as f:
        f.write('%s\n' % out)

    # Run benchmarks
    results = []
    for freq in options.freq:
        CACHE = {}
        result = bench(freq, options)
        save_to_log_file(result, log_file)
        if f != options.freq[-1]:
            time.sleep(options.sleep)
    #save_to_csv_file(results, csv_file)
