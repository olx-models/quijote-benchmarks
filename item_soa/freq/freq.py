#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Quijote Item-page freq benchmark

Usage:

    First get a lot of item ids. Then run the bench.

    $ ./get_ids.py --env=qa2 --items=100000
    $ ./freq.py --env=qa2 --duration=60 \
                --freq=30 --freq=40 --freq=50 --sleep=20 \
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
from Queue import Queue


ENVS = {
    'dev': {
        'ids_module': 'ids_dev',
        'log_file': '%s_freq_dev.log',
        'csv_log_file': '%s_freq_dev_log.csv',
        'csv_gauss_file': '%s_freq_dev_gauss.csv',
        'urls_service': ['http://dev-models.olx.com.ar:8000/quijote'],
    },
    'dev-cluster': {
        'ids_module': 'ids_dev',
        'log_file': '%s_freq_dev_cluster.log',
        'csv_log_file': '%s_freq_dev_cluster_log.csv',
        'csv_gauss_file': '%s_freq_dev_cluster_gauss.csv',
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
        'log_file': '%s_freq_qa1.log',
        'csv_log_file': '%s_freq_qa1_log.csv',
        'csv_gauss_file': '%s_freq_qa1_gauss.csv',
        'urls_service': ['http://models-quijote-qa1.olx.com.ar'],
    },
    'qa2': {
        'ids_module': 'ids_qa2',
        'log_file': '%s_freq_qa2.log',
        'csv_log_file': '%s_freq_qa2_log.csv',
        'csv_gauss_file': '%s_freq_qa2_gauss.csv',
        'urls_service': ['http://models-quijote-qa2.olx.com.ar'],
    },
    'live': {
        'ids_module': 'ids_live',
        'log_file': '%s_freq_live.log',
        'csv_log_file': '%s_freq_live_log.csv',
        'csv_gauss_file': '%s_freq_live_gauss.csv',
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


class Counter:

    def __init__(self, time_limit):
        self.time_limit = time_limit
        self._lock = RLock()
        self.int_values = []
        self.items = []
        self.last_time = 0

    def count(self, t, end_time):
        with self._lock:
            self.int_values.append(int(t))
            in_time = end_time <= self.time_limit
            self.items.append((t, end_time, in_time))
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

    def gauss(self):
        max_key = max(self.int_values)
        data = [0] * (max_key + 1)
        for v in self.int_values:
            data[v] += 1
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
    req_time = (end_time - time_begin) * 1000 #ms
    counter.count(req_time, end_time)
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
    time_limit = time_begin + options.duration
    counter = Counter(time_limit)

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

    requests_total = len(counter.items)
    requests_in_time = len(counter.get_in_time())
    requests_in_time_perc = requests_in_time * 100.0 / requests_total

    gauss = counter.gauss()

    return {'freq': freq,
            'time_between_threads': time_between_threads, 
            'real_freq': real_freq,
            'time_begin': time_begin_str,
            'time_limit': time_limit_str,
            'time_elapsed': round(time_elapsed, 2),
            'requests_total': requests_total,
            'requests_in_time': requests_in_time,
            'requests_in_time_perc': requests_in_time_perc,
            'max_request_time': round(counter.get_max_time(), 2),
            'min_request_time': round(counter.get_min_time(), 2),
            'avg_request_time': round(counter.get_avg_time(), 2),
            'gauss': gauss,
            'gauss_len': len(gauss),
            }


def save_to_log_file(result, filename):
    chunks = ("=" * 40,
              "Freq %s/s" % result['freq'],
              "\tResponse freq: %s/s" % result['real_freq'],
              "\tTime begin: %s" % result['time_begin'],
              "\tTime limit: %s" % result['time_limit'],
              "\tTime elapsed: %ss" % result['time_elapsed'],
              "\tRequests: %s" % result['requests_total'],
              "\tRequests in time: %s" % result['requests_in_time'],
              "\tRequests in time perc: %.2f%%" % result['requests_in_time_perc'],
              "\tMax request time: %sms" % result['max_request_time'],
              "\tMin request time: %sms" % result['min_request_time'],
              "\tAvg request time: %sms" % result['avg_request_time'],
              )
    out = '\n'.join(chunks)
    print '\n%s' % out
    with open(filename, 'a') as f:
        f.write('%s\n' % out)


def save_to_csv_log_file(results, filename):
    rows = {}
    for key in results[0]:
        rows[key] = []
    for result in results:
        for key in results[0]:
            rows[key].append(str(result[key]))

    titles = [('freq', 'Freq'),
              ('real_freq', 'Response freq (reqs/s)'), 
              ('time_begin', 'Time begin'),
              ('time_limit', 'Time limit'),
              ('time_elapsed', 'Time elapsed (s)'),
              ('requests_total', 'Requests'),
              ('requests_in_time', 'Requests in time'),
              ('requests_in_time_perc', 'Requests in time perc (%)'),
              ('max_request_time', 'Max request time (ms)'),
              ('min_request_time', 'Min request time (ms)'),
              ('avg_request_time', 'Avg request time (ms)'),
              ]
    with open(filename, 'w') as f:
        for key, title in titles:
            f.write("%s,%s\n" % (title, ','.join(rows[key])))


def save_to_csv_gauss_file(results, filename):
    titles = ['ms'] + [str(result['freq']) for result in results]
    max_len = max([result['gauss_len'] for result in results])
    data = [[] for _ in xrange(max_len)]
    for result in results:
        for i, row in enumerate(data):
            if i < result['gauss_len']:
                row.append(str(result['gauss'][i]))
            else:
                row.append('0')
    with open(filename, 'w') as f:
        f.write("%s\n" % ','.join(titles))
        for i, row in enumerate(data):
            f.write("%s,%s\n" % (i, ','.join(row)))


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
    parser.add_option('--duration', type=float, default=60,
                      help='Benchmark duration.')
    parser.add_option('--freq', type=int, action='append', default=[],
                      help='Amount of request by second. Can be used multiple times.')
    parser.add_option('--sleep', type=int, default=10,
                      help='Sleep time in seconds between benchmarks.')
    parser.add_option('--service', type=str, action='append', default=[],
                      help='Quijote service. Can be used multiple times for round robin.')
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
    csv_log_file = ENVS[options.env]['csv_log_file'] % time_id
    csv_gauss_file = ENVS[options.env]['csv_gauss_file'] % time_id

    # Show benchmark settings before run benchmarks
    lines = ['Benchmark settings']
    for k in ('env', 'duration', 'service', 'freq', 'sleep'):
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
        results.append(result)
        if f != options.freq[-1]:
            time.sleep(options.sleep)

    save_to_csv_log_file(results, csv_log_file)
    save_to_csv_gauss_file(results, csv_gauss_file)

    print "Done!"
    print "Generated files:"
    print "\t", log_file
    print "\t", csv_log_file
    print "\t", csv_gauss_file
