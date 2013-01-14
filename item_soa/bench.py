import sys
import random
import time
import requests
import threading
from Queue import Queue, Empty


ENVS = {
    'dev': {
        'ids_module': 'ids_dev',
        'log_file': 'result_dev.txt',
        'url_base': 'http://dev-models.olx.com.ar:8000/quijote',
    },
    'dev-cluster': {
        'ids_module': 'ids_dev',
        'log_file': 'result_dev_cluster_%s.txt',
        'url_base': 'http://dev-models.olx.com.ar:%s/quijote-cluster',
    },
    'qa1': {
        'ids_module': 'ids_qa1',
        'log_file': 'result_qa1.txt',
        'url_base': 'http://models-quijote-qa1.olx.com.ar',
    },
    'qa2': {
        'ids_module': 'ids_qa2',
        'log_file': 'result_qa2.txt',
        'url_base': 'http://models-quijote-qa2.olx.com.ar',
    },
    'live': {
        'ids_module': 'ids_live',
        'log_file': 'result_live.txt',
        'url_base': 'http://204.232.252.178',
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


def fetch(url, counter):
    sys.stdout.write('.')
    sys.stdout.flush()
    begin_time = time.time()
    response = requests.get(url)
    delta = (time.time() - begin_time) * 1000  # ms
    counter.count_item(delta)
    return response


def fetch_subresource(name, url, counter):
    response = fetch(url, counter)
    if response.status_code == 200 and name in ('category', 'location', 'seo'):
        data = response.json()
        if name in ('category', 'location'):
            url_parent = data['response']['resources']['parent']
            if url_parent:
                fetch_subresource(name, url_parent, counter)
        if name == 'seo':
            for direction in ('next', 'prev'):
                url_seo = data['response']['resources'][direction]
                if url_seo:
                    fetch(url_seo, counter)


def worker(i, queue, counters):
    item_counter, reqs_counter = counters
    url_base = ENVS[env]['url_base'] + '/v1/site/1/item/%s'

    sys.stdout.write("Worker %s started\n" % i)
    sys.stdout.flush()
    time.sleep(2)

    while True:
        try:
            id = queue.get_nowait()
        except Empty:
            break
        url = url_base % id
        item_reqs_counter = Counter('Item Request')
        response = fetch(url, item_reqs_counter)
        if response.status_code == 200:
            data = response.json()
            for name, url in data['response']['resources'].items():
                if url:
                    fetch_subresource(name, url, item_reqs_counter)
        reqs_counter.count_counter(item_reqs_counter)
        item_counter.count_item(item_reqs_counter.total_time)


def print_stats(counters, workers):
    final_item_counter = Counter('Item Pages')
    final_item_counter.workers = workers
    final_reqs_counter = Counter('Total Requests')
    final_reqs_counter.workers = workers

    for c in counters:
        final_item_counter.count_counter(c[0])
        final_reqs_counter.count_counter(c[1])

    print
    print final_item_counter
    print final_reqs_counter


if __name__ == '__main__':
    try:
        env = sys.argv[1]
        workers = int(sys.argv[2])
        limit = int(sys.argv[3])
    except:
        print "Usage %s ENV WORKERS ITEM_PAGES" % sys.argv[0]
        sys.exit(1)

    ids = __import__(ENVS[env]['ids_module'])
    random.shuffle(ids.ids)

    queue = Queue()
    for id in ids.ids[:limit]:
        queue.put(id)

    # Start threads
    jobs = []
    counters = []
    for i in range(workers):
        c = ([Counter('Item Pages'), Counter('Total Requests')])
        t = threading.Thread(target=worker, args=(i, queue, c))
        jobs.append(t)
        counters.append(c)
        t.start()

    # End threads
    for t in jobs:
        t.join()

    print_stats(counters, workers)

