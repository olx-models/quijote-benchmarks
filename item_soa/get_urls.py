import sys
import time
import requests


ENVS = {
    'dev': {
        'ids_module': 'ids_dev',
        'log_file': 'result_dev.txt',
        'url_base': 'http://dev-models.olx.com.ar:8000/quijote',
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
        self.name = name
        self.total_items = 0
        self.last_time = 0
        self.total_time = 0
        self.max_time = 0
        self.min_time = 99999999999999999

    def count(self, t):
        self.total_items += 1
        self.last_time = t
        self.total_time += t
        if t > self.max_time:
            self.max_time = t
        if t < self.min_time:
            self.min_time = t

    def __str__(self):
        if self.total_items > 0:
            avg = self.total_time / float(self.total_items)
        else:
            avg = 0
        msg_format = '\n%s: Count: %s - Avg %0.2f ms - Max %0.2f ms - Min %0.2f ms\n'
        return msg_format % (self.name, self.total_items, avg, self.max_time, self.min_time)


def fetch(url, counter):
    begin_time = time.time()
    response = requests.get(url)
    delta = (time.time() - begin_time) * 1000  # ms
    counter.count(delta)
    logfile.write('%s (%0.2f ms)\n' % (url, delta))
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


if __name__ == '__main__':
    env = sys.argv[1]
    limit = int(sys.argv[2])

    ids = __import__(ENVS[env]['ids_module'])
    logfile = open(ENVS[env]['log_file'], 'w')

    item_counter = Counter('Item Pages')
    reqs_counter = Counter('Requests')

    for i, id in enumerate(ids.ids[:limit]):
        url = ENVS[env]['url_base'] + '/v1/site/1/item/' + str(id)

        begin_time = time.time()
        response = fetch(url, reqs_counter)
        if response.status_code == 200:
            data = response.json()
            for name, url in data['response']['resources'].items():
                if url:
                    fetch_subresource(name, url, reqs_counter)
        delta = (time.time() - begin_time) * 1000  # ms
        item_counter.count(delta)

        logfile.write('%0.2f ms\n' % item_counter.last_time)

    logfile.write(str(item_counter))
    logfile.write(str(reqs_counter))
