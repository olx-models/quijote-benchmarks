import time
import requests
import ids


URL_BASE = 'http://dev-models.olx.com.ar:8000/quijote'

class Counter:
    total_requests = 0
    total_itempages = 0
    total_time = 0
    actual_time = 0
    actual_requests = 0
    max_time = 0
    min_time = 0
    max_requests = 0
    min_requests = 0

def fetch(url):
    begin_time = time.time()
    response = requests.get(url)
    delta = (time.time() - begin_time) * 1000  # ms

    Counter.total_requests += 1
    Counter.actual_requests += 1
    Counter.actual_time += delta
    Counter.total_time += delta

    logfile.write('%s (%0.2f ms)\n' % (url, delta))
    return response

def fetch_subresource(name, url):
    response = fetch(url)
    if response.status_code == 200 and name in ('category', 'location', 'seo'):
        data = response.json()
        if name in ('category', 'location'):
            url_parent = data['response']['resources']['parent']
            if url_parent:
                fetch_subresource(name, url_parent)
        if name == 'seo':
            for direction in ('next', 'prev'):
                url_seo = data['response']['resources'][direction]
                if url_seo:
                    fetch(url_seo)

if __name__ == '__main__':
    logfile = open('urls.txt', 'w')

    for i, id in enumerate(ids.ids[:500]):
        Counter.total_itempages += 1
        Counter.actual_time = 0
        Counter.actual_requests = 0
        url = URL_BASE + '/v1/site/1/item/' + str(id)
        response = fetch(url)
        if response.status_code == 200:
            data = response.json()
            for name, url in data['response']['resources'].items():
                if url:
                    fetch_subresource(name, url)

        if Counter.min_time == 0 or Counter.actual_time < Counter.min_time:
            Counter.min_time = Counter.actual_time
        if Counter.actual_time > Counter.max_time:
            Counter.max_time = Counter.actual_time
        if Counter.min_requests == 0 or Counter.actual_requests < Counter.min_requests:
            Counter.min_requests = Counter.actual_requests
        if Counter.actual_requests > Counter.max_requests:
            Counter.max_requests = Counter.actual_requests

        logfile.write('%0.2f ms\n' % Counter.actual_time)
    

    avg = Counter.total_time / Counter.total_itempages
    msg_format = '\n%s Item pages - Avg %0.2f ms - Max %0.2f ms - Min %0.2f ms\n'
    logfile.write(msg_format % (Counter.total_itempages, avg,
                                Counter.max_time, Counter.min_time))

    avg = Counter.total_requests / float(Counter.total_itempages)
    msg_format = '%s Reqs - Avg %0.2f - Max %0.2f - Min %0.2f\n'
    logfile.write(msg_format % (Counter.total_requests, avg,
                                Counter.max_requests, Counter.min_requests))


