import json
import unittest
import threading
from funkload.FunkLoadTestCase import FunkLoadTestCase
from itertools import cycle

import httplib
from urlparse import urlparse

import ids


OK_CODES = (200, 404)


class ThreadSafeIter:

    def __init__(self, iterator):
        self.iterator = iterator
        self.lock = threading.Lock()

    def __iter__(self):
        return self

    def next(self):
        with self.lock:
            return self.iterator.next()


class Response:
    def __init__(self, code, body):
        self.code = code
        self.body = body


class KeepAliveClient:

    def __init__(self):
        self._connections = {}

    def get(self, url, description=None, ok_codes=(200,)):
        url = urlparse(url)
        conn = self.get_connection(url.hostname, url.port)
        conn.request('GET', url.geturl())
        response = conn.getresponse()
        if response.status not in ok_codes:
            raise Exception('%s not in ok_codes' % response.status)
        return Response(response.status, response.read())

    def get_connection(self, host, port):
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


item_ids = ThreadSafeIter(cycle(ids.ids))


class ItemSoa(FunkLoadTestCase):

    def setUp(self):
        self.tpl_url = self.conf_get('main', 'url') + '/v1/site/1/item/%s'
        self.conn = KeepAliveClient()

    def _get_url_item(self):
        return self.tpl_url % item_ids.next()

    def test_get(self):
        url_item = self._get_url_item()
        response = self.conn.get(url_item, description="Item")
        data = json.loads(response.body)
        for name, url in data['response']['resources'].items():
            self._get_subresource(name, url)

    def _get_subresource(self, name, url):
        description = "Subresource %s" % name
        response = self.conn.get(url, description=description, ok_codes=OK_CODES)
        if response.code == 200 and name in ('category', 'location', 'seo'):
            data = json.loads(response.body)
            if name in ('category', 'location'):
                url_parent = data['response']['resources']['parent']
                if url_parent:
                    self._get_subresource(name, url_parent)
            if name == 'seo':
                for direction in ('next', 'prev'):
                    url_seo = data['response']['resources'][direction]
                    if url_seo:
                        description = "Subresource seo %s" % direction
                        self.conn.get(url_seo, description=description,
                                      ok_codes=OK_CODES)


if __name__ == '__main__':
    unittest.main()
