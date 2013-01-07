import json
import unittest
import threading
from funkload.FunkLoadTestCase import FunkLoadTestCase
from itertools import cycle
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


item_ids = ThreadSafeIter(cycle(ids.ids))


class ItemSoa(FunkLoadTestCase):

    def setUp(self):
        self.tpl_url = self.conf_get('main', 'url') + '/v1/site/1/item/%s'

    def _get_url_item(self):
        return self.tpl_url % item_ids.next()

    def test_get(self):
        url_item = self._get_url_item()
        response = self.get(url_item, description="Item")
        data = json.loads(response.body)
        for name, url in data['response']['resources'].items():
            self._get_subresource(name, url)

    def _get_subresource(self, name, url):
        description = "Subresource %s" % name
        response = self.get(url, description=description, ok_codes=OK_CODES)
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
                        self.get(url_seo, description=description,
                                 ok_codes=OK_CODES)


if __name__ == '__main__':
    unittest.main()
