import unittest
import threading
from funkload.FunkLoadTestCase import FunkLoadTestCase
from itertools import cycle
import ids


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


class DynamicData(FunkLoadTestCase):

    def setUp(self):
        url = self.conf_get('main', 'url')
        tpl_url = url + '/v0.1/site/1/item/%s/dynamic_data'
        self.url = tpl_url % item_ids.next()

    def test_get(self):
        response = self.get(self.url, description=self.url)
        self.assertTrue(response.code == 200)


if __name__ == '__main__':
    unittest.main()
