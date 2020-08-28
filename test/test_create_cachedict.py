import unittest

from sqlitecaching.dict import CacheDict


class TestCreateCacheDict(unittest.TestCase):
    def test_create_noargs(self):
        c = CacheDict.create_from_connection()
        self.assertNotEqual(c, None)
