import unittest

from sqlitecaching.dict import CacheDict


class TestCreateCacheDict(unittest.TestSuite):
    def test_create_noargs(self):
        c = CacheDict()
        self.assertEqual(c, None)
