import unittest

from sqlitecaching.dict import CacheDict
from tests import TestLevel, test_level


@test_level(TestLevel.FULL)
class TestCreateCacheDict(unittest.TestCase):
    def test_create_noargs(self):
        c = CacheDict.create_from_connection()
        self.assertNotEqual(c, None)


@test_level(TestLevel.PRE_COMMIT)
class TestCreateCacheDic2(unittest.TestCase):
    def test_create_noargs(self):
        c = CacheDict.create_from_connection()
        self.assertNotEqual(c, None)


class TestCreateCacheDic3(unittest.TestCase):
    @test_level(TestLevel.PRE_COMMIT)
    def test_create_noargs(self):
        c = CacheDict.create_from_connection()
        self.assertNotEqual(c, None)


class TestCreateCacheDic4(unittest.TestCase):
    @test_level(TestLevel.FULL)
    def test_create_noargs(self):
        c = CacheDict.create_from_connection()
        self.assertNotEqual(c, None)
