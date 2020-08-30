import unittest

import tests
from sqlitecaching.dict import CacheDict
from tests import TestLevel, test_level

logger = tests.config.get_sub_logger("cache.creation")


@test_level(TestLevel.PRE_COMMIT)
class TestCacheDictCreation(unittest.TestCase):
    def test_create_from_connection_noargs(self):
        c = CacheDict.create_from_connection()
        self.assertNotEqual(c, None)


# @test_level(TestLevel.FULL)
# class TestCreateCacheDic2(unittest.TestCase):
#     logger.warning("B")
#
#     def test_create_noargs(self):
#         logger.warning("b")
#         c = CacheDict.create_from_connection()
#         self.assertNotEqual(c, None)
#
#
# class TestCreateCacheDic3(unittest.TestCase):
#     logger.warning("C")
#
#     @test_level(TestLevel.PRE_COMMIT)
#     def test_create_noargs(self):
#         logger.warning("c")
#         c = CacheDict.create_from_connection()
#         self.assertNotEqual(c, None)
#
#
# class TestCreateCacheDic4(unittest.TestCase):
#     logger.warning("D")
#
#     @test_level(TestLevel.FULL)
#     def test_create_noargs(self):
#         logger.warning("d")
#         c = CacheDict.create_from_connection()
#         self.assertNotEqual(c, None)
