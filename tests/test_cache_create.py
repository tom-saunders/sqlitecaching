import logging

from sqlitecaching.dict import CacheDict
from tests import CacheDictTestBase, TestLevel, test_level

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


@test_level(TestLevel.PRE_COMMIT)
class TestCacheDictCreation(CacheDictTestBase):
    def test_create_from_connection_noargs(self):
        c = CacheDict.open_anon_memory()
        c = CacheDict.open_anon_disk()
        c = CacheDict.open_readonly(path=f"{self.res_dir}/readonly.empty.sqlite")
        c = CacheDict.open_readwrite(path=f"{self.res_dir}/readwrite.empty.sqlite")
        c = CacheDict.open_readwrite(path="tmpfile.sqlite", create=True)
        c = CacheDict.create_from_conn(conn=c.conn)
        self.assertNotEqual(c, None)


# @test_level(TestLevel.FULL)
# class TestCreateCacheDic2(unittest.TestCase):
#     log.warning("B")
#
#     def test_create_noargs(self):
#         log.warning("b")
#         c = CacheDict.create_from_connection()
#         self.assertNotEqual(c, None)
#
#
# class TestCreateCacheDic3(unittest.TestCase):
#     log.warning("C")
#
#     @test_level(TestLevel.PRE_COMMIT)
#     def test_create_noargs(self):
#         log.warning("c")
#         c = CacheDict.create_from_connection()
#         self.assertNotEqual(c, None)
#
#
# class TestCreateCacheDic4(unittest.TestCase):
#     log.warning("D")
#
#     @test_level(TestLevel.FULL)
#     def test_create_noargs(self):
#         logr.warning("d")
#         c = CacheDict.create_from_connection()
#         self.assertNotEqual(c, None)
