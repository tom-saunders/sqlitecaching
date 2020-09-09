import logging
import tempfile

from sqlitecaching.dict import CacheDict
from sqlitecaching.test import SqliteCachingTestBase, TestLevel, test_level

log = logging.getLogger(__name__)


@test_level(TestLevel.PRE_COMMIT)
class TestCacheDictCreation(SqliteCachingTestBase):
    def test_open_anon_memory(self):
        c = CacheDict.open_anon_memory()
        self.assertNotEqual(c, None)

    def test_open_anon_disk(self):
        c = CacheDict.open_anon_disk()
        self.assertNotEqual(c, None)

    def test_open_readonly(self):
        c = CacheDict.open_readonly(path=f"{self.res_dir}/readonly.empty.sqlite")
        self.assertNotEqual(c, None)

    def test_open_readwrite(self):
        c = CacheDict.open_readwrite(path=f"{self.res_dir}/readwrite.empty.sqlite")
        self.assertNotEqual(c, None)

    def test_open_readwrite_create(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            c = CacheDict.open_readwrite(path=f"{tmpdir}/tmpfile.sqlite", create=True)
            self.assertNotEqual(c, None)

    def test_create_from_connection_noargs(self):
        c = CacheDict._create_from_conn(conn=None)
        self.assertNotEqual(c, None)
