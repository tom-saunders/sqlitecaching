import logging
import shutil
import tempfile

from sqlitecaching.dict import CacheDict, ToCreate
from sqlitecaching.test import SqliteCachingTestBase, TestLevel, test_level

log = logging.getLogger(__name__)


@test_level(TestLevel.PRE_COMMIT)
class TestCacheDictCreation(SqliteCachingTestBase):
    tmp_dir: int

    def setUp(self):
        self.tmp_dir = tempfile.TemporaryDirectory()
        shutil.copytree(
            f"{self.res_dir}/dicts/",
            f"{self.tmp_dir}/",
            dirs_exist_ok=True,
        )

    def tearDown(self):
        self.tmp_dir.cleanup()

    def test_open_anon_memory(self):
        c = CacheDict.open_anon_memory(mapping=None)
        self.assertNotEqual(c, None)

    def test_open_anon_disk(self):
        c = CacheDict.open_anon_disk(mapping=None)
        self.assertNotEqual(c, None)

    def test_open_readonly(self):
        c = CacheDict.open_readonly(
            path=f"{self.tmp_dir}/readonly.empty.sqlite",
            mapping=None,
        )
        self.assertNotEqual(c, None)

    def test_open_readwrite(self):
        c = CacheDict.open_readwrite(
            path=f"{self.tmp_dir}/readwrite.empty.sqlite",
            mapping=None,
        )
        self.assertNotEqual(c, None)

    def test_open_readwrite_create(self):
        c = CacheDict.open_readwrite(
            path=f"{self.tmp_dir}/tmpfile.sqlite",
            mapping=None,
            create=ToCreate.DATABASE,
        )
        self.assertNotEqual(c, None)

    def test_create_from_connection_noargs(self):
        c = CacheDict._create_from_conn(
            conn=None,
            mapping=None,
        )
        self.assertNotEqual(c, None)
