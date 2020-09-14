import logging

# from sqlitecaching.config import Config, UTCFormatter
from sqlitecaching.test import SqliteCachingTestBase, TestLevel, test_level

log = logging.getLogger(__name__)


@test_level(TestLevel.PRE_COMMIT)
class TestSqliteCachingConfig(SqliteCachingTestBase):
    pass
