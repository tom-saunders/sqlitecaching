import logging

from sqlitecaching.exceptions import (
    SqliteCachingDuplicateCategoryException,
    SqliteCachingDuplicateCauseException,
    SqliteCachingException,
    SqliteCachingMissingCategoryException,
    SqliteCachingMissingCauseException,
)
from sqlitecaching.test import SqliteCachingTestBase, TestLevel, test_level

log = logging.getLogger(__name__)


@test_level(TestLevel.PRE_COMMIT)
class TestSqliteCachingException(SqliteCachingTestBase):
    __TEST_TYPE = 888
    __TEST_MISSING_TYPE = 878
    __TEST_CAUSE = 888
    __TEST_MISSING_CAUSE = 878

    __TestCategoryException = SqliteCachingException.register_category(
        category_name="TestCategoryException", category_id=__TEST_TYPE
    )
    __TestCauseException = __TestCategoryException.register_cause(
        cause_name="TestCauseException", cause_id=__TEST_CAUSE, fmt="", params=[]
    )

    def test_duplicate_category(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            DuplicateTestCategoryException = (  # noqa: F841
                SqliteCachingException.register_category(
                    category_name="DuplicateTestCategoryException",
                    category_id=self.__TEST_TYPE,
                )
            )
        actual = raised_context.exception
        self.assertEqual(
            actual.category_id, SqliteCachingDuplicateCategoryException._category_id
        )
        self.assertEqual(
            actual.cause_id, SqliteCachingDuplicateCategoryException._cause_id
        )

    def test_duplicate_cause(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            DuplicateTestCauseException = (  # noqa: F841
                self.__TestCauseException.register_cause(
                    cause_name="DuplicateTestCauseException",
                    cause_id=self.__TEST_CAUSE,
                    fmt="",
                    params=[],
                )
            )
        actual = raised_context.exception
        self.assertEqual(
            actual.category_id, SqliteCachingDuplicateCauseException._category_id
        )
        self.assertEqual(
            actual.cause_id, SqliteCachingDuplicateCauseException._cause_id
        )

    def test_missing_category(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            _ = SqliteCachingException(
                category_id=self.__TEST_MISSING_TYPE,
                cause_id=self.__TEST_CAUSE,
                params={},
                stacklevel=1,
            )
        actual = raised_context.exception
        self.assertEqual(
            actual.category_id, SqliteCachingMissingCategoryException._category_id
        )
        self.assertEqual(
            actual.cause_id, SqliteCachingMissingCategoryException._cause_id
        )

    def test_missing_cause(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            _ = SqliteCachingException(
                category_id=self.__TEST_TYPE,
                cause_id=self.__TEST_MISSING_CAUSE,
                params={},
                stacklevel=1,
            )
        actual = raised_context.exception
        self.assertEqual(
            actual.category_id, SqliteCachingMissingCauseException._category_id
        )
        self.assertEqual(actual.cause_id, SqliteCachingMissingCauseException._cause_id)
