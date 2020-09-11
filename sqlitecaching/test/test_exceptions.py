import logging

from sqlitecaching.exceptions import (
    SqliteCachingAdditionalParamsException,
    SqliteCachingDuplicateCategoryException,
    SqliteCachingDuplicateCauseException,
    SqliteCachingException,
    SqliteCachingMissingCategoryException,
    SqliteCachingMissingCauseException,
    SqliteCachingMissingParamsException,
    SqliteCachingNoCategoryForCauseException,
)
from sqlitecaching.test import SqliteCachingTestBase, TestLevel, test_level

log = logging.getLogger(__name__)


@test_level(TestLevel.PRE_COMMIT)
class TestSqliteCachingException(SqliteCachingTestBase):
    __TEST_CATEGORY = 888
    __TEST_DELETED_CATEGORY = 886
    __TEST_MISSING_CATEGORY = 777

    __TEST_CAUSE = 888
    __TEST_PARAMS_CAUSE = 887
    __TEST_MISSING_CAUSE = 777

    __TestCategoryException = SqliteCachingException.register_category(
        category_name="TestCategoryException",
        category_id=__TEST_CATEGORY,
    )
    __TestDeletedCategoryException = SqliteCachingException.register_category(
        category_name="TestDeletedCategoryException",
        category_id=__TEST_DELETED_CATEGORY,
    )
    del SqliteCachingException._categories[__TEST_DELETED_CATEGORY]

    __TestCauseException = __TestCategoryException.register_cause(
        cause_name="TestCauseException",
        cause_id=__TEST_CAUSE,
        fmt="",
        params=frozenset(
            [],
        ),
    )
    __TestParamException = __TestCategoryException.register_cause(
        cause_name="TestCauseException",
        cause_id=__TEST_PARAMS_CAUSE,
        fmt="",
        params=frozenset(
            [
                "a",
                "b",
            ],
        ),
    )

    def test_duplicate_category(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            _ = SqliteCachingException.register_category(  # noqa: F841
                category_name="DuplicateTestCategoryException",
                category_id=self.__TEST_CATEGORY,
            )
        actual = raised_context.exception
        self.assertEqual(
            actual.category_id,
            SqliteCachingDuplicateCategoryException._category_id,
        )
        self.assertEqual(
            actual.cause_id,
            SqliteCachingDuplicateCategoryException._cause_id,
        )

    def test_duplicate_cause(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            _ = self.__TestCauseException.register_cause(  # noqa: F841
                cause_name="DuplicateTestCauseException",
                cause_id=self.__TEST_CAUSE,
                fmt="",
                params=[],
            )
        actual = raised_context.exception
        self.assertEqual(
            actual.category_id,
            SqliteCachingDuplicateCauseException._category_id,
        )
        self.assertEqual(
            actual.cause_id,
            SqliteCachingDuplicateCauseException._cause_id,
        )

    def test_missing_category(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            _ = SqliteCachingException(
                category_id=self.__TEST_MISSING_CATEGORY,
                cause_id=self.__TEST_CAUSE,
                params={},
                stacklevel=1,
            )
        actual = raised_context.exception
        self.assertEqual(
            actual.category_id,
            SqliteCachingMissingCategoryException._category_id,
        )
        self.assertEqual(
            actual.cause_id,
            SqliteCachingMissingCategoryException._cause_id,
        )

    def test_missing_cause(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            _ = SqliteCachingException(
                category_id=self.__TEST_CATEGORY,
                cause_id=self.__TEST_MISSING_CAUSE,
                params={},
                stacklevel=1,
            )
        actual = raised_context.exception
        self.assertEqual(
            actual.category_id,
            SqliteCachingMissingCauseException._category_id,
        )
        self.assertEqual(actual.cause_id, SqliteCachingMissingCauseException._cause_id)

    def test_missing_params(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            _ = SqliteCachingException(
                category_id=self.__TEST_CATEGORY,
                cause_id=self.__TEST_PARAMS_CAUSE,
                params={},
                stacklevel=1,
            )
        actual = raised_context.exception
        self.assertEqual(
            actual.category_id,
            SqliteCachingMissingParamsException._category_id,
        )
        self.assertEqual(actual.cause_id, SqliteCachingMissingParamsException._cause_id)

    def test_additional_params(self):
        successful_pre = SqliteCachingException(
            category_id=self.__TEST_CATEGORY,
            cause_id=self.__TEST_CAUSE,
            params={
                "a": "b",
            },
            stacklevel=1,
        )
        self.assertEqual(successful_pre.category_id, self.__TEST_CATEGORY)
        self.assertEqual(successful_pre.cause_id, self.__TEST_CAUSE)
        with self.assertRaises(SqliteCachingException) as raised_context:
            try:
                SqliteCachingException.raise_on_additional_params(True)
                _ = SqliteCachingException(
                    category_id=self.__TEST_CATEGORY,
                    cause_id=self.__TEST_CAUSE,
                    params={
                        "a": "b",
                    },
                    stacklevel=1,
                )
            finally:
                SqliteCachingException.raise_on_additional_params(False)
        actual = raised_context.exception
        self.assertEqual(
            actual.category_id,
            SqliteCachingAdditionalParamsException._category_id,
        )
        self.assertEqual(
            actual.cause_id,
            SqliteCachingAdditionalParamsException._cause_id,
        )
        successful_post = SqliteCachingException(
            category_id=self.__TEST_CATEGORY,
            cause_id=self.__TEST_CAUSE,
            params={
                "a": "b",
            },
            stacklevel=1,
        )
        self.assertEqual(successful_post.category_id, self.__TEST_CATEGORY)
        self.assertEqual(successful_post.cause_id, self.__TEST_CAUSE)

    def test_deleted_category(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            _ = self.__TestDeletedCategoryException.register_cause(
                cause_name="TestDeletedCategoryCauseException",
                cause_id=self.__TEST_CAUSE,
                fmt="",
                params=[],
            )
        actual = raised_context.exception
        self.assertEqual(
            actual.category_id,
            SqliteCachingNoCategoryForCauseException._category_id,
        )
        self.assertEqual(
            actual.cause_id,
            SqliteCachingNoCategoryForCauseException._cause_id,
        )
