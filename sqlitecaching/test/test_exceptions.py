import logging

from sqlitecaching.exceptions import (
    CategoryID,
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
    TEST_CATEGORY = 888
    TEST_DELETED_CATEGORY = 886
    TEST_MISSING_CATEGORY = 777

    TEST_CAUSE = 888
    TEST_PARAMS_CAUSE = 887
    TEST_MISSING_CAUSE = 777

    TestCategory = SqliteCachingException.register_category(
        category_name="TestCategory",
        category_id=TEST_CATEGORY,
    )
    TestDeletedCategory = SqliteCachingException.register_category(
        category_name="TestDeletedCategory",
        category_id=TEST_DELETED_CATEGORY,
    )
    del SqliteCachingException._categories[CategoryID(TEST_DELETED_CATEGORY)]

    TestCauseException = TestCategory.register_cause(
        cause_name="TestCauseException",
        cause_id=TEST_CAUSE,
        fmt="",
        params=frozenset(
            [],
        ),
    )
    TestParamException = TestCategory.register_cause(
        cause_name="TestCauseException",
        cause_id=TEST_PARAMS_CAUSE,
        fmt="",
        params=frozenset(
            [
                "a",
                "b",
            ],
        ),
    )

    def test_successful_create(self):
        successful = self.TestCauseException({})
        self.assertEqual(
            successful.category.id,
            self.TestCauseException.category_id,
            successful.msg,
        )
        self.assertEqual(
            successful.cause.id,
            self.TestCauseException.id,
            successful.msg,
        )

    def test_duplicate_category(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            _ = SqliteCachingException.register_category(  # noqa: F841
                category_name="DuplicateTestCategory",
                category_id=self.TEST_CATEGORY,
            )
        actual = raised_context.exception
        self.assertEqual(
            actual.category.id,
            SqliteCachingDuplicateCategoryException.category_id,
            actual.msg,
        )
        self.assertEqual(
            actual.cause.id,
            SqliteCachingDuplicateCategoryException.id,
            actual.msg,
        )

    def test_duplicate_cause(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            _ = self.TestCategory.register_cause(  # noqa: F841
                cause_name="DuplicateTestCauseException",
                cause_id=self.TEST_CAUSE,
                fmt="",
                params=[],
            )
        actual = raised_context.exception
        self.assertEqual(
            actual.category.id,
            SqliteCachingDuplicateCauseException.category_id,
            actual.msg,
        )
        self.assertEqual(
            actual.cause.id,
            SqliteCachingDuplicateCauseException.id,
            actual.msg,
        )

    def test_missing_category(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            _ = SqliteCachingException(
                category_id=self.TEST_MISSING_CATEGORY,
                cause_id=self.TEST_CAUSE,
                params={},
                stacklevel=1,
            )
        actual = raised_context.exception
        self.assertEqual(
            actual.category.id,
            SqliteCachingMissingCategoryException.category_id,
            actual.msg,
        )
        self.assertEqual(
            actual.cause.id,
            SqliteCachingMissingCategoryException.id,
            actual.msg,
        )

    def test_missing_cause(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            _ = SqliteCachingException(
                category_id=self.TEST_CATEGORY,
                cause_id=self.TEST_MISSING_CAUSE,
                params={},
                stacklevel=1,
            )
        actual = raised_context.exception
        self.assertEqual(
            actual.category.id,
            SqliteCachingMissingCauseException.category_id,
            actual.msg,
        )
        self.assertEqual(
            actual.cause.id,
            SqliteCachingMissingCauseException.id,
            actual.msg,
        )

    def test_missing_params(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            _ = SqliteCachingException(
                category_id=self.TEST_CATEGORY,
                cause_id=self.TEST_PARAMS_CAUSE,
                params={},
                stacklevel=1,
            )
        actual = raised_context.exception
        self.assertEqual(
            actual.category.id,
            SqliteCachingMissingParamsException.category_id,
            actual.msg,
        )
        self.assertEqual(
            actual.cause.id,
            SqliteCachingMissingParamsException.id,
            actual.msg,
        )

    def test_missing_and_additional_params(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            try:
                SqliteCachingException.raise_on_additional_params(True)
                _ = SqliteCachingException(
                    category_id=self.TEST_CATEGORY,
                    cause_id=self.TEST_PARAMS_CAUSE,
                    params={"x": "x"},
                    stacklevel=1,
                )
            finally:
                SqliteCachingException.raise_on_additional_params(False)
        actual = raised_context.exception
        self.assertEqual(
            actual.category.id,
            SqliteCachingMissingParamsException.category_id,
            actual.msg,
        )
        self.assertEqual(
            actual.cause.id,
            SqliteCachingMissingParamsException.id,
            actual.msg,
        )

    def test_additional_params(self):
        successful_pre = SqliteCachingException(
            category_id=self.TEST_CATEGORY,
            cause_id=self.TEST_CAUSE,
            params={
                "a": "b",
            },
            stacklevel=1,
        )
        self.assertEqual(
            successful_pre.category.id,
            self.TEST_CATEGORY,
            successful_pre.msg,
        )
        self.assertEqual(
            successful_pre.cause.id,
            self.TEST_CAUSE,
            successful_pre.msg,
        )
        with self.assertRaises(SqliteCachingException) as raised_context:
            try:
                SqliteCachingException.raise_on_additional_params(True)
                _ = SqliteCachingException(
                    category_id=self.TEST_CATEGORY,
                    cause_id=self.TEST_CAUSE,
                    params={
                        "a": "b",
                    },
                    stacklevel=1,
                )
            finally:
                SqliteCachingException.raise_on_additional_params(False)
        actual = raised_context.exception
        self.assertEqual(
            actual.category.id,
            SqliteCachingAdditionalParamsException.category_id,
            actual.msg,
        )
        self.assertEqual(
            actual.cause.id,
            SqliteCachingAdditionalParamsException.id,
            actual.msg,
        )
        successful_post = SqliteCachingException(
            category_id=self.TEST_CATEGORY,
            cause_id=self.TEST_CAUSE,
            params={
                "a": "b",
            },
            stacklevel=1,
        )
        self.assertEqual(
            successful_post.category.id,
            self.TEST_CATEGORY,
            successful_post.msg,
        )
        self.assertEqual(
            successful_post.cause.id,
            self.TEST_CAUSE,
            successful_post.msg,
        )

    def test_deleted_category(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            _ = self.TestDeletedCategory.register_cause(
                cause_name="TestDeletedCategoryCauseException",
                cause_id=self.TEST_CAUSE,
                fmt="",
                params=[],
            )
        actual = raised_context.exception
        self.assertEqual(
            actual.category.id,
            SqliteCachingNoCategoryForCauseException.category_id,
            actual.msg,
        )
        self.assertEqual(
            actual.cause.id,
            SqliteCachingNoCategoryForCauseException.id,
            actual.msg,
        )
