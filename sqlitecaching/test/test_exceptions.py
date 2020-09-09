import logging

from sqlitecaching.exceptions import (
    SqliteCachingDuplicateCauseException,
    SqliteCachingDuplicateTypeException,
    SqliteCachingException,
    SqliteCachingMissingCauseException,
    SqliteCachingMissingTypeException,
)
from sqlitecaching.test import SqliteCachingTestBase, TestLevel, test_level

log = logging.getLogger(__name__)


@test_level(TestLevel.PRE_COMMIT)
class TestSqliteCachingException(SqliteCachingTestBase):
    __TEST_TYPE = 888
    __TEST_MISSING_TYPE = 878
    __TEST_CAUSE = 888
    __TEST_MISSING_CAUSE = 878

    __TestTypeException = SqliteCachingException.register_type(
        type_name="TestTypeException", type_id=__TEST_TYPE
    )
    __TestCauseException = __TestTypeException.register_cause(
        cause_name="TestCauseException", cause_id=__TEST_CAUSE, fmt="", req_params=[]
    )

    def test_duplicate_type(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            DuplicateTestTypeException = (  # noqa: F841
                SqliteCachingException.register_type(
                    type_name="DuplicateTestTypeException", type_id=self.__TEST_TYPE
                )
            )
        actual = raised_context.exception
        self.assertEqual(actual.type_id, SqliteCachingDuplicateTypeException._type_id)
        self.assertEqual(actual.cause_id, SqliteCachingDuplicateTypeException._cause_id)

    def test_duplicate_cause(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            DuplicateTestCauseException = (  # noqa: F841
                self.__TestCauseException.register_cause(
                    cause_name="DuplicateTestCauseException",
                    cause_id=self.__TEST_CAUSE,
                    fmt="",
                    req_params=[],
                )
            )
        actual = raised_context.exception
        self.assertEqual(actual.type_id, SqliteCachingDuplicateCauseException._type_id)
        self.assertEqual(
            actual.cause_id, SqliteCachingDuplicateCauseException._cause_id
        )

    def test_missing_type(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            _ = SqliteCachingException(
                type_id=self.__TEST_MISSING_TYPE,
                cause_id=self.__TEST_CAUSE,
                params={},
                stacklevel=1,
            )
        actual = raised_context.exception
        self.assertEqual(actual.type_id, SqliteCachingMissingTypeException._type_id)
        self.assertEqual(actual.cause_id, SqliteCachingMissingTypeException._cause_id)

    def test_missing_cause(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            _ = SqliteCachingException(
                type_id=self.__TEST_TYPE,
                cause_id=self.__TEST_MISSING_CAUSE,
                params={},
                stacklevel=1,
            )
        actual = raised_context.exception
        self.assertEqual(actual.type_id, SqliteCachingMissingCauseException._type_id)
        self.assertEqual(actual.cause_id, SqliteCachingMissingCauseException._cause_id)
