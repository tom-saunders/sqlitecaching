import itertools
import logging
import typing

import parameterized

from sqlitecaching.dict.mapping import (
    CacheDictMapping,
    CacheDictMappingDuplicateKeysException,
    CacheDictMappingDuplicateValuesException,
    CacheDictMappingInvalidIdentifierException,
    CacheDictMappingInvalidSQLTypeException,
    CacheDictMappingKeyValOverlapException,
    CacheDictMappingMissingKeysException,
    CacheDictMappingNoIdentifierProvidedException,
    CacheDictMappingReservedTableException,
)
from sqlitecaching.exceptions import ParamMap, SqliteCachingException
from sqlitecaching.test import SqliteCachingTestBase, TestLevel, test_level

log = logging.getLogger(__name__)


class In(typing.NamedTuple):
    table: typing.Optional[str]
    keys: typing.Mapping[typing.Optional[str], typing.Optional[str]]
    values: typing.Mapping[typing.Optional[str], typing.Optional[str]]


class Def(typing.NamedTuple):
    name: str
    mapping: In
    expected: typing.Any
    meta: typing.Optional[typing.Any] = None


class FailRes(typing.NamedTuple):
    name: str
    exception: typing.Callable[[ParamMap], SqliteCachingException]


class InputDef(typing.NamedTuple):
    result: str
    mapping: In


class FailInputDef(typing.NamedTuple):
    result: FailRes
    mapping: In


@test_level(TestLevel.PRE_COMMIT)
class TestCacheDictMapping(SqliteCachingTestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.res_dir += "mappings/"

    statement_types = [
        "create_statement",
        "clear_statement",
        "delete_statement",
        "upsert_statement",
        "remove_statement",
        "length_statement",
        "keys_statement",
        "items_statement",
        "values_statement",
    ]

    success_mapping_definitions = [
        InputDef(
            result="aA__to__",
            mapping=In(
                table="aa",
                keys={"a": "A"},
                values={},
            ),
        ),
        InputDef(
            result="aA__to__bB",
            mapping=In(
                table="aa__bb",
                keys={"a": "A"},
                values={"b": "B"},
            ),
        ),
        InputDef(
            result="aA_bB__to__",
            mapping=In(
                table="aa_bb",
                keys={"a": "A", "b": "B"},
                values={},
            ),
        ),
        InputDef(
            result="aA_bB__to__",
            mapping=In(
                table="aA_bB",
                keys={"a": "a", "b": "b"},
                values={},
            ),
        ),
        InputDef(
            result="aA_bB__to__cC",
            mapping=In(
                table="aa_bb__cc",
                keys={"a": "a", "b": "b"},
                values={"c": "C"},
            ),
        ),
        InputDef(
            result="aA_bB__to__cC_dD",
            mapping=In(
                table="aa_bb__cc_dd",
                keys={"a": "a", "b": "b"},
                values={"c": "C", "d": "D"},
            ),
        ),
        InputDef(
            result="aA_bB__to__cC_dD",
            mapping=In(
                table="aa_bb__cc_dd",
                keys={"b": "B", "a": "A"},
                values={"d": "D", "c": "C"},
            ),
        ),
        InputDef(
            result="a___to__cC",
            mapping=In(
                table="a___cc",
                keys={"a": ""},
                values={"c": "C"},
            ),
        ),
        InputDef(
            result="a___to__cC",
            mapping=In(
                table="a___cc",
                keys={"a": " "},
                values={"c": "C"},
            ),
        ),
        InputDef(
            result="a___to__cC",
            mapping=In(
                table="a___cc",
                keys={"a": None},
                values={"c": "C"},
            ),
        ),
        InputDef(
            result="aA__to__c_",
            mapping=In(
                table="aa__c_",
                keys={"a": "A"},
                values={"c": ""},
            ),
        ),
        InputDef(
            result="aA__to__c_",
            mapping=In(
                table="aa__c_",
                keys={"a": "A"},
                values={"c": " "},
            ),
        ),
        InputDef(
            result="aA__to__c_",
            mapping=In(
                table="aa__c_",
                keys={"a": "A"},
                values={"c": None},
            ),
        ),
    ]

    fail_mapping_definitions = [
        FailInputDef(
            result=FailRes(
                name="blank_table_name",
                exception=CacheDictMappingNoIdentifierProvidedException,
            ),
            mapping=In(
                table="",
                keys={"a": "A"},
                values={"b": "B"},
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="none_table_name",
                exception=CacheDictMappingNoIdentifierProvidedException,
            ),
            mapping=In(
                table=None,
                keys={"a": "A"},
                values={"b": "B"},
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="space_table_name",
                exception=CacheDictMappingInvalidIdentifierException,
            ),
            mapping=In(
                table=" ",
                keys={"a": "A"},
                values={"b": "B"},
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="invalid_table_name",
                exception=CacheDictMappingInvalidIdentifierException,
            ),
            mapping=In(
                table="x.y",
                keys={"a": "A"},
                values={"b": "B"},
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="reserved_table_name",
                exception=CacheDictMappingReservedTableException,
            ),
            mapping=In(
                table="sqlite_a",
                keys={"a": "A"},
                values={"b": "B"},
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="missing_keys",
                exception=CacheDictMappingMissingKeysException,
            ),
            mapping=In(
                table="__bB",
                keys={},
                values={"b": "B"},
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="overlapping_key_ands_values",
                exception=CacheDictMappingKeyValOverlapException,
            ),
            mapping=In(
                table="aA__aA_bB",
                keys={"a": "A"},
                values={"a": "A", "b": "B"},
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="blank_key_name",
                exception=CacheDictMappingNoIdentifierProvidedException,
            ),
            mapping=In(
                table="A_bB",
                keys={"": "A"},
                values={"b": "B"},
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="none_key_name",
                exception=CacheDictMappingNoIdentifierProvidedException,
            ),
            mapping=In(
                table="A_bB",
                keys={None: "A"},
                values={"b": "B"},
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="space_key_name",
                exception=CacheDictMappingInvalidIdentifierException,
            ),
            mapping=In(
                table="A_bB",
                keys={" ": "A"},
                values={"b": "B"},
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="invalid_key_name",
                exception=CacheDictMappingInvalidIdentifierException,
            ),
            mapping=In(
                table="A_bB",
                keys={"x.y": "A"},
                values={"b": "B"},
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="duplicated_key_name",
                exception=CacheDictMappingDuplicateKeysException,
            ),
            mapping=In(
                table="aA_AA__bB",
                keys={"a": "A", "A": "A"},
                values={"b": "B"},
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="duplicated_key_name2",
                exception=CacheDictMappingDuplicateKeysException,
            ),
            mapping=In(
                table="aA_a_A__bB",
                keys={"a": "A", "a ": "A"},
                values={"b": "B"},
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="duplicated_value_name",
                exception=CacheDictMappingDuplicateValuesException,
            ),
            mapping=In(
                table="aA_AA__bB_BB",
                keys={"a": "A"},
                values={"b": "B", "B": "B"},
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="duplicated_value_name2",
                exception=CacheDictMappingDuplicateValuesException,
            ),
            mapping=In(
                table="aA_AA__bB_b_B",
                keys={"a": "A"},
                values={"b": "B", "b ": "B"},
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="invalid_key_sqltype",
                exception=CacheDictMappingInvalidSQLTypeException,
            ),
            mapping=In(
                table="aA_B__bB",
                keys={"a": "A.B"},
                values={"b": "B"},
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="invalid_value_sqltype",
                exception=CacheDictMappingInvalidSQLTypeException,
            ),
            mapping=In(
                table="aA__bB_A",
                keys={"a": "A"},
                values={"b": "B.A"},
            ),
        ),
    ]

    create_mapping_success_params = [
        Def(
            name="{table}_{statement_type}".format(
                table=input_def.mapping.table,
                statement_type=statement_type,
            ),
            mapping=input_def.mapping,
            expected="{statement_type}_{result_name}.sql".format(
                statement_type=statement_type,
                result_name=input_def.result,
            ),
            meta=statement_type,
        )
        for (input_def, statement_type) in itertools.product(
            success_mapping_definitions,
            statement_types,
        )
    ]

    create_mapping_fail_params = [
        Def(
            name=input_def.result.name,
            mapping=input_def.mapping,
            expected=input_def.result.exception,
        )
        for input_def in fail_mapping_definitions
    ]

    @parameterized.parameterized.expand(create_mapping_success_params)
    def test_create_mapping_success(self, name, mapping, expected, statement_type):
        log.debug("create CacheDictMapping")
        actual = CacheDictMapping(
            table=mapping.table,
            keys=mapping.keys,
            values=mapping.values,
        )
        log.debug("created CacheDictMapping: %s", actual)

        expected_statement_path = self.res_dir + expected
        with open(expected_statement_path, "r") as expected_statement_file:
            expected_statement = expected_statement_file.read()
        actual_statement = getattr(actual, statement_type)()
        self.assertEqual(expected_statement, actual_statement)

        log.debug("check statement caching")
        # since all the statements use the mapping tuple, changing it will
        # cause all the statment methods to raise an error due to accessing
        # properties of None
        actual.mapping_tuple = None
        actual_second_statement = getattr(actual, statement_type)()
        self.assertIs(actual_statement, actual_second_statement)

    @parameterized.parameterized.expand(create_mapping_fail_params)
    def test_create_mapping_fail(self, name, mapping, expected, meta):
        log.debug("fail create CacheDictMapping")
        # If we use expected here rather than SqliteCachingException then
        # the test _errors_ rather than fails. The asserts afterwards will
        # fail based on the value of expected
        with self.assertRaises(SqliteCachingException) as raised_context:
            CacheDictMapping(
                table=mapping.table,
                keys=mapping.keys,
                values=mapping.values,
            )
        actual = raised_context.exception
        self.assertEqual(actual.category.id, expected.category_id, actual.msg)
        self.assertEqual(actual.cause.id, expected.id, actual.msg)
        log.info(actual.cause.params)
