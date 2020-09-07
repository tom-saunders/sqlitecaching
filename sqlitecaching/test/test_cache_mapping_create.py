import itertools
import logging
from collections import namedtuple

import parameterized

from sqlitecaching.dict.mapping import (
    CacheDictMapping,
    CacheDictMappingDuplicateKeyNameException,
    CacheDictMappingException,
    CacheDictMappingInvalidIdentifierException,
    CacheDictMappingKeyValOverlapException,
    CacheDictMappingMissingKeysException,
    CacheDictMappingNoIdentifierProvidedException,
    CacheDictMappingReservedTableException,
    CacheDictMappingTuple,
)
from sqlitecaching.test import CacheDictTestBase, TestLevel, test_level

log = logging.getLogger(__name__)

# if this isn't defined here then the listcomps inside the class fail
Def = namedtuple("Def", ["name", "mapping", "expected", "meta"], defaults=[None])


@test_level(TestLevel.PRE_COMMIT)
class TestCacheDictMapping(CacheDictTestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.res_dir += "mappings/"

    In = CacheDictMappingTuple
    InputDef = namedtuple("InputDef", ["result", "mapping"])

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
                keys={
                    "a": "A",
                },
                values={},
            ),
        ),
        InputDef(
            result="aA__to__bB",
            mapping=In(
                table="aa__bb",
                keys={
                    "a": "A",
                },
                values={
                    "b": "B",
                },
            ),
        ),
        InputDef(
            result="aA_bB__to__",
            mapping=In(
                table="aa_bb",
                keys={
                    "a": "A",
                    "b": "B",
                },
                values={},
            ),
        ),
        InputDef(
            result="aA_bB__to__",
            mapping=In(
                table="aA_bB",
                keys={
                    "a": "a",
                    "b": "b",
                },
                values={},
            ),
        ),
        InputDef(
            result="aA_bB__to__cC",
            mapping=In(
                table="aa_bb__cc",
                keys={
                    "a": "a",
                    "b": "b",
                },
                values={
                    "c": "C",
                },
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
    ]

    FailRes = namedtuple("FailRes", ["name", "exception"])
    fail_mapping_definitions = [
        InputDef(
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
        InputDef(
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
        InputDef(
            result=FailRes(
                name="invalid_table_name",
                exception=CacheDictMappingInvalidIdentifierException,
            ),
            mapping=In(
                table=" ",
                keys={"a": "A"},
                values={"b": "B"},
            ),
        ),
        InputDef(
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
        InputDef(
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
        InputDef(
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
        InputDef(
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
        InputDef(
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
        InputDef(
            result=FailRes(
                name="blank_key_name",
                exception=CacheDictMappingInvalidIdentifierException,
            ),
            mapping=In(
                table="A_bB",
                keys={" ": "A"},
                values={"b": "B"},
            ),
        ),
        InputDef(
            result=FailRes(
                name="duplicated_key_name",
                exception=CacheDictMappingDuplicateKeyNameException,
            ),
            mapping=In(
                table="aA_AA__bB",
                keys={"a": "A", "A": "A"},
                values={"b": "B"},
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

    @parameterized.parameterized.expand(create_mapping_fail_params)
    def test_create_mapping_fail(self, name, mapping, expected, meta):
        log.debug("fail create CacheDictMapping")
        # If we use expected here rather than CacheDictMappingException then
        # the test _errors_ rather than fails. The asserts afterwards will
        # fail based on the value of expected
        with self.assertRaises(CacheDictMappingException) as raised_context:
            CacheDictMapping(
                table=mapping.table,
                keys=mapping.keys,
                values=mapping.values,
            )
        actual = raised_context.exception
        self.assertEqual(actual.type_id, expected._type_id)
        self.assertEqual(actual.cause_id, expected._cause_id)
        log.info(raised_context.exception._params)
