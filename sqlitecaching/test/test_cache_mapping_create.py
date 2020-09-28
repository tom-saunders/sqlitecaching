import itertools
import logging
import typing
from dataclasses import dataclass

import parameterized

from sqlitecaching.dict.mapping import (
    CacheDictMapping,
    CacheDictMappingIncorrectKeyTypesTypeException,
    CacheDictMappingIncorrectValueTypesTypeException,
    CacheDictMappingInvalidIdentifierException,
    CacheDictMappingInvalidSQLTypeException,
    CacheDictMappingKeyValOverlapException,
    CacheDictMappingMissingKeysException,
    CacheDictMappingNoIdentifierProvidedException,
    CacheDictMappingReservedTableException,
)
from sqlitecaching.exceptions import ExceptProvider, SqliteCachingException
from sqlitecaching.test import SqliteCachingTestBase, TestLevel, test_level

log = logging.getLogger(__name__)

KT = typing.TypeVar("KT")
VT = typing.TypeVar("VT")


@dataclass
class In(typing.Generic[KT, VT]):
    table: str
    key_type: typing.Type[KT]
    value_type: typing.Type[VT]
    key_types: typing.Optional[KT] = None
    value_types: typing.Optional[VT] = None


@dataclass
class Empty:
    pass


@dataclass
class A:
    a: str


@dataclass
class B:
    b: str


@dataclass
class C:
    c: str


@dataclass
class AA:
    a: str
    A: str


@dataclass
class AB:
    a: str
    b: str


@dataclass
class BB:
    b: str
    B: str


@dataclass
class CD:
    c: str
    d: str


@dataclass
class InvIn(typing.Generic[KT, VT]):
    table: typing.Optional[str]
    key_type: typing.Optional[typing.Type[KT]]
    value_type: typing.Optional[typing.Type[VT]]
    key_types: typing.Optional[KT] = None
    value_types: typing.Optional[VT] = None


class Def(typing.NamedTuple, typing.Generic[KT, VT]):
    name: str
    mapping: typing.Union[In[KT, VT], InvIn[KT, VT]]
    expected: typing.Any
    meta: typing.Optional[typing.Any] = None


class FailRes(typing.NamedTuple):
    name: str
    exception: ExceptProvider[SqliteCachingException]


@dataclass
class InputDef(typing.Generic[KT, VT]):
    result: str
    mapping: In[KT, VT]


@dataclass
class FailInputDef(typing.Generic[KT, VT]):
    result: FailRes
    mapping: InvIn[KT, VT]


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
        "select_statement",
        "remove_statement",
        "length_statement",
        "keys_statement",
        "items_statement",
        "values_statement",
    ]

    success_mapping_definitions: typing.Iterable[InputDef] = [
        InputDef(
            result="aA__to__",
            mapping=In(
                table="aa",
                key_type=A,
                key_types=A("A"),
                value_type=Empty,
            ),
        ),
        InputDef(
            result="aA__to__bB",
            mapping=In(
                table="aa__bb",
                key_type=A,
                key_types=A("A"),
                value_type=B,
                value_types=B("B"),
            ),
        ),
        InputDef(
            result="aA_bB__to__",
            mapping=In(
                table="aa_bb",
                key_type=AB,
                key_types=AB("A", "B"),
                value_type=Empty,
            ),
        ),
        InputDef(
            result="aA_bB__to__",
            mapping=In(
                table="aa_bb",
                key_type=AB,
                key_types=AB("A", "B"),
                value_type=Empty,
            ),
        ),
        InputDef(
            result="aA_bB__to__cC",
            mapping=In(
                table="aa_bb__cc",
                key_type=AB,
                key_types=AB("A", "B"),
                value_type=C,
                value_types=C("C"),
            ),
        ),
        InputDef(
            result="aA_bB__to__cC_dD",
            mapping=In(
                table="aa_bb__cc_dd",
                key_type=AB,
                key_types=AB("A", "B"),
                value_type=CD,
                value_types=CD("C", "D"),
            ),
        ),
        InputDef(
            result="aA_bB__to__cC_dD",
            mapping=In(
                table="aa_bb__cc_dd",
                key_type=AB,
                key_types=AB("A", "B"),
                value_type=CD,
                value_types=CD("C", "D"),
            ),
        ),
        InputDef(
            result="a___to__cC",
            mapping=In(
                table="a___cc",
                key_type=A,
                value_type=C,
                value_types=C("C"),
            ),
        ),
        InputDef(
            result="a___to__cC",
            mapping=In(
                table="a___cc",
                key_type=A,
                value_type=C,
                value_types=C("C"),
            ),
        ),
        InputDef(
            result="a___to__cC",
            mapping=In(
                table="a___cc",
                key_type=A,
                key_types=A(None),  # type: ignore
                value_type=C,
                value_types=C("C"),
            ),
        ),
        InputDef(
            result="aA__to__c_",
            mapping=In(
                table="aa__c_",
                key_type=A,
                key_types=A("A"),
                value_type=C,
                value_types=C(""),
            ),
        ),
        InputDef(
            result="aA__to__c_",
            mapping=In(
                table="aa__c_",
                key_type=A,
                key_types=A("A"),
                value_type=C,
                value_types=C(None),  # type: ignore
            ),
        ),
        InputDef(
            result="aA__to__c_",
            mapping=In(
                table="aa__c_",
                key_type=A,
                key_types=A("A"),
                value_type=C,
                value_types=C(0),  # type: ignore
            ),
        ),
    ]

    fail_mapping_definitions: typing.Iterable[FailInputDef] = [
        FailInputDef(
            result=FailRes(
                name="blank_table_name",
                exception=CacheDictMappingNoIdentifierProvidedException,
            ),
            mapping=InvIn(
                table="",
                key_type=A,
                value_type=B,
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="none_table_name",
                exception=CacheDictMappingNoIdentifierProvidedException,
            ),
            mapping=InvIn(
                table=None,
                key_type=A,
                value_type=B,
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="space_table_name",
                exception=CacheDictMappingInvalidIdentifierException,
            ),
            mapping=InvIn(
                table=" ",
                key_type=A,
                value_type=B,
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="invalid_table_name",
                exception=CacheDictMappingInvalidIdentifierException,
            ),
            mapping=InvIn(
                table="x.y",
                key_type=A,
                value_type=A,
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="reserved_table_name",
                exception=CacheDictMappingReservedTableException,
            ),
            mapping=InvIn(
                table="sqlite_a",
                key_type=A,
                value_type=B,
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="missing_keys",
                exception=CacheDictMappingMissingKeysException,
            ),
            mapping=InvIn(
                table="x__bb",
                key_type=Empty,
                value_type=B,
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="overlapping_key_ands_values",
                exception=CacheDictMappingKeyValOverlapException,
            ),
            mapping=InvIn(
                table="aa__aa_bb",
                key_type=A,
                value_type=AB,
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="invalid_key_sqltype",
                exception=CacheDictMappingInvalidSQLTypeException,
            ),
            mapping=InvIn(
                table="aa_b__bb",
                key_type=A,
                key_types=A("A.B"),
                value_type=B,
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="invalid_value_sqltype",
                exception=CacheDictMappingInvalidSQLTypeException,
            ),
            mapping=InvIn(
                table="aa__bb_a",
                key_type=A,
                value_type=B,
                value_types=B("B.A"),
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="invalid_key_types_type",
                exception=CacheDictMappingIncorrectKeyTypesTypeException,
            ),
            mapping=InvIn(
                table="aa__bb_a",
                key_type=A,
                key_types=B("B"),
                value_type=B,
            ),
        ),
        FailInputDef(
            result=FailRes(
                name="invalid_value_types_type",
                exception=CacheDictMappingIncorrectValueTypesTypeException,
            ),
            mapping=InvIn(
                table="aa__bb_a",
                key_type=A,
                value_type=B,
                value_types=A("A"),
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
    def test_create_mapping_success(
        self,
        name: str,
        mapping: In,
        expected: str,
        statement_type: str,
    ):
        log.debug("create CacheDictMapping")
        actual = CacheDictMapping(  # typing: ignore
            table=mapping.table,
            key_type=mapping.key_type,
            key_types=mapping.key_types,
            value_type=mapping.value_type,
            value_types=mapping.value_types,
        )
        log.debug("created CacheDictMapping: %s", actual)

        expected_statement_path = self.res_dir + expected
        with open(expected_statement_path, "r") as expected_statement_file:
            expected_statement = expected_statement_file.read()
        actual_statement = getattr(actual, statement_type)()
        self.assertEqual(expected_statement, actual_statement)

        log.debug("check statement caching")
        # since all the statements use the table_ident, changing it will
        # cause all the statment methods to return different responses unless
        # the value is cached. The actual type is ValidIdent but it is actually
        # a str underneath.
        actual.table_ident = ""  # type: ignore
        actual_second_statement = getattr(actual, statement_type)()
        self.assertIs(actual_statement, actual_second_statement)

    @parameterized.parameterized.expand(create_mapping_fail_params)
    def test_create_mapping_fail(
        self,
        name: str,
        mapping: InvIn,
        expected: ExceptProvider[SqliteCachingException],
        _,
    ):
        log.debug("fail create CacheDictMapping")
        # If we use expected here rather than SqliteCachingException then
        # the test _errors_ rather than fails. The asserts afterwards will
        # fail based on the value of expected
        with self.assertRaises(SqliteCachingException) as raised_context:
            # We are intentionally providing values which don't meet the typing
            # specified by the __init__ method, so have to ignore types here
            CacheDictMapping(
                table=mapping.table,  # type: ignore
                key_type=mapping.key_type,  # type: ignore
                key_types=mapping.key_types,  # type: ignore
                value_type=mapping.value_type,  # type: ignore
                value_types=mapping.value_types,  # type: ignore
            )
        actual = raised_context.exception
        self.assertEqual(actual.category.id, expected.category_id, actual.msg)
        self.assertEqual(actual.cause.id, expected.id, actual.msg)
        log.info(actual.cause.params)
