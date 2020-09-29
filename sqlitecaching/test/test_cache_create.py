import enum
import logging
import os
import shutil
import sqlite3
import tempfile
import typing
from dataclasses import dataclass

import parameterized

from sqlitecaching.dict.dict import CacheDict, ToCreate
from sqlitecaching.dict.mapping import CacheDictMapping
from sqlitecaching.test import SqliteCachingTestBase, TestLevel, test_level

log = logging.getLogger(__name__)

KT = typing.TypeVar("KT")
VT = typing.TypeVar("VT")


@enum.unique
class ActionType(enum.Enum):
    ADD = enum.auto()
    REM = enum.auto()
    CLR = enum.auto()
    CRT = enum.auto()
    DEL = enum.auto()


class Action(typing.NamedTuple):
    type: ActionType
    result: typing.Optional[typing.Mapping[typing.Any, typing.Any]]
    key: typing.Optional[typing.Any] = None
    value: typing.Optional[typing.Any] = None


class Extra(typing.NamedTuple):
    sqlite_params: typing.Optional[typing.Mapping[str, typing.Any]] = None
    preexisting: typing.Optional[typing.Mapping[typing.Any, typing.Any]] = None
    actions: typing.Optional[typing.Iterable[Action]] = None


class Def(typing.NamedTuple):
    name: str
    mapping: CacheDictMapping
    extra: Extra


@dataclass(frozen=True)
class A:
    a: str


@dataclass(frozen=True)
class B:
    b: str


@dataclass(frozen=True)
class AB:
    a: str
    b: str


@dataclass(frozen=True)
class CD:
    c: str
    d: str


empty_aA__bB = CacheDictMapping(  # noqa: N816
    table="empty",
    key_type=A,
    key_types=A("A"),
    value_type=B,
    value_types=B("B"),
)
minimal_aA__bB = CacheDictMapping(  # noqa: N816
    table="minimal",
    key_type=A,
    key_types=A("A"),
    value_type=B,
    value_types=B("B"),
)
minimal_two_aA_bB__cC_dD = CacheDictMapping(  # noqa: N816
    table="minimal_two",
    key_type=AB,
    key_types=AB("A", "B"),
    value_type=CD,
    value_types=CD("C", "D"),
)

NOT_PRESENT = object()


@test_level(TestLevel.PRE_COMMIT)
class TestCacheDictCreation(SqliteCachingTestBase):
    tmp_dir: str

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp(dir=os.getcwd(), prefix=".test_tmp")
        shutil.copytree(
            f"{self.res_dir}/dicts/",
            f"{self.tmp_dir}/",
            dirs_exist_ok=True,
        )

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    success_params = [
        Def(
            name="empty",
            mapping=empty_aA__bB,
            extra=Extra(preexisting={}, actions=[]),
        ),
        Def(
            name="minimal",
            mapping=minimal_aA__bB,
            extra=Extra(
                preexisting={
                    A("a"): B("b"),
                    A("b"): B("a"),
                    A("f"): NOT_PRESENT,
                },
                actions=[],
            ),
        ),
        Def(
            name="minimal",
            mapping=minimal_two_aA_bB__cC_dD,
            extra=Extra(
                preexisting={
                    AB("a", "b"): CD("c", "d"),
                    AB("b", "a"): CD("d", "c"),
                    AB("a", "a"): NOT_PRESENT,
                },
                actions=[],
            ),
        ),
    ]

    @parameterized.parameterized.expand(success_params)
    def test_open_anon_memory(
        self,
        name: str,
        mapping: CacheDictMapping[KT, VT],
        extra: Extra,
    ):
        c = CacheDict[KT, VT].open_anon_memory(
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )
        self.assertNotEqual(c, None)

    @parameterized.parameterized.expand(success_params)
    def test_open_anon_disk(self, name: str, mapping: CacheDictMapping, extra: Extra):
        c = CacheDict.open_anon_disk(
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )
        self.assertNotEqual(c, None)

    @parameterized.parameterized.expand(success_params)
    def test_open_readonly(self, name: str, mapping: CacheDictMapping, extra: Extra):
        c = CacheDict.open_readonly(
            path=f"{self.tmp_dir}/{name}.readonly.sqlite",
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )
        if extra.preexisting:
            preexist = extra.preexisting
        else:
            preexist = {}
        for (key, expected) in preexist.items():
            if expected is not NOT_PRESENT:
                actual = c[key]
                self.assertEqual(actual, expected)
            else:
                with self.assertRaises(KeyError) as raised_context:
                    _ = c[key]
                _ = raised_context.exception

        _ = len(c)

        self.assertNotEqual(c, None)

    @parameterized.parameterized.expand(success_params)
    def test_open_readwrite(self, name: str, mapping: CacheDictMapping, extra: Extra):
        print(f"\n{mapping.create_statement()}\n")
        c = CacheDict.open_readwrite(
            path=f"{self.tmp_dir}/{name}.readwrite.sqlite",
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )
        if extra.preexisting:
            preexist = extra.preexisting
        else:
            preexist = {}
        for (key, expected) in preexist.items():
            if expected is not NOT_PRESENT:
                actual = c[key]
                self.assertEqual(actual, expected)
            else:
                with self.assertRaises(KeyError) as raised_context:
                    _ = c[key]
                _ = raised_context.exception

        self.assertNotEqual(c, None)

    @parameterized.parameterized.expand(success_params)
    def test_open_readwrite_create(
        self,
        name: str,
        mapping: CacheDictMapping,
        extra: Extra,
    ):
        c = CacheDict.open_readwrite(
            path=f"{self.tmp_dir}/{name}.create.sqlite",
            mapping=mapping,
            create=ToCreate.DATABASE,
            sqlite_params=extra.sqlite_params,
        )
        self.assertNotEqual(c, None)

    @parameterized.parameterized.expand(success_params)
    def test_create_from_connection_noargs(
        self,
        name: str,
        mapping: CacheDictMapping,
        extra: Extra,
    ):
        conn = sqlite3.connect("")
        c = CacheDict._create_from_conn(
            conn=conn,
            mapping=mapping,
        )
        self.assertNotEqual(c, None)
