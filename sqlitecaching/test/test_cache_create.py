import enum
import logging
import os
import shutil
import sqlite3
import tempfile
import typing

import parameterized

from sqlitecaching.dict.dict import CacheDict, ToCreate
from sqlitecaching.dict.mapping import CacheDictMapping
from sqlitecaching.test import SqliteCachingTestBase, TestLevel, test_level

log = logging.getLogger(__name__)


@enum.unique
class ActionType(enum.Enum):
    ADD = enum.auto()
    REM = enum.auto()
    CLR = enum.auto()
    CRT = enum.auto()
    DEL = enum.auto()


class Action(typing.NamedTuple):
    type: ActionType
    result: typing.Optional[
        typing.List[
            typing.Tuple[
                typing.Mapping[str, typing.Any],
                typing.Mapping[str, typing.Any],
            ]
        ]
    ]
    key: typing.Optional[typing.Mapping[str, typing.Any]] = None
    value: typing.Optional[typing.Mapping[str, typing.Any]] = None


class Extra(typing.NamedTuple):
    sqlite_params: typing.Optional[typing.Mapping[str, typing.Any]] = None
    preexisting: typing.Optional[
        typing.List[
            typing.Tuple[
                typing.Mapping[str, typing.Any],
                typing.Mapping[str, typing.Any],
            ]
        ]
    ] = None
    actions: typing.Optional[typing.Iterable[Action]] = None


class Def(typing.NamedTuple):
    name: str
    mapping: CacheDictMapping
    extra: Extra


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
            mapping=CacheDictMapping(
                table="empty",
                keys={"a": "A"},
                values={"b": "B"},
            ),
            extra=Extra(preexisting=[], actions=[]),
        ),
        Def(
            name="minimal",
            mapping=CacheDictMapping(
                table="minimal",
                keys={"a": "A"},
                values={"b": "B"},
            ),
            extra=Extra(
                preexisting=[
                    (
                        {"a": "a"},
                        {"b": "b"},
                    ),
                    (
                        {"a": "b"},
                        {"b": "a"},
                    ),
                    (
                        {"a": "f"},
                        {},
                    ),
                ],
                actions=[],
            ),
        ),
        Def(
            name="minimal",
            mapping=CacheDictMapping(
                table="minimal_two",
                keys={"a": "A", "b": "B"},
                values={"c": "C", "d": "D"},
            ),
            extra=Extra(
                preexisting=[
                    (
                        {"a": "a", "b": "b"},
                        {"c": "c", "d": "d"},
                    ),
                    (
                        {"a": "b", "b": "a"},
                        {"c": "d", "d": "c"},
                    ),
                    (
                        {"a": "a", "b": "a"},
                        {},
                    ),
                ],
                actions=[],
            ),
        ),
    ]

    @parameterized.parameterized.expand(success_params)
    def test_open_anon_memory(self, name: str, mapping: CacheDictMapping, extra: Extra):
        c = CacheDict.open_anon_memory(
            mapping=mapping,
            key_tuple=mapping.KeyTuple,
            value_tuple=mapping.ValueTuple,
            sqlite_params=extra.sqlite_params,
        )
        self.assertNotEqual(c, None)

    @parameterized.parameterized.expand(success_params)
    def test_open_anon_disk(self, name: str, mapping: CacheDictMapping, extra: Extra):
        c = CacheDict.open_anon_disk(
            mapping=mapping,
            key_tuple=mapping.KeyTuple,
            value_tuple=mapping.ValueTuple,
            sqlite_params=extra.sqlite_params,
        )
        self.assertNotEqual(c, None)

    @parameterized.parameterized.expand(success_params)
    def test_open_readonly(self, name: str, mapping: CacheDictMapping, extra: Extra):
        c = CacheDict.open_readonly(
            path=f"{self.tmp_dir}/{name}.readonly.sqlite",
            mapping=mapping,
            key_tuple=mapping.KeyTuple,
            value_tuple=mapping.ValueTuple,
            sqlite_params=extra.sqlite_params,
        )
        if extra.preexisting:
            preexist = extra.preexisting
        else:
            preexist = []
        for (key, expected) in preexist:
            if expected:
                actual = c[key]
                for (exp_key, exp_value) in expected.items():
                    self.assertEqual(actual[exp_key], exp_value)
            else:
                with self.assertRaises(KeyError) as raised_context:
                    _ = c[key]
                _ = raised_context.exception

        self.assertNotEqual(c, None)

    @parameterized.parameterized.expand(success_params)
    def test_open_readwrite(self, name: str, mapping: CacheDictMapping, extra: Extra):
        c = CacheDict.open_readwrite(
            path=f"{self.tmp_dir}/{name}.readwrite.sqlite",
            mapping=mapping,
            key_tuple=mapping.KeyTuple,
            value_tuple=mapping.ValueTuple,
            sqlite_params=extra.sqlite_params,
        )
        if extra.preexisting:
            preexist = extra.preexisting
        else:
            preexist = []
        for (key, expected) in preexist:
            if expected:
                actual = c[key]
                for (exp_key, exp_value) in expected.items():
                    self.assertEqual(actual[exp_key], exp_value)
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
            key_tuple=mapping.KeyTuple,
            value_tuple=mapping.ValueTuple,
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
            key_tuple=mapping.KeyTuple,
            value_tuple=mapping.ValueTuple,
        )
        self.assertNotEqual(c, None)
