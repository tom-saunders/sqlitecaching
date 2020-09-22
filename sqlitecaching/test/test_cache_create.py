import enum
import logging
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


@test_level(TestLevel.PRE_COMMIT)
class TestCacheDictCreation(SqliteCachingTestBase):
    tmp_dir: int

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp(prefix=".test_tmp")
        shutil.copytree(
            f"{self.res_dir}/dicts/",
            f"{self.tmp_dir}/",
            dirs_exist_ok=True,
        )

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)

    success_params = [
        Def(
            name="minimal",
            mapping=CacheDictMapping(
                table="minimal",
                keys={"a": "A"},
                values=None,
            ),
            extra=Extra(preexisting={}, actions=[]),
        ),
    ]

    @parameterized.parameterized.expand(success_params)
    def test_open_anon_memory(self, name: str, mapping: CacheDictMapping, extra: Extra):
        c = CacheDict.open_anon_memory(
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
        self.assertNotEqual(c, None)

    @parameterized.parameterized.expand(success_params)
    def test_open_readwrite(self, name: str, mapping: CacheDictMapping, extra: Extra):
        print(f"{self.tmp_dir}/{name}.readwrite.sqlite")
        c = CacheDict.open_readwrite(
            path=f"{self.tmp_dir}/{name}.readwrite.sqlite",
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )
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
