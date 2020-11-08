import dataclasses
import enum
import logging
import os
import shutil
import sqlite3
import tempfile
import typing
from dataclasses import dataclass

import parameterized

from sqlitecaching.dict.dict import (
    CacheDict,
    CacheDictNoSuchKeyException,
    CacheDictPopItemEmptyException,
    CacheDictReadOnlyException,
    CacheDictUpdateKwargsException,
    ToCreate,
)
from sqlitecaching.dict.mapping import CacheDictMapping
from sqlitecaching.exceptions import SqliteCachingException
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


@dataclass(frozen=True)
class Action(typing.Generic[KT, VT]):
    type: ActionType
    result: typing.Mapping[KT, VT] = dataclasses.field(default_factory=dict)
    payload: typing.Optional[typing.Tuple[KT, typing.Optional[VT]]] = None

    def __iter__(self):
        return (getattr(self, field.name) for field in dataclasses.fields(self))


@dataclass(frozen=True)
class NotPresent:
    sentinel = object()


NOT_PRESENT = NotPresent()


@dataclass(frozen=True)
class Extra(typing.Generic[KT, VT]):
    sqlite_params: typing.Optional[typing.Mapping[str, typing.Any]] = None
    preexisting: typing.Optional[
        typing.Mapping[KT, typing.Union[VT, NotPresent]]
    ] = None
    actions: typing.Iterable[Action] = dataclasses.field(default_factory=list)
    updates: typing.Iterable[typing.Tuple[KT, VT]] = dataclasses.field(
        default_factory=list,
    )

    def __iter__(self):
        return (getattr(self, field.name) for field in dataclasses.fields(self))


@dataclass(frozen=True)
class Def(typing.Generic[KT, VT]):
    name: str
    mapping: CacheDictMapping[KT, VT]
    extra: Extra[KT, VT]

    def __iter__(self):
        return (getattr(self, field.name) for field in dataclasses.fields(self))


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


@dataclass(frozen=True)
class Empty:
    pass


empty = CacheDictMapping[A, B](  # noqa: N816
    table="empty",
    key_type=A,
    key_types=A("A"),
    value_type=B,
    value_types=B("B"),
)
minimal = CacheDictMapping[A, B](  # noqa: N816
    table="minimal",
    key_type=A,
    key_types=A("A"),
    value_type=B,
    value_types=B("B"),
)
minimal_two = CacheDictMapping[AB, CD](  # noqa: N816
    table="minimal_two",
    key_type=AB,
    key_types=AB("A", "B"),
    value_type=CD,
    value_types=CD("C", "D"),
)

minimal_three = CacheDictMapping[A, Empty](  # noqa: N816
    table="minimal_three",
    key_type=A,
    key_types=A("A"),
    value_type=Empty,
    value_types=Empty(),
)


@test_level(TestLevel.PRE_COMMIT)
class TestCacheDict(SqliteCachingTestBase):
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

    def _create_missing_value(
        self,
        mapping: CacheDictMapping[KT, VT],
        missing_placeholder: bytes = b"MISSING_VALUE",
    ) -> VT:
        value_type: typing.Type[VT] = mapping.ValueType
        value_placeholder_dict = {
            field.name: missing_placeholder for field in dataclasses.fields(value_type)
        }
        value: VT
        value = value_type(**value_placeholder_dict)  # type: ignore
        return value

    success_params = [
        Def[A, B](
            name="empty",
            mapping=empty,
            extra=Extra[A, B](
                preexisting={},
                actions=[],
                updates=[
                    (A("A"), B("B")),
                    (A("B"), B("A")),
                ],
            ),
        ),
        Def[A, B](
            name="minimal",
            mapping=minimal,
            extra=Extra[A, B](
                preexisting={
                    A("a"): B("b"),
                    A("b"): B("a"),
                    A("f"): NOT_PRESENT,
                },
                actions=[],
                updates=[
                    (A("c"), B("c")),
                    (A("d"), B("d")),
                ],
            ),
        ),
        Def[AB, CD](
            name="minimal",
            mapping=minimal_two,
            extra=Extra[AB, CD](
                preexisting={
                    AB("a", "b"): CD("c", "d"),
                    AB("b", "a"): CD("d", "c"),
                    AB("a", "a"): NOT_PRESENT,
                },
                actions=[],
                updates=[
                    (AB("c", "d"), CD("a", "b")),
                    (AB("d", "c"), CD("b", "a")),
                ],
            ),
        ),
        Def[A, Empty](
            name="minimal",
            mapping=minimal_three,
            extra=Extra[A, Empty](
                preexisting={
                    A("a"): Empty(),
                    A("b"): Empty(),
                    A("c"): NOT_PRESENT,
                },
                actions=[],
                updates=[
                    (A("c"), Empty()),
                ],
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
    def test_readonly_preexist(
        self,
        name: str,
        mapping: CacheDictMapping,
        extra: Extra,
    ):
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
            with self.subTest(key=key, expected=expected):
                if expected is not NOT_PRESENT:
                    actual_value = c[key]
                    self.assertEqual(actual_value, expected)

                    actual_present = key in c
                    self.assertTrue(actual_present)

                    actual_missing = key not in c
                    self.assertFalse(actual_missing)
                else:
                    actual_present = key in c
                    self.assertFalse(actual_present)

                    actual_missing = key not in c
                    self.assertTrue(actual_missing)

                    with self.assertRaises(KeyError) as raised_context:
                        _ = c[key]
                    actual: typing.Any = raised_context.exception
                    self.assertIsInstance(actual, SqliteCachingException)
                    self.assertEqual(
                        actual.category.id,
                        CacheDictNoSuchKeyException.category_id,
                        actual.msg,
                    )
                    self.assertEqual(
                        actual.cause.id,
                        CacheDictNoSuchKeyException.id,
                        actual.msg,
                    )

    @parameterized.parameterized.expand(success_params)
    def test_readonly_preexist_get(
        self,
        name: str,
        mapping: CacheDictMapping,
        extra: Extra,
    ):
        c = CacheDict.open_readonly(
            path=f"{self.tmp_dir}/{name}.readonly.sqlite",
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )
        if extra.preexisting:
            preexist = extra.preexisting
        else:
            preexist = {}

        missing_value = "MISSING_VALUE_MARKER"

        for (key, expected) in preexist.items():
            with self.subTest(key=key, expected=expected):
                if expected is not NOT_PRESENT:
                    actual_value = c.get(key, missing_value)
                    self.assertIsNot(actual_value, missing_value)
                    self.assertEqual(actual_value, expected)
                else:
                    actual_value = c.get(key, missing_value)
                    self.assertIs(actual_value, missing_value)

    @parameterized.parameterized.expand(success_params)
    def test_readonly_preexist_get_nodefault(
        self,
        name: str,
        mapping: CacheDictMapping,
        extra: Extra,
    ):
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
            with self.subTest(key=key, expected=expected):
                if expected is not NOT_PRESENT:
                    actual_value = c.get(key)
                    self.assertEqual(actual_value, expected)
                else:
                    missing_value = c.get(key)
                    self.assertIsNone(missing_value)

    @parameterized.parameterized.expand(success_params)
    def test_readonly_setdefault(
        self,
        name: str,
        mapping: CacheDictMapping,
        extra: Extra,
    ):
        c = CacheDict.open_readonly(
            path=f"{self.tmp_dir}/{name}.readonly.sqlite",
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )
        if extra.preexisting:
            preexist = extra.preexisting
        else:
            preexist = {}

        missing_value = self._create_missing_value(mapping)

        for (key, expected) in preexist.items():
            with self.subTest(key=key, expected=expected):
                if expected is not NOT_PRESENT:
                    actual_value = c.get(key)
                    self.assertIsNot(actual_value, missing_value)
                    self.assertEqual(actual_value, expected)

                    actual_value = c.setdefault(key, missing_value)
                    self.assertIsNot(actual_value, missing_value)
                    self.assertEqual(actual_value, expected)
                else:
                    actual_value = c.get(key)
                    self.assertIsNone(actual_value)

                    with self.assertRaises(SqliteCachingException) as raised_context:
                        _ = c.setdefault(key, missing_value)
                    actual: typing.Any = raised_context.exception
                    self.assertEqual(
                        actual.category.id,
                        CacheDictReadOnlyException.category_id,
                        actual.msg,
                    )
                    self.assertEqual(
                        actual.cause.id,
                        CacheDictReadOnlyException.id,
                        actual.msg,
                    )

    @parameterized.parameterized.expand(success_params)
    def test_readonly_pop(
        self,
        name: str,
        mapping: CacheDictMapping,
        extra: Extra,
    ):
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
            with self.subTest(key=key, expected=expected):
                if expected is not NOT_PRESENT:
                    actual_value = c.get(key)
                    self.assertEqual(actual_value, expected)

                    with self.assertRaises(SqliteCachingException) as raised_context:
                        _ = c.pop(key)
                    actual: typing.Any = raised_context.exception
                    self.assertEqual(
                        actual.category.id,
                        CacheDictReadOnlyException.category_id,
                        actual.msg,
                    )
                    self.assertEqual(
                        actual.cause.id,
                        CacheDictReadOnlyException.id,
                        actual.msg,
                    )
                else:
                    actual_value = c.get(key)
                    self.assertIsNone(actual_value)

                    with self.assertRaises(KeyError) as raised_context_keyerror:
                        _ = c.pop(key)
                    actual_ex: typing.Any = raised_context_keyerror.exception
                    self.assertIsInstance(actual_ex, SqliteCachingException)
                    self.assertEqual(
                        actual_ex.category.id,
                        CacheDictNoSuchKeyException.category_id,
                        actual_ex.msg,
                    )
                    self.assertEqual(
                        actual_ex.cause.id,
                        CacheDictNoSuchKeyException.id,
                        actual_ex.msg,
                    )

    @parameterized.parameterized.expand(success_params)
    def test_readonly_pop_default(
        self,
        name: str,
        mapping: CacheDictMapping,
        extra: Extra,
    ):
        c = CacheDict.open_readonly(
            path=f"{self.tmp_dir}/{name}.readonly.sqlite",
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )
        if extra.preexisting:
            preexist = extra.preexisting
        else:
            preexist = {}

        missing_value = self._create_missing_value(mapping)

        for (key, expected) in preexist.items():
            with self.subTest(key=key, expected=expected):
                if expected is not NOT_PRESENT:
                    actual_value = c.get(key)
                    self.assertEqual(actual_value, expected)

                    with self.assertRaises(SqliteCachingException) as raised_context:
                        _ = c.pop(key, missing_value)
                    actual: typing.Any = raised_context.exception
                    self.assertEqual(
                        actual.category.id,
                        CacheDictReadOnlyException.category_id,
                        actual.msg,
                    )
                    self.assertEqual(
                        actual.cause.id,
                        CacheDictReadOnlyException.id,
                        actual.msg,
                    )
                else:
                    actual_value = c.get(key)
                    self.assertIsNone(actual_value)

                    actual = c.pop(key, missing_value)
                    self.assertIs(actual, missing_value)

    @parameterized.parameterized.expand(success_params)
    def test_readonly_popitem(
        self,
        name: str,
        mapping: CacheDictMapping,
        extra: Extra,
    ):
        c = CacheDict.open_readonly(
            path=f"{self.tmp_dir}/{name}.readonly.sqlite",
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )
        if extra.preexisting:
            preexist = extra.preexisting
            for (key, expected) in preexist.items():
                with self.subTest(key=key, expected=expected):
                    if expected is not NOT_PRESENT:
                        with self.assertRaises(
                            SqliteCachingException,
                        ) as raised_context:
                            _ = c.popitem()
                        actual: typing.Any = raised_context.exception
                        self.assertEqual(
                            actual.category.id,
                            CacheDictReadOnlyException.category_id,
                            actual.msg,
                        )
                        self.assertEqual(
                            actual.cause.id,
                            CacheDictReadOnlyException.id,
                            actual.msg,
                        )
        else:
            with self.assertRaises(KeyError) as raised_context_keyerror:
                _ = c.popitem()
            actual_empty: typing.Any = raised_context_keyerror.exception
            self.assertIsInstance(actual_empty, SqliteCachingException)
            self.assertEqual(
                actual_empty.category.id,
                CacheDictPopItemEmptyException.category_id,
                actual_empty.msg,
            )
            self.assertEqual(
                actual_empty.cause.id,
                CacheDictPopItemEmptyException.id,
                actual_empty.msg,
            )

    @parameterized.parameterized.expand(success_params)
    def test_readonly_update_none(
        self,
        name: str,
        mapping: CacheDictMapping,
        extra: Extra,
    ):
        c = CacheDict.open_readonly(
            path=f"{self.tmp_dir}/{name}.readonly.sqlite",
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )
        c.update()

    @parameterized.parameterized.expand(success_params)
    def test_readonly_update_mapping(
        self,
        name: str,
        mapping: CacheDictMapping,
        extra: Extra,
    ):
        c = CacheDict.open_readonly(
            path=f"{self.tmp_dir}/{name}.readonly.sqlite",
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )
        with self.assertRaises(SqliteCachingException) as raised_context:
            c.update({k: v for k, v in extra.updates})
        actual: typing.Any = raised_context.exception
        self.assertEqual(
            actual.category.id,
            CacheDictReadOnlyException.category_id,
            actual.msg,
        )
        self.assertEqual(
            actual.cause.id,
            CacheDictReadOnlyException.id,
            actual.msg,
        )

    @parameterized.parameterized.expand(success_params)
    def test_readonly_update_iterable(
        self,
        name: str,
        mapping: CacheDictMapping,
        extra: Extra,
    ):
        c = CacheDict.open_readonly(
            path=f"{self.tmp_dir}/{name}.readonly.sqlite",
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )
        with self.assertRaises(SqliteCachingException) as raised_context:
            c.update(extra.updates)
        actual: typing.Any = raised_context.exception
        self.assertEqual(
            actual.category.id,
            CacheDictReadOnlyException.category_id,
            actual.msg,
        )
        self.assertEqual(
            actual.cause.id,
            CacheDictReadOnlyException.id,
            actual.msg,
        )

    @parameterized.parameterized.expand(success_params)
    def test_readonly_update_kwargs(
        self,
        name: str,
        mapping: CacheDictMapping,
        extra: Extra,
    ):
        c = CacheDict.open_readonly(
            path=f"{self.tmp_dir}/{name}.readonly.sqlite",
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )
        with self.assertRaises(SqliteCachingException) as raised_context:
            c.update(x="a")
        actual = raised_context.exception
        self.assertEqual(
            actual.category.id,
            CacheDictUpdateKwargsException.category_id,
            actual.msg,
        )
        self.assertEqual(
            actual.cause.id,
            CacheDictUpdateKwargsException.id,
            actual.msg,
        )

    @parameterized.parameterized.expand(success_params)
    def test_readonly_preexist_bool(
        self,
        name: str,
        mapping: CacheDictMapping,
        extra: Extra,
    ):
        c = CacheDict.open_readonly(
            path=f"{self.tmp_dir}/{name}.readonly.sqlite",
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )
        actual = bool(c)
        if extra.preexisting:
            self.assertTrue(actual)
        else:
            self.assertFalse(actual)

    @parameterized.parameterized.expand(success_params)
    def test_readonly_preexist_complete(
        self,
        name: str,
        mapping: CacheDictMapping,
        extra: Extra,
    ):
        c = CacheDict.open_readonly(
            path=f"{self.tmp_dir}/{name}.readonly.sqlite",
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )
        if extra.preexisting:
            preexist = extra.preexisting
        else:
            preexist = {}

        item_count = 0
        for (actual_key, actual_value) in c.items():
            with self.subTest(actual_key=actual_key, actual_value=actual_value):
                in_preexist = (actual_key, actual_value) in preexist.items()
                self.assertTrue(in_preexist, "Missing key/value in preexisting items")
                item_count += 1

        preexist_present_count = sum(
            1 for x in preexist.values() if x is not NOT_PRESENT
        )
        self.assertEqual(item_count, preexist_present_count)
        self.assertEqual(len(c), preexist_present_count)

    @parameterized.parameterized.expand(success_params)
    def test_readonly_in(self, name: str, mapping: CacheDictMapping, extra: Extra):
        c = CacheDict.open_readonly(
            path=f"{self.tmp_dir}/{name}.readonly.sqlite",
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )

        key_count = 0
        for _ in c:
            key_count += 1

        self.assertEqual(key_count, len(c))

    @parameterized.parameterized.expand(success_params)
    def test_readonly_keys(self, name: str, mapping: CacheDictMapping, extra: Extra):
        c = CacheDict.open_readonly(
            path=f"{self.tmp_dir}/{name}.readonly.sqlite",
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )

        key_count = 0
        for _ in c.keys():
            key_count += 1

        self.assertEqual(key_count, len(c))

    @parameterized.parameterized.expand(success_params)
    def test_readonly_values(self, name: str, mapping: CacheDictMapping, extra: Extra):
        c = CacheDict.open_readonly(
            path=f"{self.tmp_dir}/{name}.readonly.sqlite",
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )

        value_count = 0
        for _ in c.values():
            value_count += 1

        self.assertEqual(value_count, len(c))

    @parameterized.parameterized.expand(success_params)
    def test_readonly_iter(self, name: str, mapping: CacheDictMapping, extra: Extra):
        c = CacheDict.open_readonly(
            path=f"{self.tmp_dir}/{name}.readonly.sqlite",
            mapping=mapping,
            sqlite_params=extra.sqlite_params,
        )
        key_count = 0
        for _ in iter(c):
            key_count += 1

        self.assertEqual(key_count, len(c))

        # _ = list(c)
        # _ = bool(c)

    @parameterized.parameterized.expand(success_params)
    def test_open_readwrite(self, name: str, mapping: CacheDictMapping, extra: Extra):
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
            with self.subTest(key=key, expected=expected):
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
