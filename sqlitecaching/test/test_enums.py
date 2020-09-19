import enum
import logging
import typing

import parameterized

from sqlitecaching.enums import (
    EnumDuplicateValueException,
    EnumNameClashException,
    EnumValueConversionException,
    LevelledEnum,
)
from sqlitecaching.exceptions import SqliteCachingException
from sqlitecaching.test import SqliteCachingTestBase, TestLevel, test_level

log = logging.getLogger(__name__)


class Cmp(enum.Flag):
    lt = enum.auto()
    le = enum.auto()
    gt = enum.auto()
    ge = enum.auto()
    eq = enum.auto()
    ne = enum.auto()


class Def(typing.NamedTuple):
    name: str
    left: typing.Any
    right: typing.Any
    expected: typing.Optional[typing.Any] = None


class TestEnumAB(LevelledEnum):
    A = 0
    B = 1


class TestEnumBA(LevelledEnum):
    B = 0
    A = 1


@test_level(TestLevel.PRE_COMMIT)
class TestSqliteCachingEnums(SqliteCachingTestBase):
    success_params = [
        Def(
            "ab_a__ab_a",
            TestEnumAB.A,
            TestEnumAB.A,
            Cmp.le | Cmp.ge | Cmp.eq,
        ),
        Def(
            "ab_a__ab_b",
            TestEnumAB.A,
            TestEnumAB.B,
            Cmp.lt | Cmp.le | Cmp.ne,
        ),
        Def(
            "ab_b__ab_a",
            TestEnumAB.B,
            TestEnumAB.A,
            Cmp.gt | Cmp.ge | Cmp.ne,
        ),
        Def(
            "ab_b__ab_b",
            TestEnumAB.B,
            TestEnumAB.B,
            Cmp.le | Cmp.ge | Cmp.eq,
        ),
        Def(
            "ba_a__ba_a",
            TestEnumBA.A,
            TestEnumBA.A,
            Cmp.le | Cmp.ge | Cmp.eq,
        ),
        Def(
            "ba_a__ba_b",
            TestEnumBA.A,
            TestEnumBA.B,
            Cmp.gt | Cmp.ge | Cmp.ne,
        ),
        Def(
            "ba_b__ba_a",
            TestEnumBA.B,
            TestEnumBA.A,
            Cmp.lt | Cmp.le | Cmp.ne,
        ),
        Def(
            "ba_b__ba_b",
            TestEnumBA.B,
            TestEnumBA.B,
            Cmp.le | Cmp.ge | Cmp.eq,
        ),
    ]
    fail_params = [
        Def("ab_a__ba_a", TestEnumAB.A, TestEnumBA.A),
        Def("ab_a__ba_b", TestEnumAB.A, TestEnumBA.B),
        Def("ab_b__ba_a", TestEnumAB.B, TestEnumBA.A),
        Def("ab_b__ba_b", TestEnumAB.B, TestEnumBA.B),
    ]
    mistyped_params = [
        Def("ab_a__0", TestEnumAB.A, 0),
        Def("ab_b__0", TestEnumAB.B, 0),
        Def("ab_a__1", TestEnumAB.A, 1),
        Def("ab_b__1", TestEnumAB.B, 1),
        Def("0__ba_a", 0, TestEnumBA.A),
        Def("0__ba_b", 0, TestEnumBA.B),
        Def("1__ba_a", 1, TestEnumBA.A),
        Def("1__ba_b", 1, TestEnumBA.B),
    ]
    value_str_params = [
        Def("", TestEnumAB, frozenset(["a", "b"])),
        Def("", TestEnumAB, frozenset(["b", "a"])),
        Def("", TestEnumBA, frozenset(["a", "b"])),
        Def("", TestEnumBA, frozenset(["b", "a"])),
    ]
    convert_params = [
        Def("", TestEnumAB, "a", TestEnumAB.A),
        Def("", TestEnumAB, "b", TestEnumAB.B),
        Def("", TestEnumBA, "a", TestEnumBA.A),
        Def("", TestEnumBA, "b", TestEnumBA.B),
    ]

    @parameterized.parameterized.expand(success_params)
    def test_lt_success(self, name, left, right, expected):
        actual = left < right
        self.assertEqual(actual, Cmp.lt == (expected & Cmp.lt))

    @parameterized.parameterized.expand(success_params)
    def test_le_success(self, name, left, right, expected):
        actual = left <= right
        self.assertEqual(actual, Cmp.le == (expected & Cmp.le))

    @parameterized.parameterized.expand(success_params)
    def test_gt_success(self, name, left, right, expected):
        actual = left > right
        self.assertEqual(actual, Cmp.gt == (expected & Cmp.gt))

    @parameterized.parameterized.expand(success_params)
    def test_ge_success(self, name, left, right, expected):
        actual = left >= right
        self.assertEqual(actual, Cmp.ge == (expected & Cmp.ge))

    @parameterized.parameterized.expand(success_params)
    def test_eq_success(self, name, left, right, expected):
        actual = left == right
        self.assertEqual(actual, Cmp.eq == (expected & Cmp.eq))

    @parameterized.parameterized.expand(success_params)
    def test_ne_success(self, name, left, right, expected):
        actual = left != right
        self.assertEqual(actual, Cmp.ne == (expected & Cmp.ne))

    @parameterized.parameterized.expand(fail_params)
    def test_lt_fail(self, name, left, right, _):
        with self.assertRaises(TypeError):
            _ = left < right

    @parameterized.parameterized.expand(fail_params)
    def test_le_fail(self, name, left, right, _):
        with self.assertRaises(TypeError):
            _ = left <= right

    @parameterized.parameterized.expand(fail_params)
    def test_gt_fail(self, name, left, right, _):
        with self.assertRaises(TypeError):
            _ = left > right

    @parameterized.parameterized.expand(fail_params)
    def test_ge_fail(self, name, left, right, _):
        with self.assertRaises(TypeError):
            _ = left >= right

    @parameterized.parameterized.expand(fail_params)
    def test_eq_fail(self, name, left, right, _):
        actual = left == right
        self.assertFalse(actual)

    @parameterized.parameterized.expand(fail_params)
    def test_ne_nofail(self, name, left, right, _):
        actual = left != right
        self.assertTrue(actual)

    @parameterized.parameterized.expand(mistyped_params)
    def test_nontyped_compare(self, name, left, right, _):
        with self.assertRaises(TypeError):
            _ = left < right

    def test_no_name_clashes(self):
        with self.assertRaises(SqliteCachingException) as raised_context:

            class Q(LevelledEnum):
                Q = 0
                q = 1

        actual = raised_context.exception
        self.assertEqual(actual.category.id, EnumNameClashException.category_id)
        self.assertEqual(actual.cause.id, EnumNameClashException.id)

    def test_no_duplicate_values(self):
        with self.assertRaises(SqliteCachingException) as raised_context:

            class X(LevelledEnum):
                X = 0
                Y = 0

        actual = raised_context.exception
        self.assertEqual(actual.category.id, EnumDuplicateValueException.category_id)
        self.assertEqual(actual.cause.id, EnumDuplicateValueException.id)

    @parameterized.parameterized.expand(value_str_params)
    def test_value_strs(self, name, left, expected, _):
        actual = left.value_strs()
        self.assertEqual(actual, expected)

    @parameterized.parameterized.expand(convert_params)
    def test_convert(self, name, enum, to_convert, expected):
        actual = enum.convert(to_convert)
        self.assertEqual(actual, expected)

    def test_convert_fail(self):
        with self.assertRaises(SqliteCachingException) as raised_context:
            _ = TestEnumAB.convert("mismatch")
        actual = raised_context.exception
        self.assertEqual(actual.category.id, EnumValueConversionException.category_id)
        self.assertEqual(actual.cause.id, EnumValueConversionException.id)
