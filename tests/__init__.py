import functools
import unittest

import ordered_enum


class TestLevel(ordered_enum.OrderedEnum):
    PRE_COMMIT = "pre-commit"
    FULL = "full"

    @classmethod
    def convert(cls, value):
        for candidate in cls:
            if value == candidate.value:
                return candidate
        raise Exception(f"Unknown value provided: {cls}.convert({value})")

    @classmethod
    def values(cls):
        values = []
        for candidate in cls:
            values.append(candidate.value)
        return values


class __Config:
    __test_level = TestLevel.PRE_COMMIT

    @classmethod
    def set_test_level(cls, level):
        cls.__test_level = TestLevel.convert(level)

    @classmethod
    def get_test_level(cls):
        return cls.__test_level


def set_test_level(level):
    __Config.set_test_level(level)


def test_level(level):
    print(f"test_level: {level}")

    def decorator_test_level(func):
        print(f"decorator_test_level: {func}")
        print(f"decorator_test_level: {level}")
        print(f"decorator_test_level: {__Config.get_test_level()}")
        print(f"decorator_test_level: {__Config.get_test_level() < level}")

        @functools.wraps(func)
        @unittest.skipIf(
            __Config.get_test_level() < level,
            f"Skipping test configured at level {level}",
        )
        def wrap_test_level(*args, **kwargs):
            print(f"wrap_test_level: {func}")
            print(f"wrap_test_level: {level}")
            print(f"wrap_test_level: {__Config.get_test_level()}")
            print(f"wrap_test_level: {__Config.get_test_level() < level}")
            return func(*args, **kwargs)

        return wrap_test_level

    return decorator_test_level
