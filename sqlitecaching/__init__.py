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
    @classmethod
    def set_test_level(cls, level):
        cls.__test_level = TestLevel.convert(level)

    @classmethod
    def get_test_level(cls):
        return cls.__test_level


def set_test_level(level):
    __Config.set_test_level(level)


def get_test_level():
    return __Config.get_test_level()
