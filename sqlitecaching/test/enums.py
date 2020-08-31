import ordered_enum


class TestLevel(ordered_enum.OrderedEnum):
    PRE_COMMIT = "pre-commit"
    FULL = "full"

    @classmethod
    def convert(cls, value):
        for candidate in cls:
            if value.casefold() == candidate.value.casefold():
                return candidate
        raise Exception(f"Unknown value provided: {cls}.convert({value})")

    @classmethod
    def values(cls):
        values = []
        for candidate in cls:
            values.append(candidate.value)
        return values
