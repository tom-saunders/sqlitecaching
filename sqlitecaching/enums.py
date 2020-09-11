import enum
import functools
import logging

from sqlitecaching.exceptions import SqliteCachingException

log = logging.getLogger(__name__)


EnumException = SqliteCachingException.register_category(
    category_name=f"{__name__}.EnumException",
    category_id=3,
)

EnumDuplicateValueException = EnumException.register_cause(
    cause_name=f"{__name__}.EnumDuplicateValueException",
    cause_id=0,
    fmt=(
        "duplicated value [{duplicated_value}] found in [{enum_name}] "
        "duplicated between [{existing_name}] and  [{new_name}]"
    ),
    params=frozenset(
        [
            "duplicated_value",
            "enum_name",
            "existing_name",
            "new_name",
        ],
    ),
)
EnumNameClashException = EnumException.register_cause(
    cause_name=f"{__name__}.EnumNameClashException",
    cause_id=1,
    fmt=(
        "clashing name found in [{enum_name}], clash between [{existing_name}] "
        "and [{new_name}] (names are casefold()ed to [{casefolded_name}])"
    ),
    params=frozenset(
        [
            "enum_name",
            "existing_name",
            "new_name",
            "casefolded_name",
        ],
    ),
)
EnumValueConversionException = EnumException.register_cause(
    cause_name=f"{__name__}.EnumValueConversionException",
    cause_id=2,
    fmt=(
        "unable to convert input [{to_convert}] into a [{enum_name}] - no matching "
        "value found"
    ),
    params=frozenset(
        [
            "to_convert",
            "enum_name",
        ],
    ),
)


@functools.total_ordering
class LevelledEnum(enum.Enum):
    def __init__(self, *args):
        cls = self.__class__
        enum_name = cls.__name__
        matching_values = [
            existing.name for existing in cls if existing.value == self.value
        ]
        if matching_values:
            new_name = self.name
            existing_name = matching_values[0]
            raise EnumDuplicateValueException(
                params={
                    "enum_name": enum_name,
                    "new_name": new_name,
                    "existing_name": existing_name,
                    "duplicated_value": self.value,
                },
            )
        matching_names = [
            existing.name
            for existing in cls
            if existing.name.casefold() == self.name.casefold()
        ]
        if matching_names:
            new_name = self.name
            existing_name = matching_names[0]
            casefolded_name = new_name.casefold()
            raise EnumNameClashException(
                params={
                    "enum_name": enum_name,
                    "new_name": new_name,
                    "existing_name": existing_name,
                    "casefolded_name": casefolded_name,
                },
            )

    def __lt__(self, other: "LevelledEnum"):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented

    def __eq__(self, other):
        if self.__class__ is other.__class__:
            return self.value == other.value
        return NotImplemented

    @classmethod
    def convert(cls, value):
        for candidate in cls:
            if value.replace("-", "_").casefold() == candidate._name_.casefold():
                return candidate
        raise EnumValueConversionException(
            params={
                "enum_name": cls.__name__,
                "to_convert": value,
            },
        )

    @classmethod
    def value_strs(cls):
        values = frozenset([])
        for candidate in cls:
            values = values | frozenset([candidate._name_.replace("_", "-").casefold()])
        return values


class LogLevel(LevelledEnum):
    NOTSET = logging.NOTSET
    DEBUG = logging.DEBUG
    INFO = logging.INFO
    WARNING = logging.WARNING
    ERROR = logging.ERROR
    CRITICAL = logging.CRITICAL
