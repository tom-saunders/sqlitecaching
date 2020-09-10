import enum
import logging
import typing

log = logging.getLogger(__name__)


class Level(typing.NamedTuple):
    level_name: str
    level_value: int

    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self.level_value >= other.level_value
        return NotImplemented

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.level_value > other.level_value
        return NotImplemented

    def __le__(self, other):
        if self.__class__ is other.__class__:
            return self.level_value <= other.level_value
        return NotImplemented

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.level_value < other.level_value
        return NotImplemented


class LevelledEnum(enum.Enum):
    value: Level

    def __ge__(self, other: "LevelledEnum"):
        if self.__class__ is other.__class__:
            return self.value >= other.value
        return NotImplemented

    def __gt__(self, other: "LevelledEnum"):
        if self.__class__ is other.__class__:
            return self.value > other.value
        return NotImplemented

    def __le__(self, other: "LevelledEnum"):
        if self.__class__ is other.__class__:
            return self.value <= other.value
        return NotImplemented

    def __lt__(self, other: "LevelledEnum"):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented

    @classmethod
    def convert(cls, value):
        for candidate in cls:
            if value.casefold() == candidate.value.level_name.casefold():
                return candidate
        raise Exception(f"Unknown value provided: {cls}.convert({value})")

    @classmethod
    def values(cls):
        values = []
        for candidate in cls:
            values.append(candidate.value.level_name)
        return values


class LogLevel(LevelledEnum):
    NOTSET = Level("NOTSET", logging.NOTSET)
    DEBUG = Level("DEBUG", logging.DEBUG)
    INFO = Level("INFO", logging.INFO)
    WARNING = Level("WARNING", logging.WARNING)
    ERROR = Level("ERROR", logging.ERROR)
    CRITICAL = Level("CRITICAL", logging.CRITICAL)
