import enum
import logging


class LogLevel(enum.Enum):
    NOTSET = ("NOTSET", logging.NOTSET)
    DEBUG = ("DEBUG", logging.DEBUG)
    INFO = ("INFO", logging.INFO)
    WARNING = ("WARNING", logging.WARNING)
    ERROR = ("ERROR", logging.ERROR)
    CRITICAL = ("CRITICAL", logging.CRITICAL)

    @classmethod
    def convert(cls, value):
        for candidate in cls:
            if value.casefold() == candidate.value[0].casefold():
                return candidate
        raise Exception(f"Unknown value provided: {cls}.convert({value})")

    @classmethod
    def values(cls):
        values = []
        for candidate in cls:
            values.append(candidate.value[0])
        return values
