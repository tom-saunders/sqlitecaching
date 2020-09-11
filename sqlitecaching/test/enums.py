import enum

from sqlitecaching.enums import LevelledEnum


@enum.unique
class TestLevel(LevelledEnum):
    PRE_COMMIT = 10
    FULL = 100
