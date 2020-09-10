from sqlitecaching.enums import Level, LevelledEnum


class TestLevel(LevelledEnum):
    PRE_COMMIT = Level("pre-commit", 10)
    FULL = Level("full", 100)
