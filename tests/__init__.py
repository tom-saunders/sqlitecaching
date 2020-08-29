import functools
import unittest

import sqlitecaching
from sqlitecaching.config import Config as BaseConfig
from tests.enums import TestLevel


class Config(BaseConfig):
    _test_level = TestLevel.PRE_COMMIT
    _output_dir = None

    def __init__(
        self, *args, test_level=TestLevel.PRE_COMMIT, output_dir=None, **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._test_level = test_level
        self._output_dir = output_dir

    def set_test_level(self, level):
        self._test_level = TestLevel.convert(level)

    def get_test_level(self):
        return self._test_level

    def get_output_dir(self):
        return self._output_dir

    def set_output_dir(self, output_dir):
        self._output_dir = output_dir


config = Config(log_ident="tests", parent_config=sqlitecaching.config, debug_log=True)
_logger = config.get_sub_logger("__init__")


def test_level(level):
    _logger.debug(f"test_level: {level}")

    def decorator_test_level(func):
        _logger.debug(f"decorator_test_level: {func}")
        _logger.debug(f"decorator_test_level: {level}")
        _logger.debug(f"decorator_test_level: {config.get_test_level()}")
        _logger.debug(f"decorator_test_level: {config.get_test_level() < level}")

        @functools.wraps(func)
        @unittest.skipIf(
            config.get_test_level() < level,
            f"Skipping test configured at level {level}",
        )
        def wrap_test_level(*args, **kwargs):
            _logger.debug(f"wrap_test_level: {func}")
            _logger.debug(f"wrap_test_level: {level}")
            _logger.debug(f"wrap_test_level: {config.get_test_level()}")
            _logger.debug(f"wrap_test_level: {config.get_test_level() < level}")
            return func(*args, **kwargs)

        return wrap_test_level

    return decorator_test_level
