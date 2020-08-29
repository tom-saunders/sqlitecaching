import functools
import logging
import unittest

import sqlitecaching
from tests.enums import TestLevel, TestLogLevel


class Config:
    __test_level = TestLevel.PRE_COMMIT
    __parent_log = sqlitecaching.Config.get_log_base
    __output_dir = None
    __log_base = "tests"
    __log_dir = None
    __log_level = TestLogLevel.NOTSET

    @classmethod
    def setup_logging(cls):
        log_base = cls.get_log_base()
        log_dir = cls.__log_dir

        test_logger = logging.getLogger(log_base)
        test_logger.debug(
            f"Setting up logging for sqlitecaching tests using logger: {log_base}"
        )

        if log_dir:
            debug_log_path = f"{log_dir}/debug.log"
            test_log_path = f"{log_dir}/test.log"

            debug_handler = logging.FileHandler(test_log_path)
            debug_handler.setLevel(logging.DEBUG)
            test_logger.addHandler(debug_handler)

            test_handler = logging.FileHandler(debug_log_path)
            test_handler.setLevel(cls.get_log_level().value)
            test_logger.addHandler(test_handler)

        test_logger.debug(
            f"Set up logging for sqlitecaching tests using logger: {cls.get_log_base()}"
        )

    @classmethod
    def get_sub_logger(cls, ident):
        sub_logger_path = f"{cls.get_log_base()}.{ident}"
        sub_logger = logging.getLogger(sub_logger_path)
        return sub_logger

    @classmethod
    def set_test_level(cls, level):
        cls.__test_level = TestLevel.convert(level)

    @classmethod
    def get_test_level(cls):
        return cls.__test_level

    @classmethod
    def get_log_base(cls):
        return f"{cls.__parent_log()}.{cls.__log_base}"

    @classmethod
    def set_log_dir(cls, log_dir):
        cls.__log_dir = log_dir
        cls.setup_logging()

    @classmethod
    def set_log_level(cls, log_level):
        cls.__log_level = TestLogLevel.convert(log_level)
        cls.setup_logging()

    @classmethod
    def get_output_dir(cls):
        return cls.__output_dir

    @classmethod
    def set_output_dir(cls, output_dir):
        cls.output_dir = output_dir


_logger = Config.get_sub_logger("__init__")


def test_level(level):
    _logger.debug(f"test_level: {level}")

    def decorator_test_level(func):
        _logger.debug(f"decorator_test_level: {func}")
        _logger.debug(f"decorator_test_level: {level}")
        _logger.debug(f"decorator_test_level: {Config.get_test_level()}")
        _logger.debug(f"decorator_test_level: {Config.get_test_level() < level}")

        @functools.wraps(func)
        @unittest.skipIf(
            Config.get_test_level() < level,
            f"Skipping test configured at level {level}",
        )
        def wrap_test_level(*args, **kwargs):
            _logger.debug(f"wrap_test_level: {func}")
            _logger.debug(f"wrap_test_level: {level}")
            _logger.debug(f"wrap_test_level: {Config.get_test_level()}")
            _logger.debug(f"wrap_test_level: {Config.get_test_level() < level}")
            return func(*args, **kwargs)

        return wrap_test_level

    return decorator_test_level
