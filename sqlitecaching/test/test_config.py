import logging
from unittest.mock import patch

from sqlitecaching.config import Config
from sqlitecaching.enums import LogLevel
from sqlitecaching.test import SqliteCachingTestBase, TestLevel, test_level

log = logging.getLogger(__name__)


# These tests are not great. Not sure if that's just because I
# don't know how to test them properly (likely) or i


@test_level(TestLevel.PRE_COMMIT)
class TestSqliteCachingConfig(SqliteCachingTestBase):
    def test_log_handler_warn_no_output(self):
        log_path = "log_file"
        debug_path = "debug_file"
        with patch("logging.FileHandler") as file_handler, patch(
            "sqlitecaching.config.log",
        ) as config_log:
            mock_conf = {"return_value.level": logging.WARNING}
            file_handler.configure_mock(**mock_conf)
            c = Config(log_output=(log_path, LogLevel.DEBUG))
            file_handler.assert_called_once_with(log_path)
            config_log.warning.assert_called_once()

            c.set_debug_output((debug_path, LogLevel.DEBUG))
            config_log.removeHandler.assert_called_once_with(file_handler.return_value)

    def test_log_handler_output(self):
        log_path = "log_file"
        with patch("logging.FileHandler") as file_handler, patch(
            "sqlitecaching.config.log",
        ) as config_log:
            mock_conf = {"return_value.level": logging.DEBUG}
            file_handler.configure_mock(**mock_conf)
            c = Config(
                logger_level=LogLevel.DEBUG,
                log_output=(log_path, LogLevel.DEBUG),
            )
            file_handler.assert_called_once_with(log_path)
            config_log.warning.assert_not_called()

            c.set_logger_level(LogLevel.DEBUG)
            config_log.removeHandler.assert_called_once_with(file_handler.return_value)

    def test_debug_handler_warn_no_output(self):
        log_path = "log_file"
        debug_path = "debug_file"
        with patch("logging.FileHandler") as file_handler, patch(
            "sqlitecaching.config.log",
        ) as config_log:
            mock_conf = {"return_value.level": logging.WARNING}
            file_handler.configure_mock(**mock_conf)
            c = Config(debug_output=(debug_path, LogLevel.DEBUG))
            file_handler.assert_called_once_with(debug_path)
            config_log.warning.assert_called_once()

            c.set_log_output((log_path, LogLevel.DEBUG))

            config_log.removeHandler.assert_called_once_with(file_handler.return_value)

    def test_debug_handler_output(self):
        debug_path = "debug_file"
        with patch("logging.FileHandler") as file_handler, patch(
            "sqlitecaching.config.log",
        ) as config_log:
            mock_conf = {"return_value.level": logging.DEBUG}
            file_handler.configure_mock(**mock_conf)
            c = Config(
                logger_level=LogLevel.DEBUG,
                debug_output=(debug_path, LogLevel.DEBUG),
            )
            file_handler.assert_called_once_with(debug_path)
            config_log.warning.assert_not_called()

            c.set_logger_level(LogLevel.DEBUG)
            config_log.removeHandler.assert_called_once_with(file_handler.return_value)

    def test_no_handler(self):
        debug_path = "debug_file"
        with patch("logging.FileHandler") as file_handler:
            c = Config()
        file_handler.assert_not_called()

        with patch("logging.FileHandler") as file_handler:
            mock_conf = {"return_value.level": logging.WARNING}
            file_handler.configure_mock(**mock_conf)
            c.set_debug_output((debug_path, LogLevel.DEBUG))

        file_handler.assert_called_once_with(debug_path)
