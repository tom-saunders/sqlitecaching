import logging
import time

from sqlitecaching.enums import LogLevel


class UTCFormatter(logging.Formatter):
    converter = time.gmtime


class Config:
    def __init__(
        self,
        *,
        log_ident,
        logger_level=LogLevel.WARNING,
        log_output=None,
        debug_output=None,
    ):
        self.log_ident = log_ident
        self.logger_level = logger_level
        self.log_output = log_output
        self.debug_output = debug_output

        self.logger = logging.getLogger(self.log_ident)
        self.logger.addHandler(logging.NullHandler())

        # used to allow removal of configured handlers
        # without this we end up with duplicate settings
        self._log_handlers = []
        self._setup_logging()

    def _setup_logging(self):
        self.logger.info("(re)setting up logger: %s", self.log_ident)
        self.logger.info(
            "setting logger %s level to %s", self.log_ident, self.logger_level
        )
        self.logger.setLevel(self.logger_level.value[1])

        if self._log_handlers:
            self.logger.debug(
                "clean up previously configured handlers for logger %s", self.log_ident,
            )
            for handler in self._log_handlers:
                self.logger.debug("remove handler: %s", handler)
                self.logger.removeHandler(handler)

        self._log_handlers = []

        if self.log_output:
            log_path = self.log_output[0]
            log_level = self.log_output[1]

            if self.logger_level > log_level:
                self.logger.warn(
                    (
                        "configuring log_handler at level %s for logger %s "
                        "which has logger_level: %s which will not log "
                        "additional output"
                    ),
                    log_level,
                    self.log_ident,
                    self.logger_level,
                )

            log_handler = logging.FileHandler(log_path)
            log_handler.setLevel(log_level.value[1])

            log_format = (
                "%(asctime)s %(levelname)s [%(name)s] %(funcName)s"
                "[%(filename)s:%(lineno)d] -  %(message)s"
            )
            log_formatter = UTCFormatter(log_format)
            log_handler.setFormatter(log_formatter)

            self.logger.addHandler(log_handler)
            self._log_handlers.append(log_handler)

            self.logger.debug("configured log_handler: %s", log_handler)

        if self.debug_output:
            debug_path = self.debug_output[0]
            debug_level = self.debug_output[1]

            if self.logger_level > debug_level:
                self.logger.warn(
                    (
                        "configuring debug_handler at level %s for logger %s "
                        "which has logger_level: %s which will not log "
                        "additional output"
                    ),
                    debug_level,
                    self.log_ident,
                    self.logger_level,
                )

            debug_handler = logging.FileHandler(debug_path)
            debug_handler.setLevel(debug_level.value[1])

            debug_format = (
                "%(asctime)s %(levelname)s [%(name)s] %(funcName)s"
                "[%(filename)s:%(lineno)d] - %(message)s"
            )
            debug_formatter = UTCFormatter(debug_format)
            debug_handler.setFormatter(debug_formatter)

            self.logger.addHandler(debug_handler)
            self._log_handlers.append(debug_handler)

            self.logger.debug(
                "configured debug file_handler: %s", debug_handler,
            )

        self.logger.debug("(re)set up logger: %s", self.log_ident)

    def get_sub_logger(self, ident):
        sub_ident = f"{self.log_ident}.{ident}"
        sub_logger = logging.getLogger(sub_ident)
        self.logger.info("get sub_logger: %s", sub_ident)
        sub_logger.setLevel(logging.NOTSET)
        return sub_logger

    def set_log_output(self, log_output):
        self.log_output = log_output
        self._setup_logging()

    def set_debug_output(self, debug_output):
        self.debug_output = debug_output
        self._setup_logging()

    def set_logger_level(self, logger_level):
        self.logger_level = logger_level
        self._setup_logging()
