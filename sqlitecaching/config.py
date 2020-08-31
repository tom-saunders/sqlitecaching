import logging
import time

from sqlitecaching.enums import LogLevel

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


class UTCFormatter(logging.Formatter):
    def __init__(self, *, fmt=None, datefmt=None):
        if not fmt:
            fmt = (
                "%(asctime)s %(levelname)-4.4s: %(funcName)16s: %(message)s "
                "- [%(name)s]"
            )
        if not datefmt:
            datefmt = "%Y-%m-%dT%H:%M:%S%z"
        super().__init__(fmt, datefmt)

    converter = time.gmtime


class Config:
    def __init__(
        self, *, logger_level=LogLevel.WARNING, log_output=None, debug_output=None,
    ):
        self.logger_level = logger_level
        self.log_output = log_output
        self.debug_output = debug_output

        # used to allow removal of configured handlers
        # without this we end up with duplicate settings
        self._log_handlers = []
        self._setup_logging()

    def _setup_logging(self):
        log.info("(re)setting up logger: %s", log.name)
        log.info("setting logger %s level to %s", log.name, self.logger_level)
        log.setLevel(self.logger_level.value[1])

        if self._log_handlers:
            log.debug(
                "clean up previously configured handlers for logger %s", log.name,
            )
            for handler in self._log_handlers:
                log.debug("remove handler: %s", handler)
                log.removeHandler(handler)

        self._log_handlers = []

        if self.log_output:
            log_path = self.log_output[0]
            log_level = self.log_output[1]

            if self.logger_level > log_level:
                log.warn(
                    (
                        "configuring log_handler at level %s for logger %s "
                        "which has logger_level: %s which will not log "
                        "additional output"
                    ),
                    log_level,
                    log.name,
                    self.logger_level,
                )

            log_handler = logging.FileHandler(log_path)
            log_handler.setLevel(log_level.value[1])

            log_formatter = UTCFormatter()
            log_handler.setFormatter(log_formatter)

            log.addHandler(log_handler)
            self._log_handlers.append(log_handler)

            log.debug("configured log_handler: %s", log_handler)

        if self.debug_output:
            debug_path = self.debug_output[0]
            debug_level = self.debug_output[1]

            if self.logger_level > debug_level:
                log.warn(
                    (
                        "configuring debug_handler at level %s for logger %s "
                        "which has logger_level: %s which will not log "
                        "additional output"
                    ),
                    debug_level,
                    log.name,
                    self.logger_level,
                )

            debug_handler = logging.FileHandler(debug_path)
            debug_handler.setLevel(debug_level.value[1])

            debug_format = (
                "%(asctime)s %(levelname)-4.4s: %(funcName)16s: %(message)s "
                "- [%(name)s] [%(filename)s:%(lineno)d]"
            )
            debug_formatter = UTCFormatter(fmt=debug_format)
            debug_handler.setFormatter(debug_formatter)

            log.addHandler(debug_handler)
            self._log_handlers.append(debug_handler)

            log.debug(
                "configured debug file_handler: %s", debug_handler,
            )

        log.debug("(re)set up logger: %s", log.name)

    def get_sub_logger(self, sub_ident):
        if sub_ident.startswith(f"{log.name}."):
            ident = sub_ident
        else:
            ident = f"{log.name}.{sub_ident}"

        sub_logger = logging.getLogger(ident)
        sub_logger.info("got sub_logger: %s", ident)

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
