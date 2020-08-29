import logging

from sqlitecaching.enums import LogLevel


class MinimalConfig:
    def __init__(self,):
        self._child_configs = []

    def add_child_config(self, child):
        self._child_configs.append(child)

    def remove_child_config(self, child):
        self._child_configs.remove(child)

    def get_log_name(self):
        return None


class Config(MinimalConfig):
    def __init__(
        self,
        *args,
        log_ident,
        log_file_name,
        output_dir=None,
        log_dir=None,
        log_level=LogLevel.NOTSET,
        debug_log=False,
        parent_config=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._log_dir = log_dir
        self._log_ident = log_ident
        self._log_level = log_level
        self._log_file_name = log_file_name
        self._debug_log = debug_log
        self._parent_config = parent_config
        self._log_handlers = None

        if self._parent_config:
            self._parent_config.add_child_config(self)
        self.setup_logging()

    def setup_logging(self):
        log_dir = self._log_dir
        log_name = self.get_log_name()

        logger = logging.getLogger(log_name)
        logger.info(f"Setting up logger: {log_name}")

        if self._log_handlers:
            logger.debug(
                f"clean up previously configured handlers: {self._log_handlers}"
            )
            (last_logger, handlers) = self._log_handlers
            for handler in handlers:
                logger.debug(f"remove handler: {handler}")
                last_logger.removeHandler(handler)

        handlers = []
        if log_dir:
            log_path = f"{log_dir}/{self._log_file_name}.log"
            handler = logging.FileHandler(log_path)
            handler.setLevel(self._log_level.value[1])
            logger.addHandler(handler)
            handlers.append(handler)
            logger.debug(f"configured main handler: {handler}")

            if self._debug_log:
                debug_log_path = f"{log_dir}/{self._log_file_name}.debug.log"
                debug_handler = logging.FileHandler(debug_log_path)
                debug_handler.setLevel(logging.DEBUG)
                logger.addHandler(debug_handler)
                handlers.append(debug_handler)
                logger.debug(f"configured debug handler: {debug_handler}")
        self._log_handlers = (logger, handlers)

        logger.debug(f"Set up logging using logger: {log_name}")
        self.logger = logger

    def get_sub_logger(self, ident):
        sub_logger_path = f"{self.get_log_name()}.{ident}"
        sub_logger = logging.getLogger(sub_logger_path)
        sub_logger.setLevel(self._log_level.value[1])
        return sub_logger

    def get_log_name(self):
        if self._parent_config:
            parent_log_name = self._parent_config.get_log_name()
            if parent_log_name:
                return f"{parent_log_name}.{self._log_ident}"

        return self._log_ident

    def set_log_dir(self, log_dir):
        self._log_dir = log_dir
        self.setup_logging()

    def set_log_level(self, log_level):
        self._log_level = LogLevel.convert(log_level)
        self.setup_logging()

    def set_parent_config(self, parent_config):
        if self._parent_config:
            self._parent_config.remove_child_config(self)
        parent_config.add_child_config(self)
        self._parent_config = parent_config

        for child in self._child_configs:
            child.setup_logging()

        self.setup_logging()
