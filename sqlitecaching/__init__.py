class Config:
    __log_parent = None
    __log_base = "sqlitecaching"

    @classmethod
    def setup_logging(cls):
        pass

    @classmethod
    def get_log_base(cls):
        if cls.__log_parent:
            return f"{cls.__log_parent}.{cls.__log_base}"
        else:
            return cls.__log_base

    @classmethod
    def set_log_parent(cls, log_parent):
        cls.__log_parent = log_parent
        cls.setup_logging()
