class Config:
    __log_base = "sqlitecaching"

    @classmethod
    def get_log_base(cls):
        return cls.__log_base
