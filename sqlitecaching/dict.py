import enum
import logging
import sqlite3
from collections import UserDict

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


class Mode(enum.Enum):
    RO = "mode=ro"
    RW = "mode=rw"
    RWC = "mode=rwc"
    MEM = "mode=memory"


class CacheDict(UserDict):
    __internally_constructed = object()

    default_mapping = {}

    def __init__(self, *args, conn, mapping, log_name, **kwargs):
        super().__init__(*args, **kwargs)

        internally_constructed = kwargs.get("_cd_internal_flag", None)
        if not internally_constructed:
            log.warn(
                "direct construction of %s may lead to unexpected behaviours",
                type(self).__name__,
            )
        elif internally_constructed is not type(self).__internally_constructed:
            log.warn(
                (
                    "(invalid __internal_constructed?) direct construction of %s "
                    "may lead to unexpected behaviours"
                ),
                type(self).__name__,
            )

        self.conn = conn
        self.mapping = mapping

        if log_name:
            log.warn("using caller provided logger: %s", log_name)
            self.log = logging.getLogger(log_name)
            self.log.addHandler(logging.NullLogger())
            self.log.info("using caller provided logger: %s", log_name)
        else:
            self.log = log

    # Not including
    # "detect_types",
    # as that should be dependant on mapping(?)
    _passthrough_params = [
        "timeout",
        "isolation_level",
        "factory",
        "cached_statements",
    ]

    @classmethod
    def _cleanup_sqlite_params(cls, *, sqlite_params):
        if not sqlite_params:
            return {}

        cleaned_params = {}
        for (key, value) in sqlite_params.items():
            if key in cls._passthrough_params:
                if value:
                    cleaned_params[key] = value
                else:
                    pass
            else:
                pass
        return cleaned_params

    _anon_mem_path = ":memory:"

    @classmethod
    def open_anon_memory(cls, *, mapping=None, sqlite_params=None, log_name=None):
        log.info("open anon memory")

        cleaned_sqlite_params = cls._cleanup_sqlite_params(sqlite_params=sqlite_params)

        conn = sqlite3.connect(cls._anon_mem_path, **cleaned_sqlite_params)

        return CacheDict(
            conn=conn,
            mapping=mapping,
            log_name=log_name,
            _cd_internal_flag=cls.__internally_constructed,
        )

    _anon_disk_path = ""

    @classmethod
    def open_anon_disk(cls, *, mapping=None, sqlite_params=None, log_name=None):
        log.info("open anon disk")
        cleaned_sqlite_params = cls._cleanup_sqlite_params(sqlite_params=sqlite_params)

        conn = sqlite3.connect(cls._anon_disk_path, **cleaned_sqlite_params)

        return CacheDict(
            conn=conn,
            mapping=mapping,
            log_name=log_name,
            _cd_internal_flag=cls.__internally_constructed,
        )
        return CacheDict(
            conn=conn,
            mapping=mapping,
            log_name=log_name,
            _cd_internal_flag=cls.__internally_constructed,
        )

    @classmethod
    def open_readonly(cls, *, path, mapping=None, sqlite_params=None, log_name=None):
        log.info("open readonly")
        cleaned_sqlite_params = cls._cleanup_sqlite_params(sqlite_params=sqlite_params)

        uri_path = f"file:{path}?mode=ro"
        conn = sqlite3.connect(uri_path, uri=True, **cleaned_sqlite_params)

        return CacheDict(
            conn=conn,
            mapping=mapping,
            log_name=log_name,
            _cd_internal_flag=cls.__internally_constructed,
        )

    @classmethod
    def open_readwrite(
        cls, *, path, mapping=None, create=False, sqlite_params=None, log_name=None
    ):
        log.info("open readwrite create: %s", create)
        cleaned_sqlite_params = cls._cleanup_sqlite_params(sqlite_params=sqlite_params)

        if create:
            uri_path = f"file:{path}?mode=rwc"
        else:
            uri_path = f"file:{path}?mode=rw"
        conn = sqlite3.connect(uri_path, uri=True, **cleaned_sqlite_params)
        return CacheDict(
            conn=conn,
            mapping=mapping,
            log_name=log_name,
            _cd_internal_flag=cls.__internally_constructed,
        )

    @classmethod
    def create_from_conn(cls, *, conn, mapping=None, log_name=None):
        log.info("create from existing connection")
        return CacheDict(
            conn=conn,
            mapping=mapping,
            log_name=log_name,
            _cd_internal_flag=cls.__internally_constructed,
        )
