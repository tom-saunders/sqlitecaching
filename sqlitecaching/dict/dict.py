import logging
import sqlite3
from collections import UserDict

from sqlitecaching.exceptions import SqliteCachingException

log = logging.getLogger(__name__)

CacheDictException = SqliteCachingException.register_type(
    type_name=f"{__name__}.CacheDictException", type_id=1
)
__CDE = CacheDictException


class CacheDict(UserDict):
    __internally_constructed = object()

    _ANON_MEM_PATH = ":memory:"
    _ANON_DISK_PATH = ""

    # Not including
    # "detect_types",
    # as that should be dependant on mapping(?)
    _PASSTHROUGH_PARAMS = [
        "timeout",
        "isolation_level",
        "factory",
        "cached_statements",
    ]

    def __init__(self, *, conn, mapping, log_name, **kwargs):
        # Don't populate anything by passing **kwargs here.
        super().__init__()

        internally_constructed = kwargs.get("_cd_internal_flag", None)
        if (not internally_constructed) or (
            internally_constructed is not type(self).__internally_constructed
        ):
            log.warn(
                "direct construction of [%s] may lead to unexpected behaviours",
                type(self).__name__,
            )

        self.conn = conn
        self.mapping = mapping

        if log_name:
            log.warn("using caller provided logger: [%s]", log_name)
            self.log = logging.getLogger(log_name)
            self.log.info("using caller provided logger: [%s]", log_name)
        else:
            self.log = log

    @classmethod
    def _cleanup_sqlite_params(cls, *, sqlite_params):
        if not sqlite_params:
            return {}

        cleaned_params = {}
        for (param, value) in sqlite_params.items():
            if param in cls._PASSTHROUGH_PARAMS:
                if value:
                    cleaned_params[param] = value
                else:
                    log.debug("sqlite parameter [%s] present but no value", param)
            else:
                log.warn(
                    (
                        "unsupported (by sqlitecaching) sqlite parameter [%s] "
                        "found with value [%s] - removing"
                    ),
                    param,
                    value,
                )
        return cleaned_params

    @classmethod
    def open_anon_memory(cls, *, mapping=None, sqlite_params=None, log_name=None):
        log.info("open anon memory")

        cleaned_sqlite_params = cls._cleanup_sqlite_params(sqlite_params=sqlite_params)

        conn = sqlite3.connect(cls._ANON_MEM_PATH, **cleaned_sqlite_params)

        return CacheDict(
            conn=conn,
            mapping=mapping,
            log_name=log_name,
            _cd_internal_flag=cls.__internally_constructed,
        )

    @classmethod
    def open_anon_disk(cls, *, mapping=None, sqlite_params=None, log_name=None):
        log.info("open anon disk")
        cleaned_sqlite_params = cls._cleanup_sqlite_params(sqlite_params=sqlite_params)

        conn = sqlite3.connect(cls._ANON_DISK_PATH, **cleaned_sqlite_params)

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
        log.info("open readwrite create: [%s]", create)
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
    def _create_from_conn(cls, *, conn, mapping=None, log_name=None):
        log.warn(
            "creating CacheDict from existing connection may lead to "
            "unexpected behaviour"
        )
        return CacheDict(
            conn=conn,
            mapping=mapping,
            log_name=log_name,
            _cd_internal_flag=cls.__internally_constructed,
        )
