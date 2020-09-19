import logging
import sqlite3
import typing
from collections import UserDict

from sqlitecaching.dict.mapping import CacheDictMapping
from sqlitecaching.exceptions import SqliteCachingException

log = logging.getLogger(__name__)

CacheDictCategory = SqliteCachingException.register_category(
    category_name=f"{__name__}.CacheDictCategory",
    category_id=1,
)
__CDC = CacheDictCategory


class CacheDict(UserDict):
    __internally_constructed: typing.ClassVar[typing.Any] = object()

    _ANON_MEM_PATH: typing.ClassVar[str] = ":memory:"
    _ANON_DISK_PATH: typing.ClassVar[str] = ""

    # Not including
    # "detect_types",
    # as that should be dependant on mapping(?)
    _PASSTHROUGH_PARAMS: typing.ClassVar[typing.List[str]] = [
        "timeout",
        "isolation_level",
        "factory",
        "cached_statements",
    ]

    conn: sqlite3.Connection
    mapping: CacheDictMapping

    def __init__(
        self,
        *,
        conn: sqlite3.Connection,
        mapping: CacheDictMapping,
        log_name: typing.Optional[str],
        **kwargs,
    ):
        # Don't populate anything by passing **kwargs here.
        # We only take **kwargs to attempt to note the fact that we're called
        # in an unexpected way.
        super().__init__()

        self.__is_internally_constructed(**kwargs)

        self.conn = conn
        self.mapping = mapping

        if log_name:
            log.warning("using caller provided logger: [%s]", log_name)
            self.log = logging.getLogger(log_name)
            self.log.info("using caller provided logger: [%s]", log_name)
        else:
            self.log = log

    @classmethod
    def __is_internally_constructed(
        cls,
        *,
        _cd_internal_flag: typing.Optional[typing.Any] = None,
    ) -> None:
        if (not _cd_internal_flag) or (
            _cd_internal_flag is not cls.__internally_constructed
        ):
            log.warning(
                "direct construction of [%s] may lead to unexpected behaviours",
                cls.__name__,
            )

    @classmethod
    def _cleanup_sqlite_params(
        cls,
        *,
        sqlite_params: typing.Mapping[str, typing.Any],
    ) -> typing.Mapping[str, typing.Any]:
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
                log.warning(
                    (
                        "unsupported (by sqlitecaching) sqlite parameter [%s] "
                        "found with value [%s] - removing"
                    ),
                    param,
                    value,
                )
        return cleaned_params

    @classmethod
    def open_anon_memory(
        cls,
        *,
        mapping: CacheDictMapping,
        sqlite_params: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        log_name: typing.Optional[str] = None,
    ) -> "CacheDict":
        log.info("open anon memory")

        if sqlite_params:
            cleaned_sqlite_params = cls._cleanup_sqlite_params(
                sqlite_params=sqlite_params,
            )
        else:
            cleaned_sqlite_params = {}

        conn = sqlite3.connect(cls._ANON_MEM_PATH, **cleaned_sqlite_params)

        return CacheDict(
            conn=conn,
            mapping=mapping,
            log_name=log_name,
            _cd_internal_flag=cls.__internally_constructed,
        )

    @classmethod
    def open_anon_disk(
        cls,
        *,
        mapping=None,
        sqlite_params=None,
        log_name=None,
    ) -> "CacheDict":
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
    def open_readonly(
        cls,
        *,
        path,
        mapping=None,
        sqlite_params=None,
        log_name=None,
    ) -> "CacheDict":
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
        cls,
        *,
        path,
        mapping=None,
        create=False,
        sqlite_params=None,
        log_name=None,
    ) -> "CacheDict":
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
    def _create_from_conn(cls, *, conn, mapping=None, log_name=None) -> "CacheDict":
        log.warning(
            "creating CacheDict from existing connection may lead to "
            "unexpected behaviour",
        )
        return CacheDict(
            conn=conn,
            mapping=mapping,
            log_name=log_name,
            _cd_internal_flag=cls.__internally_constructed,
        )
