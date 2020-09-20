import enum
import logging
import sqlite3
import typing
import weakref
from collections import UserDict

from sqlitecaching.dict.mapping import CacheDictMapping
from sqlitecaching.exceptions import SqliteCachingException

log = logging.getLogger(__name__)

CacheDictCategory = SqliteCachingException.register_category(
    category_name=f"{__name__}.CacheDictCategory",
    category_id=1,
)
__CDC = CacheDictCategory

CacheDictFilteredSqliteParamsException = __CDC.register_cause(
    cause_name=f"{__name__}.CacheDictFilteredSqliteParamsException",
    cause_id=0,
    fmt="sqlite_params provided to CacheDict contained unsupported keys: [{filtered}]",
    params=frozenset(["filtered"]),
)

CacheDictReadOnlyException = __CDC.register_cause(
    cause_name=f"{__name__}.CacheDictReadOnlyException",
    cause_id=1,
    fmt="attempting to perform [{op}] on readonly table [{table}]",
    params=frozenset(["op", "table"]),
)


@enum.unique
class ToCreate(enum.Enum):
    NONE = enum.auto()
    DATABASE = enum.auto()


class CacheDict(UserDict):
    _internally_constructed: typing.ClassVar[typing.Any] = object()
    _raise_on_filtered_sqlite_params: typing.ClassVar[bool] = False

    ANON_MEM_PATH: typing.ClassVar[str] = ":memory:"
    ANON_DISK_PATH: typing.ClassVar[str] = ""

    PASSTHROUGH_PARAMS: typing.ClassVar[typing.List[str]] = [
        "timeout",
        "detect_types",
        "isolation_level",
        "factory",
        "cached_statements",
    ]

    conn: sqlite3.Connection
    mapping: CacheDictMapping
    read_only: bool
    _finalize: weakref.finalize

    def __init__(
        self,
        *,
        conn: sqlite3.Connection,
        mapping: CacheDictMapping,
        read_only: bool = False,
        **kwargs,
    ):
        # Don't populate anything by passing **kwargs here.
        # We only take **kwargs to attempt to note the fact that we're called
        # in an unexpected way.
        super().__init__()

        self._finalize = weakref.finalize(
            self,
            self.__class__._finalize_instance,
            id=id(self),
            conn=conn,
        )

        self._is_internally_constructed(**kwargs)

        self.conn = conn
        self.mapping = mapping
        self.read_only = read_only

        if not read_only:
            # creating the mapped table should be idempotent
            # (CREATE TABLE ... IF NOT EXIST ...)
            # but obviously doesn't work for readonly connections
            self.create_table()

        log.info("created [%#0x] conn: [%s]", id(self), self.conn)

    def create_table(self) -> None:
        if self.read_only:
            raise CacheDictReadOnlyException(
                {
                    "op": "create",
                    "table": self.mapping.table_ident,
                },
            )
        log.debug("create table [%#0x]")
        create_stmt = self.mapping.create_statement()
        cursor = self.conn.execute(create_stmt)
        cursor.fetchone()

    def close(self) -> None:
        log.info(
            "closing [%#0x] conn: [%s]",
            id(self),
            self.conn,
        )
        self._finalize()

    @classmethod
    def _finalize_instance(cls, *, id: int, conn: sqlite3.Connection) -> None:
        log.info("_finalize_instance [%#0x] conn: [%s]", id, conn)
        try:
            conn.close()
        except Exception:
            log.info("exception when closing conn: [%s]", conn, exc_info=True)

    @classmethod
    def _is_internally_constructed(
        cls,
        *,
        _cd_internal_flag: typing.Optional[typing.Any] = None,
    ) -> None:
        if (not _cd_internal_flag) or (
            _cd_internal_flag is not cls._internally_constructed
        ):
            log.warning(
                "direct construction of [%s] may lead to unexpected behaviours",
                cls.__name__,
            )

    @classmethod
    def raise_on_filtered_sqlite_params(
        cls,
        should_raise: typing.Optional[bool] = None,
        /,
    ) -> bool:
        if should_raise is not None:
            log.warning(
                "setting [%s]._raise_on_filtered_sqlite_params to [%s]",
                cls.__name__,
                should_raise,
            )
            cls._raise_on_filtered_sqlite_params = should_raise
        return cls._raise_on_filtered_sqlite_params

    @classmethod
    def _cleanup_sqlite_params(
        cls,
        sqlite_params: typing.Optional[typing.Mapping[str, typing.Any]],
        /,
    ) -> typing.Mapping[str, typing.Any]:
        if not sqlite_params:
            return {}

        filtered_params: typing.FrozenSet[str] = frozenset([])
        cleaned_params = {}
        for (param, value) in sqlite_params.items():
            if param in cls.PASSTHROUGH_PARAMS:
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
                filtered_params = filtered_params | frozenset([param])
        if cls.raise_on_filtered_sqlite_params():
            log.info("raising for filtered sqlite params")
            raise CacheDictFilteredSqliteParamsException({"filtered": filtered_params})
        return cleaned_params

    @classmethod
    def _open_connection(
        cls,
        **kwargs,
    ) -> sqlite3.Connection:
        conn = sqlite3.connect(**kwargs)
        conn.row_factory = sqlite3.Row
        return conn

    @classmethod
    def open_anon_memory(
        cls,
        *,
        mapping: CacheDictMapping,
        sqlite_params: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    ) -> "CacheDict":
        log.info("open anon memory")

        cleaned_sqlite_params = cls._cleanup_sqlite_params(sqlite_params)

        conn = cls._open_connection(database=cls.ANON_MEM_PATH, **cleaned_sqlite_params)

        cache_dict = CacheDict(
            conn=conn,
            mapping=mapping,
            _cd_internal_flag=cls._internally_constructed,
        )

        return cache_dict

    @classmethod
    def open_anon_disk(
        cls,
        *,
        mapping: CacheDictMapping,
        sqlite_params: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    ) -> "CacheDict":
        log.info("open anon disk")
        cleaned_sqlite_params = cls._cleanup_sqlite_params(sqlite_params)

        conn = cls._open_connection(
            database=cls.ANON_DISK_PATH,
            **cleaned_sqlite_params,
        )

        cache_dict = CacheDict(
            conn=conn,
            mapping=mapping,
            _cd_internal_flag=cls._internally_constructed,
        )

        # TODO create table
        return cache_dict

    @classmethod
    def open_readonly(
        cls,
        *,
        path: str,
        mapping: CacheDictMapping,
        sqlite_params: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    ) -> "CacheDict":
        log.info("open readonly")
        cleaned_sqlite_params = cls._cleanup_sqlite_params(sqlite_params)

        uri_path = f"file:{path}?mode=ro"
        conn = cls._open_connection(
            database=uri_path,
            uri=True,
            **cleaned_sqlite_params,
        )

        cache_dict = CacheDict(
            conn=conn,
            mapping=mapping,
            read_only=True,
            _cd_internal_flag=cls._internally_constructed,
        )

        return cache_dict

    @classmethod
    def open_readwrite(
        cls,
        *,
        path: str,
        mapping: CacheDictMapping,
        create: typing.Optional[ToCreate] = ToCreate.NONE,
        sqlite_params: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    ) -> "CacheDict":
        log.info("open readwrite create: [%s]", create)
        cleaned_sqlite_params = cls._cleanup_sqlite_params(sqlite_params)

        if create == ToCreate.DATABASE:
            uri_path = f"file:{path}?mode=rwc"
        else:
            uri_path = f"file:{path}?mode=rw"
        conn = cls._open_connection(
            database=uri_path,
            uri=True,
            **cleaned_sqlite_params,
        )

        cache_dict = CacheDict(
            conn=conn,
            mapping=mapping,
            _cd_internal_flag=cls._internally_constructed,
        )

        return cache_dict

    @classmethod
    def _create_from_conn(
        cls,
        *,
        conn: sqlite3.Connection,
        mapping: CacheDictMapping,
    ) -> "CacheDict":
        log.warning(
            "creating CacheDict from existing connection may lead to "
            "unexpected behaviour",
        )
        cache_dict = CacheDict(
            conn=conn,
            mapping=mapping,
            _cd_internal_flag=cls._internally_constructed,
        )

        return cache_dict
