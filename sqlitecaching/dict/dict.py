import enum
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

CacheDictFilteredSqliteParamsException = __CDC.register_cause(
    cause_name=f"{__name__}.CacheDictFilteredSqliteParamsException",
    cause_id=0,
    fmt="sqlite_params provided to CacheDict contained unsupported keys: [{filtered}]",
    params=frozenset(["filtered"]),
)


@enum.unique
class ToCreate(enum.Enum):
    NONE = enum.auto()
    TABLE = enum.auto()
    DATABASE = enum.auto()


class CacheDict(UserDict):
    __internally_constructed: typing.ClassVar[typing.Any] = object()
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

    def __init__(
        self,
        *,
        conn: sqlite3.Connection,
        mapping: CacheDictMapping,
        read_only: bool = False,
        logger: typing.Optional[logging.Logger] = None,
        **kwargs,
    ):
        # Don't populate anything by passing **kwargs here.
        # We only take **kwargs to attempt to note the fact that we're called
        # in an unexpected way.
        super().__init__()

        self.__is_internally_constructed(logger, **kwargs)

        self.conn = conn
        self.mapping = mapping
        self.read_only = read_only

        if logger:
            log.warning("using caller provided logger: [%s]", logger.name)
            self.log = logger
            self.log.info("using caller provided logger: [%s]", logger.name)
        else:
            self.log = log

    @classmethod
    def __is_internally_constructed(
        cls,
        logger: typing.Optional[logging.Logger] = None,
        /,
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
            if logger:
                logger.warning(
                    "direct construction of [%s] may lead to unexpected behaviours",
                    cls.__name__,
                )

    @classmethod
    def raise_on_filtered_sqlite_params(
        cls,
        should_raise: typing.Optional[bool] = None,
        /,
        *,
        logger: typing.Optional[logging.Logger] = None,
    ) -> bool:
        if should_raise is not None:
            log.warning(
                "setting [%s]._raise_on_filtered_sqlite_params to [%s]",
                cls.__name__,
                should_raise,
            )
            if logger:
                logger.warning(
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
        *,
        logger: typing.Optional[logging.Logger] = None,
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
                    if logger:
                        logger.debug(
                            "sqlite parameter [%s] present but no value",
                            param,
                        )
            else:
                log.warning(
                    (
                        "unsupported (by sqlitecaching) sqlite parameter [%s] "
                        "found with value [%s] - removing"
                    ),
                    param,
                    value,
                )
                if logger:
                    logger.warning(
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
            if logger:
                logger.info("raising for filtered sqlite params")
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
        logger: typing.Optional[logging.Logger] = None,
    ) -> "CacheDict":
        log.info("open anon memory")
        if logger:
            logger.info("open anon memory")

        cleaned_sqlite_params = cls._cleanup_sqlite_params(sqlite_params)

        conn = cls._open_connection(database=cls.ANON_MEM_PATH, **cleaned_sqlite_params)

        cache_dict = CacheDict(
            conn=conn,
            mapping=mapping,
            logger=logger,
            _cd_internal_flag=cls.__internally_constructed,
        )

        # TODO create table
        return cache_dict

    @classmethod
    def open_anon_disk(
        cls,
        *,
        mapping: CacheDictMapping,
        sqlite_params: typing.Optional[typing.Mapping[str, typing.Any]] = None,
        logger: typing.Optional[logging.Logger] = None,
    ) -> "CacheDict":
        log.info("open anon disk")
        if logger:
            logger.info("open anon disk")
        cleaned_sqlite_params = cls._cleanup_sqlite_params(sqlite_params)

        conn = cls._open_connection(
            database=cls.ANON_DISK_PATH,
            **cleaned_sqlite_params,
        )

        cache_dict = CacheDict(
            conn=conn,
            mapping=mapping,
            logger=logger,
            _cd_internal_flag=cls.__internally_constructed,
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
        logger: typing.Optional[logging.Logger] = None,
    ) -> "CacheDict":
        log.info("open readonly")
        if logger:
            logger.info("open readonly")
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
            logger=logger,
            _cd_internal_flag=cls.__internally_constructed,
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
        logger: typing.Optional[logging.Logger] = None,
    ) -> "CacheDict":
        log.info("open readwrite create: [%s]", create)
        if logger:
            logger.info("open readwrite create: [%s]", create)
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
            logger=logger,
            _cd_internal_flag=cls.__internally_constructed,
        )

        # TODO create table if create_table in (ToCreate.TABLE, ToCreate.DATABASE)
        return cache_dict

    @classmethod
    def _create_from_conn(
        cls,
        *,
        conn: sqlite3.Connection,
        mapping: CacheDictMapping,
        logger: typing.Optional[logging.Logger] = None,
    ) -> "CacheDict":
        log.warning(
            "creating CacheDict from existing connection may lead to "
            "unexpected behaviour",
        )
        if logger:
            logger.warning(
                "creating CacheDict from existing connection may lead to "
                "unexpected behaviour",
            )
        return CacheDict(
            conn=conn,
            mapping=mapping,
            logger=logger,
            _cd_internal_flag=cls.__internally_constructed,
        )
