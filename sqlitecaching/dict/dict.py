import dataclasses
import datetime
import enum
import functools
import logging
import sqlite3
import typing
import weakref

from sqlitecaching.dict.mapping import CacheDictMapping, SqlStatement
from sqlitecaching.exceptions import SqliteCachingException

log = logging.getLogger(__name__)

try:
    _ = CacheDictCategory  # type: ignore
    log.info("Not redefining exceptions")
except NameError:
    CacheDictCategory = SqliteCachingException.register_category(
        category_name=f"{__name__}.CacheDictCategory",
        category_id=1,
    )
    __CDC = CacheDictCategory

    CacheDictFilteredSqliteParamsException = __CDC.register_cause(
        cause_name=f"{__name__}.CacheDictFilteredSqliteParamsException",
        cause_id=0,
        fmt=(
            "sqlite_params provided to CacheDict contained unsupported keys: "
            "[{filtered}]"
        ),
        params=frozenset(["filtered"]),
    )

    CacheDictReadOnlyException = __CDC.register_cause(
        cause_name=f"{__name__}.CacheDictReadOnlyException",
        cause_id=1,
        fmt="attempting to perform [{op}] on readonly table [{table}]",
        params=frozenset(["op", "table"]),
    )
    CacheDictKeyTypeException = __CDC.register_cause(
        cause_name=f"{__name__}.CacheDictKeyTypeException",
        cause_id=2,
        fmt="key [{key}] has incorrect type [{key_type}] (expected KT: [{KT}])",
        params=frozenset(["key", "key_type", "KT"]),
        additional_excepts=frozenset([TypeError]),
    )
    CacheDictNoSuchKeyException = __CDC.register_cause(
        cause_name=f"{__name__}.CacheDictNoSuchKeyException",
        cause_id=3,
        fmt="key [{key}] not present in table [{table}]",
        params=frozenset(["key", "table"]),
        additional_excepts=frozenset([KeyError]),
    )
    CacheDictValueTypeException = __CDC.register_cause(
        cause_name=f"{__name__}.CacheDictValueTypeException",
        cause_id=4,
        fmt="value [{value}] has incorrect type [{value_type}] (expected VT: [{VT}]",
        params=frozenset(["value", "value_type", "VT"]),
        additional_excepts=frozenset([TypeError]),
    )
    CacheDictConnectionClosedException = __CDC.register_cause(
        cause_name=f"{__name__}.CacheDictConnectionClosedException",
        cause_id=5,
        fmt="operation [{op}] attempted after database connection closed",
        params=frozenset(["op"]),
    )
    CacheDictMethodUnsupportedException = __CDC.register_cause(
        cause_name=f"{__name__}.CacheDictMethodUnsupportedException",
        cause_id=6,
        fmt="method [{method}] is unsupported by CacheDict",
        params=frozenset(["method"]),
    )


@enum.unique
class ToCreate(enum.Enum):
    NONE = enum.auto()
    DATABASE = enum.auto()


T = typing.TypeVar("T")
KT = typing.TypeVar("KT")
VT = typing.TypeVar("VT")


@dataclasses.dataclass(frozen=True)
class Metadata(typing.Generic[KT, VT]):
    key_tuple: typing.Type[KT]
    key_columns: typing.FrozenSet[str]
    value_tuple: typing.Type[VT]
    value_columns: typing.FrozenSet[str]
    count_column: str
    timestamp_column: str


@dataclasses.dataclass(frozen=True)
class CacheDictRow(typing.Generic[KT, VT]):
    key: typing.Optional[KT]
    value: typing.Optional[VT]
    count: typing.Optional[int]
    timestamp: typing.Optional[datetime.datetime]


class CacheDictKeysView(typing.KeysView[KT]):
    pass


class CacheDictValuesView(typing.ValuesView[KT]):
    pass


class CacheDictItemsView(typing.ItemsView[KT, VT]):
    pass


class CacheDict(typing.Dict[KT, VT]):
    _internally_constructed: typing.ClassVar[typing.Any] = object()
    _raise_on_filtered_sqlite_params: typing.ClassVar[bool] = False
    __MARKER = object()

    ANON_MEM_PATH: typing.ClassVar[str] = ":memory:"
    ANON_DISK_PATH: typing.ClassVar[str] = ""

    PASSTHROUGH_PARAMS: typing.ClassVar[typing.List[str]] = [
        "timeout",
        "detect_types",
        "factory",
        "cached_statements",
    ]

    conn: typing.Optional[sqlite3.Connection] = None
    mapping: CacheDictMapping
    filepath: str
    read_only: bool
    _finalize: weakref.finalize
    initialized: bool = False

    def __init__(
        self: "CacheDict[KT, VT]",
        *,
        conn: sqlite3.Connection,
        mapping: CacheDictMapping[KT, VT],
        filepath: str,
        read_only: bool = False,
        **kwargs,
    ) -> None:
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
        metadata = Metadata[KT, VT](
            mapping.KeyType,
            frozenset(i.replace('"', "") for i in mapping.key_idents),
            mapping.ValueType,
            frozenset(i.replace('"', "") for i in mapping.value_idents),
            mapping.COUNT_COLUMN,
            mapping.TIMESTAMP_COLUMN,
        )
        self.conn.row_factory = functools.partial(
            self._tuple_factory,
            metadata=metadata,
        )
        self.mapping = mapping
        self.filepath = filepath
        self.read_only = read_only

        if not read_only:
            # creating the mapped table should be idempotent
            # (CREATE TABLE IF NOT EXIST ...)
            # but obviously doesn't work for readonly connections
            self.create_table()

        log.info("created [%#0x] conn: [%s]", id(self), self.conn)
        self.initialized = True

    def create_table(self: "CacheDict[KT, VT]") -> None:
        log.debug("create table [%r]", self)
        if self.read_only:
            raise CacheDictReadOnlyException(
                {
                    "op": "create",
                    "table": self.mapping.table_ident,
                },
            )
        create_stmt = self.mapping.create_statement()
        self._execute(create_stmt, op="create")

    def clear(self: "CacheDict[KT, VT]") -> None:
        log.debug("clear [%r]", self)
        if self.read_only:
            raise CacheDictReadOnlyException(
                {
                    "op": "clear",
                    "table": self.mapping.table_ident,
                },
            )
        clear_stmt = self.mapping.clear_statement()
        self._execute(clear_stmt, op="clear")

    def delete_table(self: "CacheDict[KT, VT]") -> None:
        log.debug("delete table [%r]", self)
        if self.read_only:
            raise CacheDictReadOnlyException(
                {
                    "op": "delete",
                    "table": self.mapping.table_ident,
                },
            )
        delete_stmt = self.mapping.delete_statement()
        self._execute(delete_stmt, op="delete")

    def close(self: "CacheDict[KT, VT]") -> None:
        log.warning("closing [%r]", self)
        # _finalize() closes the connection
        self._finalize()
        self.conn = None  # type: ignore

    def interrupt(self: "CacheDict[KT, VT]") -> None:
        log.debug("interrupt [%r]", self)
        if not self.conn:
            log.warning("interrupt with None connection [%r]", self)
            raise CacheDictConnectionClosedException({"op": "interrupt"})

        try:
            return self.conn.interrupt()
        except Exception:
            log.debug("exception from interrupt", exc_info=True)
            raise

    def rollback(self: "CacheDict[KT, VT]") -> None:
        log.debug("rollback [%r]", self)
        if not self.conn:
            log.warning("rollback with None connection [%r]", self)
            raise CacheDictConnectionClosedException({"op": "rollback"})

        try:
            return self.conn.rollback()
        except Exception:
            log.debug("exception from rollback", exc_info=True)
            raise

    def commit(self: "CacheDict[KT, VT]") -> None:
        log.debug("committing [%r]", self)
        if not self.conn:
            log.warning("commit with None connection [%r]", self)
            raise CacheDictConnectionClosedException({"op": "commit"})

        try:
            return self.conn.commit()
        except Exception:
            log.debug("exception from commit", exc_info=True)
            raise

    def copy(self: "CacheDict[KT, VT]") -> typing.NoReturn:
        raise CacheDictMethodUnsupportedException({"method": "copy()"})

    @typing.overload
    def get(self: "CacheDict[KT, VT]", key: KT) -> typing.Optional[VT]:
        ...

    @typing.overload
    def get(
        self: "CacheDict[KT, VT]",
        key: KT,
        default: T,
    ) -> typing.Union[VT, T]:
        ...

    def get(
        self: "CacheDict[KT, VT]",
        key: KT,
        default: typing.Optional[T] = None,
    ) -> typing.Union[typing.Optional[VT], T]:
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def setdefault(
        self: "CacheDict[KT, VT]",
        key: KT,
        default: VT = __MARKER,  # type: ignore
    ) -> VT:
        try:
            current: VT = self[key]
            return current
        except KeyError:
            pass
        value: VT
        if default is self.__MARKER:
            try:
                value = self.mapping.ValueType()
            except Exception:
                log.warn(
                    "Exception from default constr. ValueType [%s]",
                    self._get_value_type_mapping(),
                    exc_info=True,
                )
                raise
        else:
            value = default
        self[key] = value
        return value

    @typing.overload
    def pop(self: "CacheDict[KT, VT]", key: KT) -> VT:
        ...

    @typing.overload
    def pop(
        self: "CacheDict[KT, VT]",
        key: KT,
        default: T = ...,
    ) -> typing.Union[VT, T]:
        ...

    def pop(
        self: "CacheDict[KT, VT]",
        key: KT,
        default: T = __MARKER,  # type: ignore
    ) -> typing.Union[VT, T]:
        try:
            value = self[key]
            del self[key]
            return value
        except KeyError:
            if default is self.__MARKER:
                raise
            else:
                return default

    def popitem(self: "CacheDict[KT, VT]") -> typing.Tuple[KT, VT]:
        raise Exception()

    @typing.overload
    def update(
        self: "CacheDict[KT, VT]",
        mapping: typing.Mapping[KT, VT],
        /,
        **kwargs: VT,
    ) -> None:
        ...

    @typing.overload
    def update(
        self: "CacheDict[KT, VT]",
        mapping: typing.Iterable[typing.Tuple[KT, VT]],
        /,
        **kwargs: VT,
    ) -> None:
        ...

    @typing.overload
    def update(self: "CacheDict[KT, VT]", /, **kwargs: VT) -> None:
        ...

    def update(
        self: "CacheDict[KT, VT]",
        mapping: typing.Union[
            typing.Mapping[KT, VT],
            typing.Iterable[typing.Tuple[KT, VT]],
        ] = (),
        /,
        **kwargs: VT,
    ) -> None:
        raise Exception()

    def keys(self: "CacheDict[KT, VT]") -> typing.KeysView[KT]:
        return CacheDictKeysView(self)

    def values(self: "CacheDict[KT, VT]") -> typing.ValuesView[VT]:
        return CacheDictValuesView(self)

    def items(self: "CacheDict[KT, VT]") -> typing.ItemsView[KT, VT]:
        return CacheDictItemsView(self)

    @property
    def in_transaction(self: "CacheDict[KT, VT]"):
        log.debug("get in_transaction [%r]", self)
        if not self.conn:
            log.warning("get in_transaction with None connection [%r]", self)
            raise CacheDictConnectionClosedException({"op": "in_transaction"})
        return self.conn.in_transaction

    @classmethod
    def raise_on_filtered_sqlite_params(
        cls: typing.Type["CacheDict[KT, VT]"],
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
    def open_anon_memory(
        cls: typing.Type["CacheDict[KT, VT]"],
        *,
        mapping: CacheDictMapping[KT, VT],
        sqlite_params: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    ) -> "CacheDict[KT, VT]":
        log.info("open anon memory")

        cleaned_sqlite_params = cls._cleanup_sqlite_params(sqlite_params)

        conn = cls._open_connection(database=cls.ANON_MEM_PATH, **cleaned_sqlite_params)

        cache_dict = CacheDict[KT, VT](
            conn=conn,
            mapping=mapping,
            filepath=cls.ANON_MEM_PATH,
            _cd_internal_flag=cls._internally_constructed,
        )

        return cache_dict

    @classmethod
    def open_anon_disk(
        cls: typing.Type["CacheDict[KT, VT]"],
        *,
        mapping: CacheDictMapping[KT, VT],
        sqlite_params: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    ) -> "CacheDict[KT, VT]":
        log.info("open anon disk")
        cleaned_sqlite_params = cls._cleanup_sqlite_params(sqlite_params)

        conn = cls._open_connection(
            database=cls.ANON_DISK_PATH,
            **cleaned_sqlite_params,
        )

        cache_dict = CacheDict[KT, VT](
            conn=conn,
            mapping=mapping,
            filepath=cls.ANON_DISK_PATH,
            _cd_internal_flag=cls._internally_constructed,
        )

        return cache_dict

    @classmethod
    def open_readonly(
        cls: typing.Type["CacheDict[KT, VT]"],
        *,
        path: str,
        mapping: CacheDictMapping[KT, VT],
        sqlite_params: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    ) -> "CacheDict[KT, VT]":
        log.info("open readonly")
        cleaned_sqlite_params = cls._cleanup_sqlite_params(sqlite_params)

        uri_path = f"file:{path}?mode=ro"
        conn = cls._open_connection(
            database=uri_path,
            uri=True,
            **cleaned_sqlite_params,
        )

        cache_dict = CacheDict[KT, VT](
            conn=conn,
            mapping=mapping,
            filepath=uri_path,
            read_only=True,
            _cd_internal_flag=cls._internally_constructed,
        )

        return cache_dict

    @classmethod
    def open_readwrite(
        cls: typing.Type["CacheDict[KT, VT]"],
        *,
        path: str,
        mapping: CacheDictMapping[KT, VT],
        create: typing.Optional[ToCreate] = ToCreate.NONE,
        sqlite_params: typing.Optional[typing.Mapping[str, typing.Any]] = None,
    ) -> "CacheDict[KT, VT]":
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

        cache_dict = CacheDict[KT, VT](
            conn=conn,
            mapping=mapping,
            filepath=uri_path,
            _cd_internal_flag=cls._internally_constructed,
        )

        return cache_dict

    def _execute(
        self: "CacheDict[KT, VT]",
        statement: SqlStatement,
        params: typing.Optional[typing.Iterable[typing.Any]] = None,
        /,
        *,
        op: str,
    ) -> sqlite3.Cursor:
        log.debug("_execute [%s] for [%r]", statement, self)
        if not self.conn:
            log.warning("_execute() with None connection [%r]", self)
            raise CacheDictConnectionClosedException({"op": op})
        try:
            if params:
                return self.conn.execute(statement, params)
            else:
                return self.conn.execute(statement)
        except Exception:
            log.debug("exception from execute", exc_info=True)
            raise

    @classmethod
    def _create_from_conn(
        cls: typing.Type["CacheDict[KT, VT]"],
        *,
        conn: sqlite3.Connection,
        mapping: CacheDictMapping[KT, VT],
    ) -> "CacheDict[KT, VT]":
        log.warning(
            "creating CacheDict from existing connection may lead to "
            "unexpected behaviour",
        )
        cache_dict = CacheDict[KT, VT](
            conn=conn,
            mapping=mapping,
            filepath="UNKNOWN(_create_from_conn)",
            _cd_internal_flag=cls._internally_constructed,
        )

        return cache_dict

    def __len__(self: "CacheDict[KT, VT]") -> int:
        log.debug("len [%r]", self)
        length_stmt = self.mapping.length_statement()
        cursor = self._execute(length_stmt, op="length")
        res: typing.Optional[CacheDictRow[KT, VT]] = cursor.fetchone()
        if res:
            rlen = res.count
            if rlen:
                return rlen
        return 0

    def __bool__(self: "CacheDict[KT, VT]") -> bool:
        log.debug("bool [%s]", repr(self))
        if not self.initialized:
            log.warning("preinitial, return false")
            return False
        bool_stmt = self.mapping.bool_statement()
        cursor = self._execute(bool_stmt, op="bool")
        res: typing.Optional[CacheDictRow[KT, VT]] = cursor.fetchone()
        if res:
            if res.timestamp:
                return True
        return False

    def __delitem__(self: "CacheDict[KT, VT]", key: KT, /) -> None:
        log.debug("delete [%#0x] key: [%s]", id(self), key)
        if self.read_only:
            raise CacheDictReadOnlyException(
                {
                    "op": "delitem",
                    "table": self.mapping.table_ident,
                },
            )
        if not isinstance(key, self.mapping.KeyType):
            raise CacheDictKeyTypeException(
                {
                    "key": key,
                    "key_type": type(key),
                    "KT": self._get_key_type_mapping(),
                },
            )
        try:
            _ = self.__getitem__(key)
        except Exception:
            log.debug(
                "exception getting [%s], cannot delete in [%#0x]",
                key,
                id(self),
                exc_info=True,
            )
            raise
        else:
            remove_stmt = self.mapping.remove_statement()
            self._execute(remove_stmt, dataclasses.astuple(key), op="remove")

    def __contains__(self: "CacheDict[KT, VT]", key: object, /) -> bool:
        log.debug("get [%#0x] key: [%s]", id(self), key)
        if not isinstance(key, self.mapping.KeyType):
            raise CacheDictKeyTypeException(
                {
                    "key": key,
                    "key_type": type(key),
                    "KT": self._get_key_type_mapping(),
                },
            )
        select_stmt = self.mapping.select_statement()
        cursor = self._execute(select_stmt, dataclasses.astuple(key), op="contains")
        res: typing.Optional[CacheDictRow[KT, VT]] = cursor.fetchone()
        if res:
            # if there are no value columns then the timestamp is returned
            if res.value or res.timestamp:
                return True
        return False

    def __getitem__(self: "CacheDict[KT, VT]", key: KT, /) -> VT:
        log.debug("get [%#0x] key: [%s]", id(self), key)
        if not isinstance(key, self.mapping.KeyType):
            raise CacheDictKeyTypeException(
                {
                    "key": key,
                    "key_type": type(key),
                    "KT": self._get_key_type_mapping(),
                },
            )
        select_stmt = self.mapping.select_statement()
        cursor = self._execute(select_stmt, dataclasses.astuple(key), op="select")
        res: typing.Optional[CacheDictRow[KT, VT]] = cursor.fetchone()
        if not res:
            raise CacheDictNoSuchKeyException(
                {"key": key, "table": self.mapping.table_ident},
            )
        elif not res.value:
            raise Exception()
        return res.value

    def __iter__(self: "CacheDict[KT, VT]") -> typing.Iterator[KT]:
        keys_stmt = self.mapping.keys_statement()
        cursor = self._execute(keys_stmt, op="keys")
        res: typing.Optional[CacheDictRow[KT, VT]] = cursor.fetchone()
        if res:
            key: typing.Optional[KT] = res.key
            while key:
                yield key
                res = cursor.fetchone()
                if res:
                    key = res.key
                else:
                    key = None

    def __setitem__(self: "CacheDict[KT, VT]", key: KT, value: VT, /) -> None:
        log.debug("set [%#0x] key: [%s] value: [%s]", id(self), key, value)
        if self.read_only:
            raise CacheDictReadOnlyException(
                {
                    "op": "setitem",
                    "table": self.mapping.table_ident,
                },
            )
        if not isinstance(key, self.mapping.KeyType):
            raise CacheDictKeyTypeException(
                {
                    "key": key,
                    "key_type": type(key),
                    "KT": self._get_key_type_mapping(),
                },
            )

        if not isinstance(value, self.mapping.ValueType):
            raise CacheDictValueTypeException(
                {
                    "value": value,
                    "value_type": type(value),
                    "VT": self._get_value_type_mapping(),
                },
            )

        upsert_stmt = self.mapping.upsert_statement()
        try:
            self._execute(
                upsert_stmt,
                (datetime.datetime.now(),)
                + dataclasses.astuple(key)
                + dataclasses.astuple(value),
                op="upsert",
            )
        except Exception:
            # TODO can this actually happen?
            # raise ???
            log.warning("exception from upsert", exc_info=True)
            raise

    def __reversed__(self: "CacheDict[KT, VT]") -> typing.Iterator[KT]:
        keys_stmt = self.mapping.keys_statement(asc=False)
        cursor = self._execute(keys_stmt, op="reversed")
        res: typing.Optional[CacheDictRow[KT, VT]] = cursor.fetchone()
        if res:
            key: typing.Optional[KT] = res.key
            while key:
                yield key
                res = cursor.fetchone()
                if res:
                    key = res.key
                else:
                    key = None

    @classmethod
    def _finalize_instance(
        cls: typing.Type["CacheDict[KT, VT]"],
        *,
        id: int,
        conn: sqlite3.Connection,
    ) -> None:
        log.info("_finalize_instance [%#0x] conn: [%s]", id, conn)
        try:
            conn.close()
        except Exception:  # pragma: no cover
            # We don't really care what the exception is as we cannot do
            # anything about it. If it's rethrown it will just be output
            # to stderr
            log.error("exception when closing conn: [%s]", conn, exc_info=True)

    def _get_key_type_mapping(
        self: "CacheDict[KT, VT]",
    ) -> typing.Mapping[str, typing.Type]:
        return {f.name: f.type for f in dataclasses.fields(self.mapping.KeyType)}

    def _get_value_type_mapping(
        self: "CacheDict[KT, VT]",
    ) -> typing.Mapping[str, typing.Type]:
        return {f.name: f.type for f in dataclasses.fields(self.mapping.ValueType)}

    @classmethod
    def _is_internally_constructed(
        cls: typing.Type["CacheDict[KT, VT]"],
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
    def _cleanup_sqlite_params(
        cls: typing.Type["CacheDict[KT, VT]"],
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
    def _tuple_factory(
        cls: typing.Type["CacheDict[KT, VT]"],
        cursor: sqlite3.Cursor,
        row: typing.Tuple[typing.Any, ...],
        /,
        *,
        metadata: Metadata[KT, VT],
    ) -> CacheDictRow[KT, VT]:
        keys_dict: typing.Dict[str, typing.Any] = {}
        values_dict: typing.Dict[str, typing.Any] = {}

        keys: typing.Optional[KT] = None
        values: typing.Optional[VT] = None
        count: typing.Optional[int] = None
        timestamp: typing.Optional[datetime.datetime] = None

        columns = (x[0] for x in cursor.description)
        for (k, v) in zip(columns, row):
            if k == metadata.timestamp_column:
                timestamp = v
            elif k == metadata.count_column:
                count = v
            elif k in metadata.key_columns:
                keys_dict[k] = v
            elif k in metadata.value_columns:
                values_dict[k] = v

        if keys_dict:
            keys = metadata.key_tuple(**keys_dict)  # type: ignore
        if values_dict:
            values = metadata.value_tuple(**values_dict)  # type: ignore
        elif len(metadata.value_columns) == 0 and timestamp:
            values = metadata.value_tuple()

        rv = CacheDictRow[KT, VT](keys, values, count, timestamp)

        return rv

    @classmethod
    def _open_connection(
        cls: typing.Type["CacheDict[KT, VT]"],
        **kwargs,
    ) -> sqlite3.Connection:
        conn = sqlite3.connect(**kwargs)
        return conn
