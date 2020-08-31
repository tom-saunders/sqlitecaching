import collections
import logging
import re
import sqlite3
from collections import UserDict, namedtuple

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


class CacheDict(UserDict):
    __internally_constructed = object()

    __ANON_MEM_PATH = ":memory:"
    __ANON_DISK_PATH = ""

    # Not including
    # "detect_types",
    # as that should be dependant on mapping(?)
    __PASSTHROUGH_PARAMS = [
        "timeout",
        "isolation_level",
        "factory",
        "cached_statements",
    ]

    __IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")

    def __init__(self, *, conn, mapping, log_name, **kwargs):
        # Don't populate anything by passing **kwargs here.
        super().__init__()

        internally_constructed = kwargs.get("_cd_internal_flag", None)
        if (not internally_constructed) or (
            internally_constructed is not type(self).__internally_constructed
        ):
            log.warn(
                "direct construction of %s may lead to unexpected behaviours",
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

    @classmethod
    def _cleanup_sqlite_params(cls, *, sqlite_params):
        if not sqlite_params:
            return {}

        cleaned_params = {}
        for (param, value) in sqlite_params.items():
            if param in cls.__PASSTHROUGH_PARAMS:
                if value:
                    cleaned_params[param] = value
                else:
                    log.debug("sqlite parameter %s present but no value", param)
            else:
                log.warn(
                    (
                        "unsupported (by sqlitecaching) sqlite parameter %s "
                        "found with value %s - removing"
                    ),
                    param,
                    value,
                )
        return cleaned_params

    @classmethod
    def open_anon_memory(cls, *, mapping=None, sqlite_params=None, log_name=None):
        log.info("open anon memory")

        cleaned_sqlite_params = cls._cleanup_sqlite_params(sqlite_params=sqlite_params)

        conn = sqlite3.connect(cls.__ANON_MEM_PATH, **cleaned_sqlite_params)

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

        conn = sqlite3.connect(cls.__ANON_DISK_PATH, **cleaned_sqlite_params)

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


CacheDictColumnTypes = namedtuple("CacheDictColumnTypes", ["pytype", "sqltype"])
CacheDictMappingTuple = namedtuple(
    "CacheDictMappingTuple", ["table_name", "key_columns", "value_columns"]
)


class CacheDictMapping:
    def __init__(self, *, table_name, keys, values):
        key_columns = collections.OrderedDict()
        value_columns = collections.OrderedDict()
        keyval_columns = []

        self.validate_identifier(table_name)

        for (name, types) in keys.items():
            self._handle_column(key_columns, name, types)

        unset_value = object()
        for (name, types) in values.items():
            in_keys = key_columns.get(name, unset_value)
            if in_keys is not unset_value:
                keyval_columns.append(name)
            else:
                self._handle_column(value_columns, name, types)

        if keyval_columns:
            log.error(
                (
                    "the set of key columns and value columns must be disjoint. "
                    "columns [ %s ] occur in both key and value sets"
                ),
                keyval_columns,
            )
            raise CacheDictException(
                (
                    "the set of key columns and value columns must be disjoint. "
                    "columns [ %s ] occur in both key and value sets"
                )
                % ", ".join(keyval_columns)
            )

        self.Keys = namedtuple("Keys", key_columns.keys())
        self.Values = namedtuple("Values", value_columns.keys())

        self.key_info = self.Keys(**key_columns)
        self.value_info = self.Values(**value_columns)
        self.mapping_tuple = CacheDictMappingTuple(
            table=table_name, keys=self.key_info, values=self.value_info
        )

    @classmethod
    def _handle_column(cls, column_dict, name, types, /):
        pytype = types["pytype"]
        sqltype = types["sqltype"]
        type_tuple = CacheDictColumnTypes._make([pytype, sqltype])

        cls._validate_identifier(name)
        cls._validate_pytype(pytype)
        cls._validate_sqltype(sqltype)

        column_dict[name] = type_tuple

    @classmethod
    def _validate_identifier(cls, identifier, /):

        pass

    @classmethod
    def _validate_pytype(cls, pytype, /):
        pass

    @classmethod
    def _validate_sqltype(cls, sqltype, /):
        pass


class CacheDictException(Exception):
    pass
