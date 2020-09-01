import collections
import logging
import re
import sqlite3
import textwrap
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
            self.log.addHandler(logging.NullLogger())
            self.log.info("using caller provided logger: [%s]", log_name)
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


CacheDictColumnTypes = namedtuple("CacheDictColumnTypes", ["pytype", "sqltype"])
CacheDictMappingTuple = namedtuple(
    "CacheDictMappingTuple", ["table_name", "key_columns", "value_columns"]
)


class CacheDictMapping:
    def __init__(self, *, table_name, keys, values):
        key_columns = collections.OrderedDict()
        value_columns = collections.OrderedDict()
        keyval_columns = []

        validated_table_name = self._validate_identifier(table_name)
        if validated_table_name.startswith("sqlite_"):
            log.error(
                "table_name cannot start with sqlite_ : [%s]", validated_table_name
            )
            raise CacheDictException(
                "table_name cannot start with sqlite_ : [%s]" % validated_table_name
            )

        for (name, types) in keys.items():
            validated_name = self._validate_identifier(name)
            self._handle_column(key_columns, validated_name, types)

        unset_value = object()
        for (name, types) in values.items():
            validated_name = self._validate_identifier(name)
            in_keys = key_columns.get(validated_name, unset_value)
            if in_keys is not unset_value:
                keyval_columns.append(validated_name)
            else:
                self._handle_column(value_columns, validated_name, types)

        if keyval_columns:
            log.error(
                (
                    "the set of key columns and value columns must be disjoint. "
                    "columns [%s] occur in both key and value sets"
                ),
                keyval_columns,
            )
            raise CacheDictException(
                (
                    "the set of key columns and value columns must be disjoint. "
                    "columns [%s] occur in both key and value sets"
                )
                % keyval_columns
            )

        self.Keys = namedtuple("Keys", key_columns.keys())
        self.Values = namedtuple("Values", value_columns.keys())

        self.key_info = self.Keys(**key_columns)
        self.value_info = self.Values(**value_columns)
        self.mapping_tuple = CacheDictMappingTuple(
            table=table_name, keys=self.key_info, values=self.value_info
        )

    def create_table(self, *, conn):
        pass

    @classmethod
    def _handle_column(cls, column_dict, validated_name, types, /):
        pytype = types["pytype"]
        sqltype = types["sqltype"]

        cls._validate_pytype(pytype)
        validated_sqltype = cls._validate_sqltype(sqltype)

        type_tuple = CacheDictColumnTypes(pytype=pytype, sqltype=validated_sqltype)
        column_dict[validated_name] = type_tuple

    __IDENTIFIER_RE_DEFN = textwrap.dedent(
        # must be  >#< aligned to set initial indent correctly
        r"""        #
        ^               # start of string
        [a-z]           # start with an ascii letter
        [a-z0-9_]{0,62} # followed by up to 62 alphanumeric or underscores
        $               # end of string
        """
    )

    __IDENTIFIER_PATTERN = re.compile(
        __IDENTIFIER_RE_DEFN, flags=(re.ASCII | re.IGNORECASE | re.VERBOSE),
    )

    @classmethod
    def _validate_identifier(cls, identifier, /):
        match = cls.__IDENTIFIER_PATTERN.match(identifier)
        if not match:
            log.error(
                (
                    "sqlitecaching identifier provided: [%s] does not match "
                    "requirements [%s]"
                ),
                identifier,
                cls.__IDENTIFIER_RE_DEFN,
            )
            raise CacheDictException(
                (
                    "sqlitecaching identifier provided: [%s] does not match "
                    "requirements [%s]"
                )
                % (identifier, cls.__IDENTIFIER_RE_DEFN)
            )
        casefolded_identifier = identifier.casefold()
        if identifier != casefolded_identifier:
            log.warning(
                (
                    "sqlitecaching identifier [%s] is not casefolded. Using "
                    "casefolded value [%s]"
                ),
                identifier,
                casefolded_identifier,
            )

        return casefolded_identifier

    @classmethod
    def _validate_pytype(cls, pytype, /):
        if not pytype:
            return
        elif pytype not in sqlite3.adapters:
            log.warn(
                (
                    "sqltype [%s] is not present in sqlite3.converters, so may not "
                    "be convertable from stored value to python object"
                ),
                pytype,
            )

    @classmethod
    def _validate_sqltype(cls, sqltype, /):
        validated_sqltype = cls._validate_identifier(sqltype)
        # converters are all stored uppercase, so have to .upper()
        if validated_sqltype.upper() not in sqlite3.converters:
            log.warn(
                (
                    "sqltype [%s] is not present in sqlite3.converters, so may not "
                    "be convertable from stored value to python object"
                ),
                validated_sqltype,
            )
        return validated_sqltype


class CacheDictException(Exception):
    pass
