import collections
import logging
import re
import sqlite3
from collections import UserDict, namedtuple

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


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


CacheDictMappingTuple = namedtuple("CacheDictMappingTuple", ["table", "keys", "values"])


class CacheDictMapping:
    def __init__(self, *, table, keys, values):
        if not keys:
            fmt = "sqlitecaching keys must not be empty"
            log.error(fmt)
            raise CacheDictException(fmt)

        validated_table = self._validate_identifier(identifier=table)
        if validated_table.startswith("sqlite_"):
            fmt = "table cannot start with sqlite_ : [%s]"
            log.error(fmt, validated_table)
            raise CacheDictException(fmt % validated_table)

        key_columns = collections.OrderedDict()
        value_columns = collections.OrderedDict()
        keyval_columns = []

        for (name, sqltype) in keys.items():
            validated_name = self._validate_identifier(identifier=name)
            self._handle_column(
                column_dict=key_columns, validated_name=validated_name, sqltype=sqltype
            )

        unset_value = object()
        for (name, sqltype) in values.items():
            validated_name = self._validate_identifier(identifier=name)
            in_keys = key_columns.get(validated_name, unset_value)
            if in_keys is not unset_value:
                keyval_columns.append(validated_name)
            else:
                self._handle_column(
                    column_dict=value_columns,
                    validated_name=validated_name,
                    sqltype=sqltype,
                )

        if keyval_columns:
            fmt = (
                "the sets of key columns and value columns must be disjoint. "
                "columns [%s] occur in both key and value sets"
            )
            log.error(
                fmt, keyval_columns,
            )
            raise CacheDictException(fmt % keyval_columns)

        self.Keys = namedtuple("Keys", key_columns.keys())
        self.Values = namedtuple("Values", value_columns.keys())

        self.key_info = self.Keys(**key_columns)
        self.value_info = self.Values(**value_columns)
        self.mapping_tuple = CacheDictMappingTuple(
            table=validated_table, keys=self.key_info, values=self.value_info
        )

        self._create_statement = None
        self._clear_statement = None
        self._delete_statement = None
        self._upsert_statement = None

    # fmt: off
    _CREATE_FMT = (
        "-- sqlitecaching create table\n"
        "CREATE TABLE {table_identifier}\n"
        "(\n"
        "    -- keys\n"
        "    {key_column_definitions}\n"
        "    -- values\n"
        "    {value_column_definitions}\n"
        "    {primary_key_definition}\n"
        ");\n"
    )
    _PRIMARY_KEY_FMT = (
        "PRIMARY KEY (\n"
        "        {primary_key_columns}\n"
        "    ) ON CONFLICT ABORT"
    )
    # fmt: on

    def create_statement(self):
        if self._create_statement:
            return self._create_statement

        table_identifier = self.mapping_tuple.table

        keys = self.mapping_tuple.keys
        key_columns = sorted(keys._fields)

        values = self.mapping_tuple.values
        value_columns = sorted(values._fields)

        # fmt: off
        key_column_definitions = "".join(
            [
                f"{column} {getattr(keys, column)}, -- primary key\n    "
                for column in key_columns
            ]
        )
        value_column_definitions = "".join(
            [
                f"{column} {getattr(values, column)}, -- value\n    "
                for column in value_columns
            ]
        )
        # fmt: on

        primary_key_columns = ",\n        ".join(key_columns)
        primary_key_definition = self._PRIMARY_KEY_FMT.format(
            primary_key_columns=primary_key_columns
        )

        unstripped_create_statement = self._CREATE_FMT.format(
            table_identifier=table_identifier,
            key_column_definitions=key_column_definitions,
            value_column_definitions=value_column_definitions,
            primary_key_definition=primary_key_definition,
        )

        create_lines = []
        for line in unstripped_create_statement.splitlines():
            create_lines.append(line.rstrip())
        # needed for trailing newline
        create_lines.append("")
        self._create_statement = "\n".join(create_lines)
        return self._create_statement

    # fmt: off
    _CLEAR_FMT = (
        "-- sqlitecaching clear table\n"
        "DELETE from {table_identifier};\n"
    )
    # fmt: on

    def clear_statement(self):
        if self._clear_statement:
            return self._clear_statement

        table_identifier = self.mapping_tuple.table

        unstripped_clear_statement = self._CLEAR_FMT.format(
            table_identifier=table_identifier
        )

        clear_lines = []
        for line in unstripped_clear_statement.splitlines():
            clear_lines.append(line.rstrip())
        # needed for trailing newline
        clear_lines.append("")
        self._clear_statement = "\n".join(clear_lines)
        return self._clear_statement

    # fmt: off
    _DELETE_FMT = (
        "-- sqlitecaching delete table\n"
        "DROP TABLE {table_identifier};\n"
    )
    # fmt: on

    def delete_statement(self):
        if self._delete_statement:
            return self._delete_statement

        table_identifier = self.mapping_tuple.table

        unstripped_delete_statement = self._DELETE_FMT.format(
            table_identifier=table_identifier
        )

        delete_lines = []
        for line in unstripped_delete_statement.splitlines():
            delete_lines.append(line.rstrip())
        # needed for trailing newline
        delete_lines.append("")
        self._delete_statement = "\n".join(delete_lines)
        return self._delete_statement

    # fmt: off
    _UPSERT_FMT = (
        "-- sqlitecaching insert or update into table\n"
        "INSERT INTO {table_identifier}\n"
        "(\n"
        "    -- all_columns\n"
        "    {all_columns}\n"
        ") VALUES (\n"
        "    -- all_values\n"
        "    {all_values}\n"
        "){upsert_stmt};\n"
    )
    _UPSERT_STMT_FMT = (
        " ON CONFLICT (\n"
        "    -- key columns\n"
        "    {key_columns}\n"
        ")\n"
        "DO UPDATE SET (\n"
        "    -- value_columns\n"
        "    {value_columns}\n"
        ") = (\n"
        "    -- value_values\n"
        "    {value_values}\n"
        ")\n"
    )
    # fmt: on

    def upsert_statement(self):
        if self._upsert_statement:
            return self._upsert_statement

        table_identifier = self.mapping_tuple.table

        keys = self.mapping_tuple.keys
        key_column_names = sorted(keys._fields)
        # fmt: off
        key_columns = "".join(
            [
                f"{name}, -- key\n    "
                for name in key_column_names
            ]
        )
        # fmt: on

        values = self.mapping_tuple.values
        value_column_names = sorted(values._fields)
        # fmt: off
        value_columns = "".join(
            [
                f"{name}, -- value\n    "
                for name in value_column_names
            ]
        )
        value_values = "".join(
            [
                f"excluded.{name}, -- value\n    "
                for name in value_column_names
            ]
        )
        # fmt: on

        all_columns = key_columns + value_columns
        all_columns_count = len(key_column_names) + len(value_column_names)
        all_values = "".join(["?,\n    " for _ in range(0, all_columns_count)])

        upsert_stmt = ""
        if value_column_names:
            upsert_stmt = self._UPSERT_STMT_FMT.format(
                value_columns=value_columns,
                value_values=value_values,
                key_columns=key_columns,
            )

        unstripped_upsert_statement = self._UPSERT_FMT.format(
            table_identifier=table_identifier,
            all_columns=all_columns,
            all_values=all_values,
            upsert_stmt=upsert_stmt,
        )

        upsert_lines = []
        for line in unstripped_upsert_statement.splitlines():
            upsert_lines.append(line.rstrip())
        # needed for trailing newline
        upsert_lines.append("")
        self._upsert_statement = "\n".join(upsert_lines)
        return self._upsert_statement

    @classmethod
    def _handle_column(cls, *, column_dict, validated_name, sqltype):
        validated_sqltype = cls._validate_sqltype(sqltype=sqltype)
        column_dict[validated_name] = validated_sqltype

    # fmt: off
    _IDENTIFIER_RE_DEFN = (
        r"^               # start of string""\n"
        r"[a-z]           # start with an ascii letter""\n"
        r"[a-z0-9_]{0,62} # followed by up to 62 alphanumeric or underscores""\n"
        r"$               # end of string""\n"
    )
    # fmt: on

    _IDENTIFIER_PATTERN = re.compile(
        _IDENTIFIER_RE_DEFN, flags=(re.ASCII | re.IGNORECASE | re.VERBOSE),
    )

    @classmethod
    def _validate_identifier(cls, *, identifier):
        if identifier != identifier.strip():
            log.info(
                (
                    "sqlitecaching identifier provided: [%s] has whitespace "
                    "which will be stripped."
                ),
                identifier,
            )
            identifier = identifier.strip()
        match = cls._IDENTIFIER_PATTERN.match(identifier)
        if not match:
            fmt = (
                "sqlitecaching identifier provided: [%s] does not match "
                "requirements [%s]"
            )
            log.error(
                fmt, identifier, cls._IDENTIFIER_RE_DEFN,
            )
            raise CacheDictException(fmt % (identifier, cls._IDENTIFIER_RE_DEFN))
        lower_identifier = identifier.lower()
        if identifier != lower_identifier:
            log.warning(
                (
                    "sqlitecaching identifier [%s] is not lowercase. Using "
                    "lowercased value [%s]"
                ),
                identifier,
                lower_identifier,
            )

        return lower_identifier

    @classmethod
    def _validate_sqltype(cls, *, sqltype):
        if sqltype != sqltype.strip():
            log.info(
                (
                    "sqlitecaching sqltype provided: [%s] has whitespace "
                    "which will be stripped."
                ),
                sqltype,
            )
            sqltype = sqltype.strip()
        if not sqltype:
            return ""
        match = cls._IDENTIFIER_PATTERN.match(sqltype)
        if not match:
            fmt = (
                "sqlitecaching sqltype provided: [%s] does not match "
                "requirements [%s]"
            )
            log.error(
                fmt, sqltype, cls._IDENTIFIER_RE_DEFN,
            )
            raise CacheDictException(fmt % (sqltype, cls._IDENTIFIER_RE_DEFN))
        upper_sqltype = sqltype.upper()
        if sqltype != upper_sqltype:
            log.warning(
                (
                    "sqlitecaching sqltype [%s] is not uppercase. Using "
                    "uppercased value [%s]"
                ),
                sqltype,
                upper_sqltype,
            )

        if upper_sqltype not in sqlite3.converters:
            log.warn(
                (
                    "sqltype [%s] is not currently present in sqlite3.converters. "
                    "if sqlite cannot default convert, it may be returned as bytes()"
                ),
                upper_sqltype,
            )
        return upper_sqltype


class CacheDictException(Exception):
    pass
