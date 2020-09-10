import collections
import logging
import re
import sqlite3
from collections import namedtuple

from sqlitecaching.exceptions import SqliteCachingException

log = logging.getLogger(__name__)

CacheDictMappingTuple = namedtuple("CacheDictMappingTuple", ["table", "keys", "values"])


CacheDictMappingException = SqliteCachingException.register_category(
    category_name=f"{__name__}.CacheDictMappingException",
    category_id=2,
)
__CDME = CacheDictMappingException

CacheDictMappingMissingKeysException = __CDME.register_cause(
    cause_name=f"{__name__}.MappingMissingKeys",
    cause_id=0,
    fmt="Mapping must have keys, provided: [{no_keys}]",
    params=frozenset(["no_keys"]),
)
CacheDictMappingReservedTableException = __CDME.register_cause(
    cause_name=f"{__name__}.ReservedTableException",
    cause_id=1,
    fmt="table cannot start with sqlite_ : [{table_name}]",
    params=frozenset(["table_name"]),
)
CacheDictMappingInvalidIdentifierException = __CDME.register_cause(
    cause_name=f"{__name__}.InvalidIdentifierException",
    cause_id=2,
    fmt="identifier provided: [{identifier}] does not match requirements [{re}]",
    params=frozenset(["identifier", "re"]),
)
CacheDictMappingKeyValOverlapException = __CDME.register_cause(
    cause_name=f"{__name__}.KeyValColumnOverlapException",
    cause_id=3,
    fmt=(
        "the sets of key columns and value columns must be disjoint. columns [%s] "
        "occur in both key and value sets"
    ),
    params=frozenset(["columns"]),
)
CacheDictMappingNoIdentifierProvidedException = __CDME.register_cause(
    cause_name=f"{__name__}.NoIdentifierProvidedException",
    cause_id=4,
    fmt="The identifier provided: [%s] does not have a value.",
    params=frozenset(["identifier"]),
)
CacheDictMappingDuplicateKeyNameException = __CDME.register_cause(
    cause_name=f"{__name__}.DuplicateKeyNameException",
    cause_id=5,
    fmt=(
        "The key column identifier provided: [%s] has already been added (as "
        "[%s], after casefold())"
    ),
    params=frozenset(["identifier", "validated_identifier"]),
)
CacheDictMappingInvalidSQLTypeException = __CDME.register_cause(
    cause_name=f"{__name__}.InvalidSQLTypeException",
    cause_id=6,
    fmt="sqltype provided: [%s] does not match requirements [%s]",
    params=frozenset(["sqltype", "re"]),
)


class CacheDictMapping:
    def __init__(self, *, table, keys, values):
        if not keys:
            raise CacheDictMappingMissingKeysException(params={"no_keys": keys})

        validated_table = self._validate_identifier(identifier=table)
        if validated_table.startswith("sqlite_"):
            raise CacheDictMappingReservedTableException(
                params={"table_name": validated_table},
            )

        key_columns = collections.OrderedDict()
        value_columns = collections.OrderedDict()
        keyval_columns = []

        unset_value = object()
        for (name, sqltype) in keys.items():
            validated_name = self._validate_identifier(identifier=name)
            in_keys = key_columns.get(validated_name, unset_value)
            if in_keys is not unset_value:
                raise CacheDictMappingDuplicateKeyNameException(
                    params={"identifier": name, "validated_identifier": validated_name},
                )
            else:
                self._handle_column(
                    column_dict=key_columns,
                    validated_name=validated_name,
                    sqltype=sqltype,
                )

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
            keyval_str = "'" + "', '".join(keyval_columns) + "'"
            raise CacheDictMappingKeyValOverlapException(params={"columns": keyval_str})

        self.Keys = namedtuple("Keys", sorted(key_columns.keys()))
        self.Values = namedtuple("Values", sorted(value_columns.keys()))

        self.key_info = self.Keys(**key_columns)
        self.value_info = self.Values(**value_columns)
        self.mapping_tuple = CacheDictMappingTuple(
            table=validated_table,
            keys=self.key_info,
            values=self.value_info,
        )

        self._create_statement = None
        self._clear_statement = None
        self._delete_statement = None
        self._upsert_statement = None
        self._remove_statement = None
        self._length_statement = None
        self._keys_statement = None
        self._items_statement = None
        self._values_statement = None

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

        table_identifier = f"'{self.mapping_tuple.table}'"

        keys = self.mapping_tuple.keys
        key_columns = sorted(keys._fields)

        values = self.mapping_tuple.values
        value_columns = sorted(values._fields)

        # fmt: off
        key_column_definitions = ", -- primary key\n    ".join(
            [
                f"'{column}' {getattr(keys, column)}"
                for column in key_columns
            ],
        )
        key_column_definitions += ", -- primary key"

        if value_columns:
            value_column_definitions = ", -- value\n    ".join(
                [
                    f"'{column}' {getattr(values, column)}"
                    for column in value_columns
                ],
            )
            value_column_definitions += ", -- value"
        else:
            value_column_definitions = "-- no values defined"
        # fmt: on

        primary_key_columns = "'"
        primary_key_columns += "',\n        '".join(key_columns)
        primary_key_columns += "'"
        primary_key_definition = self._PRIMARY_KEY_FMT.format(
            primary_key_columns=primary_key_columns,
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

        table_identifier = f"'{self.mapping_tuple.table}'"

        unstripped_clear_statement = self._CLEAR_FMT.format(
            table_identifier=table_identifier,
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

        table_identifier = f"'{self.mapping_tuple.table}'"

        unstripped_delete_statement = self._DELETE_FMT.format(
            table_identifier=table_identifier,
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
        "    -- all columns\n"
        "    {all_columns}\n"
        ") VALUES (\n"
        "    -- all values\n"
        "    {all_values}\n"
        "){upsert_stmt}\n"
        ";\n"
    )
    _UPSERT_STMT_FMT = (
        " ON CONFLICT (\n"
        "    -- key columns\n"
        "    {key_columns}\n"
        ") DO UPDATE SET (\n"
        "    -- value columns\n"
        "    {value_columns}\n"
        ") = (\n"
        "    -- value values\n"
        "    {value_values}\n"
        ")"
    )
    # fmt: on

    def upsert_statement(self):
        if self._upsert_statement:
            return self._upsert_statement

        table_identifier = f"'{self.mapping_tuple.table}'"

        keys = self.mapping_tuple.keys
        key_column_names = sorted(keys._fields)

        values = self.mapping_tuple.values
        value_column_names = sorted(values._fields)

        key_columns = "'"
        key_columns += "', -- key\n    '".join(key_column_names)
        key_columns += "'"
        all_columns = key_columns
        key_columns += " -- key"

        if value_column_names:
            all_columns += ", -- key\n    "
            value_columns = "'"
            value_columns += "', -- value\n    '".join(value_column_names)
            value_columns += "' -- value"
            all_columns += value_columns

            value_values = ", -- value\n    ".join(
                [f"excluded.'{c}'" for c in value_column_names],
            )
            value_values += " -- value"

            upsert_stmt = self._UPSERT_STMT_FMT.format(
                value_columns=value_columns,
                value_values=value_values,
                key_columns=key_columns,
            )
        else:
            all_columns += " -- key\n    "
            all_columns += "-- no values defined"
            upsert_stmt = " DO NOTHING\n"
            upsert_stmt += "-- no conflict action as no values defined"

        all_values = ",\n    ".join(
            ["?" for _ in range(0, len(key_column_names) + len(value_column_names))],
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

    # fmt: off
    _REMOVE_FMT = (
        "-- sqlitecaching remove from table\n"
        "DELETE FROM {table_identifier}\n"
        "WHERE (\n"
        "    -- key columns\n"
        "    {key_columns}\n"
        ") = (\n"
        "    -- key values\n"
        "    {key_values}\n"
        ");\n"
    )
    # fmt: on

    def remove_statement(self):
        if self._remove_statement:
            return self._remove_statement

        table_identifier = f"'{self.mapping_tuple.table}'"

        keys = self.mapping_tuple.keys
        key_column_names = sorted(keys._fields)
        key_columns = "'"
        key_columns += "', -- key\n    '".join(key_column_names)
        key_columns += "' -- key"

        key_columns_count = len(key_column_names)
        key_values = ",\n    ".join(["?" for _ in range(0, key_columns_count)])
        unstripped_remove_statement = self._REMOVE_FMT.format(
            table_identifier=table_identifier,
            key_columns=key_columns,
            key_values=key_values,
        )

        remove_lines = []
        for line in unstripped_remove_statement.splitlines():
            remove_lines.append(line.rstrip())
        # needed for trailing newline
        remove_lines.append("")
        self._remove_statement = "\n".join(remove_lines)
        return self._remove_statement

    # fmt: off
    _LENGTH_FMT = (
        "-- sqlitecaching table length\n"
        "SELECT COUNT(*) FROM {table_identifier};\n"
    )
    # fmt: on

    def length_statement(self):
        if self._length_statement:
            return self._length_statement

        table_identifier = f"'{self.mapping_tuple.table}'"

        unstripped_length_statement = self._LENGTH_FMT.format(
            table_identifier=table_identifier,
        )

        length_lines = []
        for line in unstripped_length_statement.splitlines():
            length_lines.append(line.rstrip())
        # needed for trailing newline
        length_lines.append("")
        self._length_statement = "\n".join(length_lines)
        return self._length_statement

    # fmt: off
    _KEYS_FMT = (
        "-- sqlitecaching table keys\n"
        "SELECT\n"
        "    {key_columns}\n"
        "FROM {table_identifier};\n"
    )
    # fmt: on

    def keys_statement(self):
        if self._keys_statement:
            return self._keys_statement

        table_identifier = f"'{self.mapping_tuple.table}'"

        keys = self.mapping_tuple.keys
        key_column_names = sorted(keys._fields)
        key_columns = "'"
        key_columns += "', -- key\n    '".join(key_column_names)
        key_columns += "' -- key"

        unstripped_keys_statement = self._KEYS_FMT.format(
            key_columns=key_columns,
            table_identifier=table_identifier,
        )

        keys_lines = []
        for line in unstripped_keys_statement.splitlines():
            keys_lines.append(line.rstrip())
        # needed for trailing newline
        keys_lines.append("")
        self._keys_statement = "\n".join(keys_lines)
        return self._keys_statement

    # fmt: off
    _ITEMS_FMT = (
        "-- sqlitecaching table items\n"
        "SELECT\n"
        "    -- all columns\n"
        "    {all_columns}\n"
        "FROM {table_identifier};\n"
    )
    # fmt: on

    def items_statement(self):
        if self._items_statement:
            return self._items_statement

        table_identifier = f"'{self.mapping_tuple.table}'"

        keys = self.mapping_tuple.keys
        key_column_names = sorted(keys._fields)
        all_columns = "'"
        all_columns += "', -- key\n    '".join(key_column_names)

        values = self.mapping_tuple.values
        value_column_names = sorted(values._fields)
        if value_column_names:
            all_columns += "', -- key\n    '"
            all_columns += "', -- value\n    '".join(value_column_names)
            all_columns += "' -- value"
        else:
            all_columns += "' -- key"

        unstripped_items_statement = self._ITEMS_FMT.format(
            all_columns=all_columns,
            table_identifier=table_identifier,
        )

        items_lines = []
        for line in unstripped_items_statement.splitlines():
            items_lines.append(line.rstrip())
        # needed for trailing newline
        items_lines.append("")
        self._items_statement = "\n".join(items_lines)
        return self._items_statement

    # fmt: off
    _VALUES_FMT = (
        "-- sqlitecaching table values\n"
        "SELECT\n"
        "    {value_columns}\n"
        "FROM {table_identifier};\n"
    )
    # fmt: on

    def values_statement(self):
        if self._values_statement:
            return self._values_statement

        table_identifier = f"'{self.mapping_tuple.table}'"

        values = self.mapping_tuple.values
        value_column_names = sorted(values._fields)
        if value_column_names:
            value_columns = "'"
            value_columns += "', -- value\n    '".join(value_column_names)
            value_columns += "' -- value"
        else:
            value_columns = "null -- null value to permit querying"

        unstripped_values_statement = self._VALUES_FMT.format(
            value_columns=value_columns,
            table_identifier=table_identifier,
        )

        values_lines = []
        for line in unstripped_values_statement.splitlines():
            values_lines.append(line.rstrip())
        # needed for trailing newline
        values_lines.append("")
        self._values_statement = "\n".join(values_lines)
        return self._values_statement

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
        _IDENTIFIER_RE_DEFN,
        flags=(re.ASCII | re.IGNORECASE | re.VERBOSE),
    )

    @classmethod
    def _validate_identifier(cls, *, identifier):
        if not identifier:
            raise CacheDictMappingNoIdentifierProvidedException(
                params={"identifier": identifier},
            )

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
                fmt,
                identifier,
                cls._IDENTIFIER_RE_DEFN,
            )
            raise CacheDictMappingInvalidIdentifierException(
                params={"identifier": identifier, "re": cls._IDENTIFIER_RE_DEFN},
            )
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
            raise CacheDictMappingInvalidSQLTypeException(
                params={"sqltype": sqltype, "re": cls._IDENTIFIER_RE_DEFN},
            )
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
