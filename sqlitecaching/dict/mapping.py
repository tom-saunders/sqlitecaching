import collections
import logging
import re
import sqlite3
import typing

from sqlitecaching.exceptions import SqliteCachingException

log = logging.getLogger(__name__)


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
        "the sets of key columns and value columns must be disjoint. columns "
        "[{columns}] occur in both key and value sets"
    ),
    params=frozenset(["columns"]),
)
CacheDictMappingNoIdentifierProvidedException = __CDME.register_cause(
    cause_name=f"{__name__}.NoIdentifierProvidedException",
    cause_id=4,
    fmt="The identifier provided: [{identifier}] does not have a value.",
    params=frozenset(["identifier"]),
)
CacheDictMappingDuplicateKeysException = __CDME.register_cause(
    cause_name=f"{__name__}.DuplicateKeysException",
    cause_id=5,
    fmt="Duplicate key column identifiers provided: [{dups}]",
    params=frozenset(["dups"]),
)
CacheDictMappingInvalidSQLTypeException = __CDME.register_cause(
    cause_name=f"{__name__}.InvalidSQLTypeException",
    cause_id=6,
    fmt="sqltype provided: [{sqltype}] does not match requirements [{re}]",
    params=frozenset(["sqltype", "re"]),
)
CacheDictMappingDuplicateValuesException = __CDME.register_cause(
    cause_name=f"{__name__}.DuplicateValuesException",
    cause_id=7,
    fmt="Duplicate value column identifiers provided: [{dups}]",
    params=frozenset(["dups"]),
)

Ident = typing.NewType("Ident", str)
IdentIn = typing.Union[Ident, str]
SqlType = typing.NewType("SqlType", str)
SqlTypeIn = typing.Union[SqlType, str]

ValidIdent = typing.NewType("ValidIdent", str)
ValidSqlType = typing.NewType("ValidSqlType", str)

SqlStatement = typing.NewType("SqlStatement", str)

ColMapping = typing.Mapping[ValidIdent, ValidSqlType]


class IdentClash(typing.NamedTuple):
    original: Ident
    clashes: typing.FrozenSet[Ident]


class ColInfo(typing.NamedTuple):
    original: Ident
    sqltype: ValidSqlType


class CacheDictMapping:
    table_ident: ValidIdent
    keys: ColMapping
    values: ColMapping

    _create_statement: typing.Optional[SqlStatement]
    _clear_statement: typing.Optional[SqlStatement]
    _delete_statement: typing.Optional[SqlStatement]
    _upsert_statement: typing.Optional[SqlStatement]
    _remove_statement: typing.Optional[SqlStatement]
    _length_statement: typing.Optional[SqlStatement]
    _keys_statement: typing.Optional[SqlStatement]
    _items_statement: typing.Optional[SqlStatement]
    _values_statement: typing.Optional[SqlStatement]

    def __init__(
        self,
        *,
        table: Ident,
        keys: typing.Mapping[IdentIn, typing.Optional[SqlTypeIn]],
        values: typing.Optional[typing.Mapping[IdentIn, typing.Optional[SqlTypeIn]]],
    ):
        if not keys:
            raise CacheDictMappingMissingKeysException(params={"no_keys": keys})

        _keys = {
            Ident(column): (SqlType(sqltype) if sqltype else None)
            for (column, sqltype) in keys.items()
        }

        if not values:
            log.info("providing empty dict for values")
            _values = {}
        else:
            _values = {
                Ident(column): (SqlType(sqltype) if sqltype else None)
                for (column, sqltype) in values.items()
            }

        validated_table = self._validate_identifier(identifier=table)
        if validated_table.startswith("'sqlite_"):
            raise CacheDictMappingReservedTableException(
                params={"table_name": validated_table},
            )

        key_columns: typing.Dict[ValidIdent, ColInfo] = collections.OrderedDict()
        value_columns: typing.Dict[ValidIdent, ColInfo] = collections.OrderedDict()

        dup_key_columns: typing.Dict[ValidIdent, IdentClash] = {}
        keyval_columns: typing.Dict[ValidIdent, IdentClash] = {}
        dup_value_columns: typing.Dict[ValidIdent, IdentClash] = {}

        unset_value = ColInfo(Ident("PLACE.HOLDER"), ValidSqlType("PLACE.HOLDER"))
        for (name, sqltype) in _keys.items():
            validated_name = self._validate_identifier(identifier=name)
            in_keys = key_columns.get(validated_name, unset_value)
            if in_keys is not unset_value:
                original_name = in_keys.original
                dup_cols = dup_key_columns.get(
                    validated_name,
                    IdentClash(original_name, frozenset()),
                )
                dup_key_columns[validated_name] = IdentClash(
                    original_name,
                    dup_cols.clashes | frozenset([name]),
                )
            else:
                self._handle_column(
                    column_dict=key_columns,
                    original_name=name,
                    validated_name=validated_name,
                    sqltype=sqltype,
                )

        for (name, sqltype) in _values.items():
            validated_name = self._validate_identifier(identifier=name)
            in_keys = key_columns.get(validated_name, unset_value)
            in_values = value_columns.get(validated_name, unset_value)
            if in_keys is not unset_value:
                original_name = in_keys.original
                keyval_cols = keyval_columns.get(
                    validated_name,
                    IdentClash(original_name, frozenset()),
                )
                keyval_columns[validated_name] = IdentClash(
                    original_name,
                    keyval_cols.clashes | frozenset([name]),
                )
            elif in_values is not unset_value:
                original_name = in_values.original
                val_cols = dup_value_columns.get(
                    validated_name,
                    IdentClash(original_name, frozenset()),
                )
                dup_value_columns[validated_name] = IdentClash(
                    original_name,
                    val_cols.clashes | frozenset([name]),
                )
            else:
                self._handle_column(
                    column_dict=value_columns,
                    original_name=name,
                    validated_name=validated_name,
                    sqltype=sqltype,
                )

        # This is somewhat abusing the exception handling process...
        # Note we're treating exceptions as a stack so doing this in the
        # reversed order
        to_raise = None

        if dup_value_columns:
            val_strs: typing.List[str] = []
            for (key, val) in dup_value_columns.items():
                dup_idents = "', '".join(val.clashes)
                val_str = (
                    f"column [{key}] (input [{val.original}]) clashes with "
                    f"['{dup_idents}']"
                )
                val_strs.append(val_str)
            dup_val_str = ", ".join(val_strs)
            to_raise = CacheDictMappingDuplicateValuesException(
                params={"dups": dup_val_str},
            )

        if keyval_columns:
            keyval_str = "'" + "', '".join(keyval_columns) + "'"
            ex = CacheDictMappingKeyValOverlapException(params={"columns": keyval_str})
            ex.__cause__ = to_raise
            to_raise = ex

        if dup_key_columns:
            key_strs: typing.List[str] = []
            for (key, val) in dup_key_columns.items():
                dup_idents = "', '".join(val.clashes)
                key_str = (
                    f"column [{key}] (input [{val.original}]) clashes with "
                    f"['{dup_idents}']"
                )
                key_strs.append(key_str)
            dup_key_str = ", ".join(key_strs)
            ex = CacheDictMappingDuplicateKeysException(
                params={"dups": dup_key_str},
            )
            ex.__cause__ = to_raise
            to_raise = ex

        # TODO add some testing around the handling of multierrors...
        if to_raise:
            raise to_raise

        self.table_ident = validated_table
        self.keys = {
            ident: col_info.sqltype for (ident, col_info) in key_columns.items()
        }
        self.values = {
            ident: col_info.sqltype for (ident, col_info) in value_columns.items()
        }

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
    _CREATE_FMT: typing.ClassVar[str] = (
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
    _PRIMARY_KEY_FMT: typing.ClassVar[str] = (
        "PRIMARY KEY (\n"
        "        {primary_key_columns}\n"
        "    ) ON CONFLICT ABORT"
    )
    # fmt: on

    def create_statement(self) -> SqlStatement:
        if self._create_statement:
            return self._create_statement

        key_columns = sorted(self.keys.keys())

        value_columns = sorted(self.values.keys())

        # fmt: off
        key_column_definitions = ", -- primary key\n    ".join(
            [
                f"{column} {self.keys[column]}"
                for column in key_columns
            ],
        )
        key_column_definitions += ", -- primary key"

        if value_columns:
            value_column_definitions = ", -- value\n    ".join(
                [
                    f"{column} {self.values[column]}"
                    for column in value_columns
                ],
            )
            value_column_definitions += ", -- value"
        else:
            value_column_definitions = "-- no values defined"
        # fmt: on

        primary_key_columns = ",\n        ".join(key_columns)
        primary_key_definition = self._PRIMARY_KEY_FMT.format(
            primary_key_columns=primary_key_columns,
        )

        unstripped_create_statement = self._CREATE_FMT.format(
            table_identifier=self.table_ident,
            key_column_definitions=key_column_definitions,
            value_column_definitions=value_column_definitions,
            primary_key_definition=primary_key_definition,
        )

        create_lines = []
        for line in unstripped_create_statement.splitlines():
            create_lines.append(line.rstrip())
        # needed for trailing newline
        create_lines.append("")
        create_statement = SqlStatement("\n".join(create_lines))
        self._create_statement = create_statement
        return create_statement

    # fmt: off
    _CLEAR_FMT: typing.ClassVar[str] = (
        "-- sqlitecaching clear table\n"
        "DELETE from {table_identifier};\n"
    )
    # fmt: on

    def clear_statement(self) -> SqlStatement:
        if self._clear_statement:
            return self._clear_statement

        unstripped_clear_statement = self._CLEAR_FMT.format(
            table_identifier=self.table_ident,
        )

        clear_lines = []
        for line in unstripped_clear_statement.splitlines():
            clear_lines.append(line.rstrip())
        # needed for trailing newline
        clear_lines.append("")
        clear_statement = SqlStatement("\n".join(clear_lines))
        self._clear_statement = clear_statement
        return clear_statement

    # fmt: off
    _DELETE_FMT: typing.ClassVar[str] = (
        "-- sqlitecaching delete table\n"
        "DROP TABLE {table_identifier};\n"
    )
    # fmt: on

    def delete_statement(self) -> SqlStatement:
        if self._delete_statement:
            return self._delete_statement

        unstripped_delete_statement = self._DELETE_FMT.format(
            table_identifier=self.table_ident,
        )

        delete_lines = []
        for line in unstripped_delete_statement.splitlines():
            delete_lines.append(line.rstrip())
        # needed for trailing newline
        delete_lines.append("")
        delete_statement = SqlStatement("\n".join(delete_lines))
        self._delete_statement = delete_statement
        return delete_statement

    # fmt: off
    _UPSERT_FMT: typing.ClassVar[str] = (
        "-- sqlitecaching insert or update into table\n"
        "INSERT INTO {table_identifier}\n"
        "(\n"
        "    -- all columns\n"
        "    {all_columns}\n"
        ") VALUES (\n"
        "    -- all values\n"
        "    {all_values}\n"
        ") ON CONFLICT {upsert_stmt}\n"
        ";\n"
    )
    _UPSERT_STMT_FMT: typing.ClassVar[str] = (
        "(\n"
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

    def upsert_statement(self) -> SqlStatement:
        if self._upsert_statement:
            return self._upsert_statement

        key_column_names = sorted(self.keys.keys())

        value_column_names = sorted(self.values.keys())

        key_columns = ", -- key\n    ".join(key_column_names)
        all_columns = key_columns
        key_columns += " -- key"

        if value_column_names:
            all_columns += ", -- key\n    "
            value_columns = ", -- value\n    ".join(value_column_names)
            value_columns += " -- value"
            all_columns += value_columns

            value_values = ", -- value\n    ".join(
                [f"excluded.{c}" for c in value_column_names],
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
            upsert_stmt = "DO NOTHING\n"
            upsert_stmt += "-- no conflict action as no values defined"

        all_values = ",\n    ".join(
            ["?" for _ in range(0, len(key_column_names) + len(value_column_names))],
        )
        unstripped_upsert_statement = self._UPSERT_FMT.format(
            table_identifier=self.table_ident,
            all_columns=all_columns,
            all_values=all_values,
            upsert_stmt=upsert_stmt,
        )

        upsert_lines = []
        for line in unstripped_upsert_statement.splitlines():
            upsert_lines.append(line.rstrip())
        # needed for trailing newline
        upsert_lines.append("")
        upsert_statement = SqlStatement("\n".join(upsert_lines))
        self._upsert_statement = upsert_statement
        return upsert_statement

    # fmt: off
    _REMOVE_FMT: typing.ClassVar[str] = (
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

    def remove_statement(self) -> SqlStatement:
        if self._remove_statement:
            return self._remove_statement

        key_column_names = sorted(self.keys.keys())
        key_columns = ", -- key\n    ".join(key_column_names)
        key_columns += " -- key"

        key_columns_count = len(key_column_names)
        key_values = ",\n    ".join(["?" for _ in range(0, key_columns_count)])
        unstripped_remove_statement = self._REMOVE_FMT.format(
            table_identifier=self.table_ident,
            key_columns=key_columns,
            key_values=key_values,
        )

        remove_lines = []
        for line in unstripped_remove_statement.splitlines():
            remove_lines.append(line.rstrip())
        # needed for trailing newline
        remove_lines.append("")
        remove_statement = SqlStatement("\n".join(remove_lines))
        self._remove_statement = remove_statement
        return remove_statement

    # fmt: off
    _LENGTH_FMT: typing.ClassVar[str] = (
        "-- sqlitecaching table length\n"
        "SELECT COUNT(*) FROM {table_identifier};\n"
    )
    # fmt: on

    def length_statement(self) -> SqlStatement:
        if self._length_statement:
            return self._length_statement

        unstripped_length_statement = self._LENGTH_FMT.format(
            table_identifier=self.table_ident,
        )

        length_lines = []
        for line in unstripped_length_statement.splitlines():
            length_lines.append(line.rstrip())
        # needed for trailing newline
        length_lines.append("")
        length_statement = SqlStatement("\n".join(length_lines))
        self._length_statement = length_statement
        return length_statement

    # fmt: off
    _KEYS_FMT: typing.ClassVar[str] = (
        "-- sqlitecaching table keys\n"
        "SELECT\n"
        "    {key_columns}\n"
        "FROM {table_identifier};\n"
    )
    # fmt: on

    def keys_statement(self) -> SqlStatement:
        if self._keys_statement:
            return self._keys_statement

        key_column_names = sorted(self.keys.keys())
        key_columns = ", -- key\n    ".join(key_column_names)
        key_columns += " -- key"

        unstripped_keys_statement = self._KEYS_FMT.format(
            key_columns=key_columns,
            table_identifier=self.table_ident,
        )

        keys_lines = []
        for line in unstripped_keys_statement.splitlines():
            keys_lines.append(line.rstrip())
        # needed for trailing newline
        keys_lines.append("")
        keys_statement = SqlStatement("\n".join(keys_lines))
        self._keys_statement = keys_statement
        return keys_statement

    # fmt: off
    _ITEMS_FMT: typing.ClassVar[str] = (
        "-- sqlitecaching table items\n"
        "SELECT\n"
        "    -- all columns\n"
        "    {all_columns}\n"
        "FROM {table_identifier};\n"
    )
    # fmt: on

    def items_statement(self) -> SqlStatement:
        if self._items_statement:
            return self._items_statement

        key_column_names = sorted(self.keys.keys())
        all_columns = ", -- key\n    ".join(key_column_names)

        value_column_names = sorted(self.values.keys())
        if value_column_names:
            all_columns += ", -- key\n    "
            all_columns += ", -- value\n    ".join(value_column_names)
            all_columns += " -- value"
        else:
            all_columns += " -- key"

        unstripped_items_statement = self._ITEMS_FMT.format(
            all_columns=all_columns,
            table_identifier=self.table_ident,
        )

        items_lines = []
        for line in unstripped_items_statement.splitlines():
            items_lines.append(line.rstrip())
        # needed for trailing newline
        items_lines.append("")
        items_statement = SqlStatement("\n".join(items_lines))
        self._items_statement = items_statement
        return items_statement

    # fmt: off
    _VALUES_FMT: typing.ClassVar[str] = (
        "-- sqlitecaching table values\n"
        "SELECT\n"
        "    {value_columns}\n"
        "FROM {table_identifier};\n"
    )
    # fmt: on

    def values_statement(self) -> SqlStatement:
        if self._values_statement:
            return self._values_statement

        value_column_names = sorted(self.values.keys())
        if value_column_names:
            value_columns = ", -- value\n    ".join(value_column_names)
            value_columns += " -- value"
        else:
            value_columns = "null -- null value to permit querying"

        unstripped_values_statement = self._VALUES_FMT.format(
            value_columns=value_columns,
            table_identifier=self.table_ident,
        )

        values_lines = []
        for line in unstripped_values_statement.splitlines():
            values_lines.append(line.rstrip())
        # needed for trailing newline
        values_lines.append("")
        values_statement = SqlStatement("\n".join(values_lines))
        self._values_statement = values_statement
        return values_statement

    @classmethod
    def _handle_column(
        cls,
        *,
        column_dict: typing.Dict[ValidIdent, ColInfo],
        original_name: Ident,
        validated_name: ValidIdent,
        sqltype: typing.Optional[SqlType],
    ) -> None:
        validated_sqltype = cls._validate_sqltype(sqltype=sqltype)
        column_dict[validated_name] = ColInfo(original_name, validated_sqltype)

    # fmt: off
    _IDENTIFIER_RE_DEFN: typing.ClassVar[str] = (
        r"^               # start of string""\n"
        r"[a-z]           # start with an ascii letter""\n"
        r"[a-z0-9_]{0,62} # followed by up to 62 alphanumeric or underscores""\n"
        r"$               # end of string""\n"
    )
    # fmt: on

    _IDENTIFIER_PATTERN: typing.ClassVar[typing.Pattern[str]] = re.compile(
        _IDENTIFIER_RE_DEFN,
        flags=(re.ASCII | re.IGNORECASE | re.VERBOSE),
    )

    @classmethod
    def _validate_identifier(cls, *, identifier: Ident) -> ValidIdent:
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
            identifier = Ident(identifier.strip())
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

        return ValidIdent(f"'{lower_identifier}'")

    @classmethod
    def _validate_sqltype(cls, *, sqltype: typing.Optional[SqlType]) -> ValidSqlType:
        if not sqltype:
            log.info(
                "sqltype provided [%s] will be replaced with an empty string",
                sqltype,
            )
            sqltype = SqlType("")
        elif sqltype != sqltype.strip():
            log.info(
                (
                    "sqlitecaching sqltype provided: [%s] has whitespace "
                    "which will be stripped."
                ),
                sqltype,
            )
            sqltype = SqlType(sqltype.strip())
        if not sqltype:
            return ValidSqlType("")
        match = cls._IDENTIFIER_PATTERN.match(sqltype)
        if not match:
            raise CacheDictMappingInvalidSQLTypeException(
                params={"sqltype": sqltype, "re": cls._IDENTIFIER_RE_DEFN},
            )
        upper_sqltype = ValidSqlType(sqltype.upper())
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
            log.warning(
                (
                    "sqltype [%s] is not currently present in sqlite3.converters. "
                    "if sqlite cannot default convert, it may be returned as bytes()"
                ),
                upper_sqltype,
            )
        return upper_sqltype
