import dataclasses
import logging
import re
import sqlite3
import typing

from sqlitecaching.exceptions import SqliteCachingException

log = logging.getLogger(__name__)


try:
    _ = CacheDictMappingCategory  # type: ignore
    log.info("Not redefining exceptions")  # pragma: no cover
except NameError:
    CacheDictMappingCategory = SqliteCachingException.register_category(
        category_name=f"{__name__}.CacheDictMappingCategory",
        category_id=2,
    )
    __CDMC = CacheDictMappingCategory

    CacheDictMappingMissingKeysException = __CDMC.register_cause(
        cause_name=f"{__name__}.MappingMissingKeys",
        cause_id=0,
        fmt="Mapping must have keys, provided: [{no_keys}]",
        params=frozenset(["no_keys"]),
    )
    CacheDictMappingReservedTableException = __CDMC.register_cause(
        cause_name=f"{__name__}.ReservedTableException",
        cause_id=1,
        fmt="table cannot start with sqlite_ : [{table_name}]",
        params=frozenset(["table_name"]),
    )
    CacheDictMappingInvalidIdentifierException = __CDMC.register_cause(
        cause_name=f"{__name__}.InvalidIdentifierException",
        cause_id=2,
        fmt="identifier provided: [{identifier}] does not match requirements [{re}]",
        params=frozenset(["identifier", "re"]),
    )
    CacheDictMappingKeyValOverlapException = __CDMC.register_cause(
        cause_name=f"{__name__}.KeyValColumnOverlapException",
        cause_id=3,
        fmt=(
            "the sets of key columns and value columns must be disjoint. columns "
            "[{columns}] occur in both key and value sets"
        ),
        params=frozenset(["columns"]),
    )
    CacheDictMappingNoIdentifierProvidedException = __CDMC.register_cause(
        cause_name=f"{__name__}.NoIdentifierProvidedException",
        cause_id=4,
        fmt="The identifier provided: [{identifier}] does not have a value.",
        params=frozenset(["identifier"]),
    )
    CacheDictMappingInvalidSQLParamTypeException = __CDMC.register_cause(
        cause_name=f"{__name__}.InvalidSQLParamTypeException",
        cause_id=5,
        fmt=(
            "SQL parameter type value was provided as a truthy value of type "
            "[{type}]. Parameter types must be strings."
        ),
        params=frozenset(["type"]),
    )
    CacheDictMappingInvalidSQLTypeException = __CDMC.register_cause(
        cause_name=f"{__name__}.InvalidSQLTypeException",
        cause_id=6,
        fmt="sqltype provided: [{sqltype}] does not match requirements [{re}]",
        params=frozenset(["sqltype", "re"]),
    )
    CacheDictMappingKeyTypeNotDataclassException = __CDMC.register_cause(
        cause_name=f"{__name__}.KeyTypeNotDataclassException",
        cause_id=7,
        fmt="Key type provided [{type}] is not a dataclass",
        params=frozenset(["type"]),
    )
    CacheDictMappingValueTypeNotDataclassException = __CDMC.register_cause(
        cause_name=f"{__name__}.ValueTypeNotDataclassException",
        cause_id=8,
        fmt="Value type provided [{type}] is not a dataclass",
        params=frozenset(["type"]),
    )
    CacheDictMappingIncorrectKeyTypesTypeException = __CDMC.register_cause(
        cause_name=f"{__name__}.IncorrectKeyTypesTypeException",
        cause_id=9,
        fmt=(
            "The type of the key_types parameter provided: [{key_types}] is not "
            "an instance of the key_type parameter [{key_type}]"
        ),
        params=frozenset(["key_types", "key_type"]),
    )
    CacheDictMappingIncorrectValueTypesTypeException = __CDMC.register_cause(
        cause_name=f"{__name__}.IncorrectValueTypesTypeException",
        cause_id=10,
        fmt=(
            "The type of the value_types parameter provided: [{value_types}] is "
            "not an instance of the value_type parameter [{value_type}]"
        ),
        params=frozenset(["value_types", "value_type"]),
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


KT = typing.TypeVar("KT")
VT = typing.TypeVar("VT")


class CacheDictMapping(typing.Generic[KT, VT]):
    table_ident: ValidIdent
    key_idents: typing.FrozenSet[ValidIdent]
    value_idents: typing.FrozenSet[ValidIdent]
    key_columns: ColMapping
    value_columns: ColMapping

    KeyType: typing.Type[KT]
    ValueType: typing.Type[VT]

    _create_statement: typing.Optional[SqlStatement] = None
    _clear_statement: typing.Optional[SqlStatement] = None
    _delete_statement: typing.Optional[SqlStatement] = None
    _upsert_statement: typing.Optional[SqlStatement] = None
    _select_statement: typing.Optional[SqlStatement] = None
    _remove_statement: typing.Optional[SqlStatement] = None
    _length_statement: typing.Optional[SqlStatement] = None
    _keys_statement: typing.Optional[SqlStatement] = None
    _items_statement: typing.Optional[SqlStatement] = None
    _values_statement: typing.Optional[SqlStatement] = None

    def __init__(
        self,
        *,
        table: IdentIn,
        key_type: typing.Type[KT],
        value_type: typing.Type[VT],
        key_types: typing.Optional[KT] = None,
        value_types: typing.Optional[VT] = None,
    ):
        self.table_ident = self._validate_ident(Ident(table))
        if self.table_ident.startswith('"sqlite_'):
            raise CacheDictMappingReservedTableException({"table_name": table})

        if not dataclasses.is_dataclass(key_type):
            raise CacheDictMappingKeyTypeNotDataclassException({"type": key_type})
        if not dataclasses.is_dataclass(value_type):
            raise CacheDictMappingValueTypeNotDataclassException({"type": value_type})

        self.key_idents = self._validate_idents(dataclasses.fields(key_type))
        self.value_idents = self._validate_idents(dataclasses.fields(value_type))

        if not self.key_idents:
            raise CacheDictMappingMissingKeysException({"no_keys": self.key_idents})

        overlap_idents = self.key_idents & self.value_idents
        if overlap_idents:
            raise CacheDictMappingKeyValOverlapException({"columns": overlap_idents})

        self.KeyType = key_type
        self.ValueType = value_type

        if key_types:
            if not isinstance(key_types, key_type):
                raise CacheDictMappingIncorrectKeyTypesTypeException(
                    {"key_types": type(key_types), "key_type": key_type},
                )
            self.key_columns = self._column_info(dataclasses.asdict(key_types))
        else:
            self.key_columns = {c: ValidSqlType("") for c in self.key_idents}
        if value_types:
            if not isinstance(value_types, value_type):
                raise CacheDictMappingIncorrectValueTypesTypeException(
                    {"value_types": type(value_types), "value_type": value_type},
                )
            self.value_columns = self._column_info(dataclasses.asdict(value_types))
        else:
            self.value_columns = {c: ValidSqlType("") for c in self.value_idents}

    @classmethod
    def _column_info(cls, types: typing.Mapping[str, SqlTypeIn], /) -> ColMapping:
        columns: typing.Dict[ValidIdent, ValidSqlType] = {}
        for (c, t) in types.items():
            c = cls._validate_ident(Ident(c))
            if t:
                if not isinstance(t, str):
                    raise CacheDictMappingInvalidSQLParamTypeException(
                        {"type": type(t)},
                    )
                c_sqltype = cls._validate_sqltype(SqlType(t))
            else:
                c_sqltype = ValidSqlType("")
            columns[c] = c_sqltype
        return columns

    @classmethod
    def _validate_idents(
        cls,
        fields: typing.Iterable[dataclasses.Field],
        /,
    ) -> typing.FrozenSet[ValidIdent]:
        field_names = [f.name for f in fields]
        valid_idents: typing.FrozenSet[ValidIdent] = frozenset([])
        for field in field_names:
            ident = Ident(field)
            valid_ident = cls._validate_ident(ident)
            valid_idents |= {valid_ident}
        return valid_idents

    # fmt: off
    _IDENTIFIER_RE_DEFN: typing.ClassVar[str] = (
        r"^               # start of string""\n"
        r"[a-z]           # start with a lowercase ascii letter""\n"
        r"[a-z0-9_]{0,62} # followed by 0-62 lowercase ascii/numbers/underscores""\n"
        r"$               # end of string""\n"
    )
    # fmt: on

    _IDENTIFIER_PATTERN: typing.ClassVar[typing.Pattern[str]] = re.compile(
        _IDENTIFIER_RE_DEFN,
        flags=(re.ASCII | re.VERBOSE),
    )

    @classmethod
    def _validate_ident(cls, ident: Ident, /) -> ValidIdent:
        if not ident:
            raise CacheDictMappingNoIdentifierProvidedException(
                {"identifier": ident},
            )

        match = cls._IDENTIFIER_PATTERN.match(ident)
        if not match:
            raise CacheDictMappingInvalidIdentifierException(
                {"identifier": ident, "re": cls._IDENTIFIER_RE_DEFN},
            )

        return ValidIdent(f'"{ident}"')

    # fmt: off
    _SQLTYPE_RE_DEFN: typing.ClassVar[str] = (
        r"^               # start of string""\n"
        r"[A-Z]           # start with a uppercase ascii letter""\n"
        r"[A-Z0-9_]{0,62} # followed by 0-62 uppercase ascii/numbers/underscores""\n"
        r"$               # end of string""\n"
    )
    # fmt: on

    _SQLTYPE_PATTERN: typing.ClassVar[typing.Pattern[str]] = re.compile(
        _SQLTYPE_RE_DEFN,
        flags=(re.ASCII | re.VERBOSE),
    )

    @classmethod
    def _validate_sqltype(cls, sqltype: SqlType, /) -> ValidSqlType:
        match = cls._SQLTYPE_PATTERN.match(sqltype)
        if not match:
            raise CacheDictMappingInvalidSQLTypeException(
                {"sqltype": sqltype, "re": cls._SQLTYPE_RE_DEFN},
            )

        if sqltype not in sqlite3.converters:
            log.warning(
                (
                    "sqltype [%s] is not currently present in sqlite3.converters. "
                    "if sqlite cannot default convert, it may be returned as bytes()"
                ),
                sqltype,
            )
        return ValidSqlType(sqltype)

    # fmt: off
    _CREATE_FMT: typing.ClassVar[str] = (
        "-- sqlitecaching create table\n"
        "CREATE TABLE IF NOT EXISTS {table_identifier}\n"
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

        key_columns = sorted(self.key_idents)

        value_columns = sorted(self.value_idents)

        # fmt: off
        key_column_definitions = ", -- primary key\n    ".join(
            [
                f"{column} {self.key_columns[column]}"
                for column in key_columns
            ],
        )
        key_column_definitions += ", -- primary key"

        if value_columns:
            value_column_definitions = ", -- value\n    ".join(
                [
                    f"{column} {self.value_columns[column]}"
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

        key_column_names = sorted(self.key_idents)

        value_column_names = sorted(self.value_idents)

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
    _SELECT_FMT: typing.ClassVar[str] = (
        "-- sqlitecaching retrieve from table\n"
        "SELECT\n"
        "    {value_columns}\n"
        "FROM {table_identifier}\n"
        "WHERE (\n"
        "    -- key columns\n"
        "    {key_columns}\n"
        ") = (\n"
        "    -- key values\n"
        "    {key_values}\n"
        ");\n"
    )
    # fmt: on

    def select_statement(self) -> SqlStatement:
        if self._select_statement:
            return self._select_statement

        value_column_names = sorted(self.value_idents)
        if value_column_names:
            value_columns = ", -- value\n    ".join(value_column_names)
            value_columns += " -- value"
        else:
            value_columns = "null -- no value columns so use null"

        key_column_names = sorted(self.key_idents)
        key_columns = ", -- key\n    ".join(key_column_names)
        key_columns += " -- key"

        key_columns_count = len(key_column_names)
        key_values = ",\n    ".join(["?" for _ in range(0, key_columns_count)])
        unstripped_select_statement = self._SELECT_FMT.format(
            table_identifier=self.table_ident,
            value_columns=value_columns,
            key_columns=key_columns,
            key_values=key_values,
        )

        select_lines = []
        for line in unstripped_select_statement.splitlines():
            select_lines.append(line.rstrip())
        # needed for trailing newline
        select_lines.append("")
        select_statement = SqlStatement("\n".join(select_lines))
        self._select_statement = select_statement
        return select_statement

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

        key_column_names = sorted(self.key_idents)
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

        key_column_names = sorted(self.key_idents)
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

        key_column_names = sorted(self.key_idents)
        all_columns = ", -- key\n    ".join(key_column_names)

        value_column_names = sorted(self.value_idents)
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

        value_column_names = sorted(self.value_idents)
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
