import itertools
import logging
from collections import namedtuple

import parameterized

from sqlitecaching.dict import CacheDictMapping, CacheDictMappingTuple
from sqlitecaching.test import CacheDictTestBase, TestLevel, test_level

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

# if this isn't defined here then the listcomps inside the class fail
Def = namedtuple("Def", ["name", "mapping", "statement_type", "expected"])


@test_level(TestLevel.PRE_COMMIT)
class TestCacheDictMapping(CacheDictTestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.res_dir += "mappings/"

    In = CacheDictMappingTuple
    InputDef = namedtuple("InputDef", ["filename", "mapping"])

    statement_types = [
        "create_statement",
        "clear_statement",
        "delete_statement",
        "upsert_statement",
        "remove_statement",
        "length_statement",
        "keys_statement",
        "items_statement",
        "values_statement",
    ]

    mapping_definitions = [
        InputDef(
            filename="aA__to__", mapping=In(table="aa", keys={"a": "A"}, values={},),
        ),
        InputDef(
            filename="aA__to__bB",
            mapping=In(table="aa__bb", keys={"a": "A"}, values={"b": "B"},),
        ),
        InputDef(
            filename="aA_bB__to__",
            mapping=In(table="aa_bb", keys={"a": "A", "b": "B"}, values={},),
        ),
        InputDef(
            filename="aA_bB__to__",
            mapping=In(table="aA_bB", keys={"a": "a", "b": "b"}, values={},),
        ),
        InputDef(
            filename="aA_bB__to__cC",
            mapping=In(table="aa_bb__cc", keys={"a": "a", "b": "b"}, values={"c": "C"}),
        ),
        InputDef(
            filename="aA_bB__to__cC_dD",
            mapping=In(
                table="aa_bb__cc_dd",
                keys={"a": "a", "b": "b"},
                values={"c": "C", "d": "D"},
            ),
        ),
        InputDef(
            filename="aA_bB__to__cC_dD",
            mapping=In(
                table="aa_bb__cc_dd",
                keys={"b": "B", "a": "A"},
                values={"d": "D", "c": "C"},
            ),
        ),
    ]

    create_mapping_success_params = [
        Def(
            name="%s_%s"
            % (getattr(getattr(input_def, "mapping"), "table"), statement_type),
            mapping=getattr(input_def, "mapping"),
            statement_type=statement_type,
            expected="%s_%s.sql" % (statement_type, getattr(input_def, "filename")),
        )
        for (input_def, statement_type) in itertools.product(
            mapping_definitions, statement_types
        )
    ]

    @parameterized.parameterized.expand(create_mapping_success_params)
    def test_create_mapping_success(self, name, mapping, statement_type, expected):
        log.debug("create CacheDictMapping")
        actual = CacheDictMapping(
            table=mapping.table, keys=mapping.keys, values=mapping.values
        )
        log.debug("created CacheDictMapping: %s", actual)

        expected_statement_path = self.res_dir + expected
        with open(expected_statement_path, "r") as expected_statement_file:
            expected_statement = expected_statement_file.read()
        actual_statement = getattr(actual, statement_type)()
        self.assertEqual(expected_statement, actual_statement)
