import logging
from collections import namedtuple

import parameterized

from sqlitecaching.dict import CacheDictMapping, CacheDictMappingTuple
from sqlitecaching.test import CacheDictTestBase, TestLevel, test_level

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


@test_level(TestLevel.PRE_COMMIT)
class TestCacheDictMapping(CacheDictTestBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.res_dir += "mappings/"

    Def = namedtuple("Def", ["name", "input", "expected"])
    In = CacheDictMappingTuple
    Statements = namedtuple(
        "Statements",
        [
            "create_statement",
            "clear_statement",
            "delete_statement",
            "upsert_statement",
            "remove_statement",
            "length_statement",
            "keys_statement",
            "items_statement",
            "values_statement",
        ],
        defaults=([True for _ in range(0, 9)]),
    )

    create_mapping_success_params = [
        Def(
            name="aA__to__",
            input=In(table="aa", keys={"a": "A"}, values={},),
            expected=Statements(),
        ),
        Def(
            name="aA__to__bB",
            input=In(table="aa__bb", keys={"a": "A"}, values={"b": "B"},),
            expected=Statements(),
        ),
        Def(
            name="aA_bB__to__",
            input=In(table="aa_bb", keys={"a": "A", "b": "B"}, values={},),
            expected=Statements(),
        ),
        Def(
            name="aA_bB__to__",
            input=In(table="aA_bB", keys={"a": "a", "b": "b"}, values={},),
            expected=Statements(),
        ),
        Def(
            name="aA_bB__to__cC",
            input=In(table="aa_bb__cc", keys={"a": "a", "b": "b"}, values={"c": "C"}),
            expected=Statements(),
        ),
        Def(
            name="aA_bB__to__cC_dD",
            input=In(
                table="aa_bb__cc_dd",
                keys={"a": "a", "b": "b"},
                values={"c": "C", "d": "D"},
            ),
            expected=Statements(),
        ),
    ]

    @parameterized.parameterized.expand(create_mapping_success_params)
    def test_create_mapping_success(self, name, input, expected):
        log.debug("create CacheDictMapping")
        actual = CacheDictMapping(
            table=input.table, keys=input.keys, values=input.values
        )
        log.debug("created CacheDictMapping: %s", actual)

        for statement_type in expected._fields:
            with self.subTest(name=name, statement_type=statement_type):
                expected_statement = getattr(expected, statement_type)
                expected_statement_name = f"{statement_type}_{name}.sql"
                expected_statement_path = self.res_dir + expected_statement_name
                with open(expected_statement_path, "r") as expected_statement_file:
                    expected_statement = expected_statement_file.read()
                    actual_statement = getattr(actual, statement_type)()
                    self.assertEqual(expected_statement, actual_statement)
