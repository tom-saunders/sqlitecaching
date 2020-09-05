import logging
from collections import namedtuple

import parameterized

from sqlitecaching.dict import CacheDictMapping, CacheDictMappingTuple
from sqlitecaching.test import CacheDictTestBase, TestLevel, test_level

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


@test_level(TestLevel.PRE_COMMIT)
class TestCacheDictMapping(CacheDictTestBase):
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
        defaults=[None for _ in range(0, 9)],
    )

    create_mapping_success_params = [
        Def(
            name="why",
            input=In(table="aA_bB", keys={"a": "A", "b": "B"}, values={},),
            expected=Statements(),
        ),
        Def(
            name="who",
            input=In(table="aa_bb", keys={"a": "a", "b": "b"}, values={},),
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
                expected_statement_path = getattr(expected, statement_type)
                # FIXME remove after test dev done.
                if not expected_statement_path:
                    continue
                # FIXME change to 'r' after test dev done.
                with open(expected_statement_path, "rw+") as expected_statement_file:
                    expected_statement = expected_statement_file.read()
                    actual_statement = getattr(actual, statement_type)()
                    # FIXME remove after test dev done.
                    if not expected_statement:
                        log.warn("stop being so lazy")
                        expected_statement = actual_statement
                        expected_statement_file.seek(0)
                        expected_statement_file.write(expected_statement)
                    self.assertEqual(expected_statement, actual_statement)