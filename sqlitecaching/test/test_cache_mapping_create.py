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
            "create",
            "clear",
            "delete",
            "upsert",
            "remove",
            "length",
            "keys",
            "items",
            "values",
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
