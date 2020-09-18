import logging
import typing

import parameterized

from sqlitecaching.dict import CacheDict
from sqlitecaching.test import SqliteCachingTestBase, TestLevel, test_level

log = logging.getLogger(__name__)


class Mapping(typing.NamedTuple):
    table: str
    keys: typing.Mapping[str, typing.Optional[str]]
    values: typing.Optional[typing.Mapping[str, typing.Optional[str]]] = None


class TestDef(typing.NamedTuple):
    name: str
    provider: typing.Callable[..., CacheDict]
    provider_params: typing.Mapping[str, typing.Any]
    mapping: Mapping
    inputs: typing.Sequence[typing.Tuple[typing.Any, typing.Any]]
    outputs: typing.Mapping[typing.Any, typing.Any]


@test_level(TestLevel.PRE_COMMIT)
class TestCacheDictPersistence(SqliteCachingTestBase):
    def provide_test_config(self):
        log.debug("using base persistence configuration")

        test_config = {
            "mapping_config": {},
            "inputs": [
                {
                    "inputs": [("a", "a"), ("b", "b"), ("a", "b")],
                    "expected_outputs": {"b": "b", "a": "b"},
                },
                {"inputs": [("a", "a")], "expected_outputs": {"a": "a"}},
                {"inputs": [("a", "a"), ("a", "b")], "expected_outputs": {"a": "b"}},
            ],
        }
        return test_config

    @parameterized.parameterized.expand(
        [
            TestDef(
                name="why",
                provider=CacheDict.open_anon_memory,
                provider_params={},
                mapping=Mapping(table="tabl", keys={"z": None}),
                inputs=[("a", "a")],
                outputs={"a": "a"},
            ),
            TestDef(
                name="who",
                provider=CacheDict.open_anon_memory,
                provider_params={},
                mapping=Mapping(table="tabl", keys={"z": None}),
                inputs=[("a", "a"), ("a", "b")],
                outputs={"a": "b"},
            ),
            TestDef(
                name="where",
                provider=CacheDict.open_anon_memory,
                provider_params={},
                mapping=Mapping(table="tabl", keys={"z": None}),
                inputs=[("a", "a"), ("b", "b")],
                outputs={"a": "a", "b": "b"},
            ),
        ],
    )
    def test_retrieve_stored_value(
        self,
        name,
        provider,
        provider_params,
        mapping,
        inputs,
        expected_outputs,
    ):
        missing_value = object()
        cache_dict = provider(mapping=mapping, **provider_params)

        log.debug("load inputs into cache_dist")
        for (input_key, input_value) in inputs:
            cache_dict[input_key] = input_value

        log.debug(
            "check all actual keys in cache_dist are expected and "
            "are associated with the value expected",
        )
        for (actual_key, actual_value) in cache_dict.items():
            expected_value = expected_outputs.get(actual_key, missing_value)
            self.assertNotEqual(
                expected_value,
                missing_value,
                f"key not found in expected_outputs: {actual_key}",
            )
            self.assertEqual(
                actual_value,
                expected_value,
                f"retrieved value does not match expected for key: {actual_key}",
            )

        # FIXME why
        log.debug("check all expected keys are in cache_dist")
        for expected_key in expected_outputs:
            # TODO ???
            actual_value = cache_dict.get(expected_key, missing_value)
            self.assertNotEqual(
                actual_value,
                missing_value,
                f"expected key not found in actual_outputs: {expected_key}",
            )
            # Do not need to test value equality as that will have been done
            # when iterating across the actual values.
