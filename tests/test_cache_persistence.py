import logging

from tests import CacheDictTestBase, TestLevel, test_level

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())


class CacheDictPersistenceTestBase(CacheDictTestBase):
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

    def _test_retrieve_stored_value(self, *, cache_dict, inputs, expected_outputs):
        missing_value = object()

        log.debug("load inputs into cache_dist")
        for (input_key, input_value) in inputs:
            cache_dict[input_key] = input_value

        log.debug(
            "check all actual keys in cache_dist are expected and "
            "are associated with the value expected"
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

        log.debug("check all expected keys are in cache_dist")
        for (expected_key, expected_value) in expected_outputs.items():
            actual_value = cache_dict.get(expected_key, missing_value)
            self.assertNotEqual(
                actual_value,
                missing_value,
                f"expected key not found in actual_outputs: {expected_key}",
            )
            # Do not need to test value equality as that will have been done
            # when iterating across the actual values.

    def _test_retrieve_stored_values(self, *, dict_provider, config_provider=None):
        if not config_provider:
            config_provider = self.provide_test_config
        test_config = config_provider()
        mapping_config = test_config["mapping_config"]
        log.debug(
            "running %d inputs with mapping %s",
            len(test_config["inputs"]),
            mapping_config,
        )
        for test_input in test_config["inputs"]:
            cache_dict = dict_provider(mapping_config)
            with self.subTest(
                cache_dict=cache_dict,
                test_input=test_input,
                mapping_config=mapping_config,
            ):
                log.debug("running with input %s", test_input)
                self._test_retrieve_stored_value(
                    cache_dict=cache_dict,
                    inputs=test_input["inputs"],
                    expected_outputs=test_input["expected_outputs"],
                )


@test_level(TestLevel.PRE_COMMIT)
class TestMemoryCacheDictPersistence(CacheDictPersistenceTestBase):
    def test_memory_cache_persistence(self):
        def dict_provider(_):
            return dict()

        self._test_retrieve_stored_values(dict_provider=dict_provider)
