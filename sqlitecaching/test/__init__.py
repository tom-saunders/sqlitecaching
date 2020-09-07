import logging
import unittest

from sqlitecaching.config import Config as BaseConfig
from sqlitecaching.test.enums import TestLevel

log = logging.getLogger(__name__)


class Config(BaseConfig):
    def __init__(
        self,
        *args,
        test_level=TestLevel.PRE_COMMIT,
        output_dir=None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._test_level = test_level
        self._output_dir = output_dir
        self._resource_dir = "./sqlitecaching/test/resources/"

    def set_test_level(self, level):
        self._test_level = TestLevel.convert(level)

    def get_test_level(self):
        return self._test_level

    def set_output_dir(self, output_dir):
        self._output_dir = output_dir

    def get_resource_dir(self):
        return self._resource_dir

    def set_resource_dir(self, path):
        self._resource_dir = path


config = Config(logger=logging.getLogger(".".join(__name__.split(".")[:-1])))


def test_level(level):
    log.debug("config: %s level: %s", config.get_test_level(), level)
    return unittest.skipIf(
        config.get_test_level() < level,
        (
            f"Skipping test configured at level {level} as configured level is "
            f"{config.get_test_level()}"
        ),
    )


class CacheDictTestBase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.res_dir = config.get_resource_dir()
