import logging

from sqlitecaching.config import Config

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
log.addHandler(logging.NullHandler())

conf = Config(logger=log)
