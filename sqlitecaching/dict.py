import sqlite3
from collections import UserDict


class CacheDict(UserDict):
    __please_do_not_construct_directly = object()

    def __init__(self, *, flag=None, placeholder=None):
        super().__init__()
        if flag != self.__please_do_not_construct_directly:
            raise Exception("e")
        self.placeholder = placeholder
        self.db = sqlite3.connect(":memory:")

    @classmethod
    def create_from_connection(cls):
        return CacheDict(flag=cls.__please_do_not_construct_directly,)
