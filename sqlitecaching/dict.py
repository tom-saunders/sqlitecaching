import sqlite3
from collections import UserDict


class CacheDict(UserDict):
    def __init__(self, *, placeholder=None):
        super().__init__()
        self.placeholder = placeholder
        self.db = sqlite3.connect(":memory:")
