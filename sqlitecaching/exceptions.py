import logging
import typing
from collections import namedtuple

log = logging.getLogger(__name__)


class Cause(typing.NamedTuple):
    name: str
    fmt: str
    params: typing.Type
    exception: typing.Type["SqliteCachingException"]


class Category(typing.NamedTuple):
    name: str
    exception: typing.Type["SqliteCachingException"]
    causes: typing.Dict[int, Cause]


class SqliteCachingException(Exception):
    _categories: typing.ClassVar[typing.Dict[int, Category]] = {}

    def __init__(
        self,
        *,
        category_id: int,
        cause_id: int,
        params: typing.Mapping[str, typing.Any],
        stacklevel: int,
    ):
        self.category_id = category_id
        self.cause_id = cause_id
        if not params:
            params = {}
        self.params = params

        self._category = self._categories.get(category_id, None)
        if not self._category:
            raise SqliteCachingException(
                category_id=0,
                cause_id=0,
                params={"category_id": category_id},
                stacklevel=1,
            )
        self._cause = self._category.causes.get(cause_id, None)
        if not self._cause:
            raise SqliteCachingException(
                category_id=0,
                cause_id=2,
                params={
                    "cause_id": cause_id,
                    "category_id": category_id,
                    "category_name": self._category.name,
                },
                stacklevel=1,
            )
        self._fmt = self._cause.fmt
        self._param_type = self._cause.params
        self._params = self._param_type(**self.params)

        self._msg = self._fmt.format(**self.params)

        log.error("Exception: [%s]", self._msg)
        log.debug(
            "raising [%s] with msg [%s]",
            type(self).__name__,
            self._msg,
            stack_info=True,
            stacklevel=4,
        )

        super().__init__(self._msg)

    @classmethod
    def register_category(cls, *, category_name: str, category_id: int):
        log.info("registering category [%s] with id [%d]", category_name, category_id)
        existing_category = cls._categories.get(category_id, None)
        if existing_category:
            log.error(
                (
                    "previously registered category with id [%d (%s)], cannot "
                    "overwrite with [%s]"
                ),
                category_id,
                existing_category.name,
                category_name,
            )
            raise SqliteCachingException(
                category_id=0,
                cause_id=1,
                params={
                    "category_id": category_id,
                    "existing_category_name": existing_category.name,
                    "category_name": category_name,
                },
                stacklevel=1,
            )

        class CategoryException(SqliteCachingException):

            _category_id: typing.ClassVar[int] = category_id
            _category_name: typing.ClassVar[str] = category_name

            def __init__(
                self,
                *,
                cause_id: int,
                params: typing.Mapping[str, typing.Any],
                stacklevel: int,
            ):
                super().__init__(
                    category_id=self._category_id,
                    cause_id=cause_id,
                    params=params,
                    stacklevel=(stacklevel + 1),
                )

            @classmethod
            def register_cause(
                cls,
                *,
                cause_name: str,
                cause_id: int,
                fmt: str,
                params: typing.AbstractSet[str],
            ):
                log.info(
                    "registering cause [%s] for category [%d (%s)] with id [%d]",
                    cause_name,
                    cls._category_id,
                    cls._category_name,
                    cause_id,
                )

                category = cls._categories.get(cls._category_id, None)
                if not category:
                    raise Exception("e")
                causes = category.causes
                existing_cause = causes.get(cause_id, None)
                if existing_cause:
                    raise SqliteCachingException(
                        category_id=0,
                        cause_id=3,
                        params={
                            "cause_id": cause_id,
                            "existing_cause_name": existing_cause.name,
                            "cause_name": cause_name,
                        },
                        stacklevel=1,
                    )
                Params = namedtuple("Params", params)  # type: ignore

                class CauseException(CategoryException):
                    _cause_id = cause_id

                    def __init__(
                        self,
                        *,
                        params: typing.Mapping[str, typing.Any],
                        stacklevel: int = 2,
                    ):
                        super().__init__(
                            cause_id=self._cause_id,
                            params=params,
                            stacklevel=(stacklevel + 1),
                        )

                cause = Cause(
                    name=cause_name,
                    fmt=fmt,
                    params=Params,
                    exception=CauseException,
                )
                causes[cause_id] = cause

                CauseException.__name__ = cause_name
                CauseException.__qualname__ = cause_name
                return CauseException

        category = Category(name=category_name, exception=CategoryException, causes={})
        cls._categories[category_id] = category

        CategoryException.__name__ = category_name
        CategoryException.__qualname__ = category_name
        return CategoryException


SqliteCachingMetaException = SqliteCachingException.register_category(
    category_name=f"{__name__}.SqliteCachingMetaException",
    category_id=0,
)
SqliteCachingMissingCategoryException = SqliteCachingMetaException.register_cause(
    cause_name=f"{__name__}.SqliteCachingMissingCategoryException",
    cause_id=0,
    fmt="No category matching {category_id} was found",
    params=["category_id"],
)
SqliteCachingDuplicateCategoryException = SqliteCachingMetaException.register_cause(
    cause_name=f"{__name__}.SqliteCachingDuplicateCategoryException",
    cause_id=1,
    fmt=(
        "previously registered category with id [{category_id} "
        "({existing_category_name})], cannot overwrite with [{category_name}]"
    ),
    params=["category_id", "existing_category_name", "category_name"],
)
SqliteCachingMissingCauseException = SqliteCachingMetaException.register_cause(
    cause_name=f"{__name__}.SqliteCachingMissingCauseException",
    cause_id=2,
    fmt=(
        "No cause matching {cause_id} was found for category: [{category_id} "
        "({category_name})]"
    ),
    params=["cause_id", "category_id", "category_name"],
)
SqliteCachingDuplicateCauseException = SqliteCachingMetaException.register_cause(
    cause_name=f"{__name__}.SqliteCachingMissingCauseException",
    cause_id=3,
    fmt=(
        "previously registered cause with id [{cause_id} ({existing_cause_name})], "
        "cannot overwrite with [{cause_name}]"
    ),
    params=["cause_id", "existing_cause_name", "cause_name"],
)
