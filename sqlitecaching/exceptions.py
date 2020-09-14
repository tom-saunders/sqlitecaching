import logging
import typing

log = logging.getLogger(__name__)

Name = typing.NewType("Name", str)
Format = typing.NewType("Format", str)
CategoryID = typing.NewType("CategoryID", int)
CauseID = typing.NewType("CauseID", int)

ParamSet = typing.FrozenSet[str]
ParamMap = typing.Mapping[str, typing.Any]

NameStr = typing.Union[Name, str]
FormatStr = typing.Union[Format, str]
CategoryInt = typing.Union[CategoryID, int]
CauseInt = typing.Union[CauseID, int]


class Cause(typing.NamedTuple):
    name: Name
    fmt: Format
    params: ParamSet
    exception: typing.Type["SqliteCachingException"]


class Category(typing.NamedTuple):
    name: Name
    exception: typing.Type["SqliteCachingException"]
    causes: typing.Dict[CauseID, Cause]


class SqliteCachingException(Exception):
    _categories: typing.ClassVar[typing.Dict[CategoryID, Category]] = {}
    _raise_on_additional_params: typing.ClassVar[bool] = False
    _register_category: typing.ClassVar[
        typing.Callable[
            [
                typing.Any,
                Name,
                CategoryID,
            ],
            typing.Type["SqliteCachingException"],
        ]
    ]
    _register_cause: typing.ClassVar[
        typing.Callable[
            [
                typing.Any,
                Name,
                CauseID,
                Format,
                ParamSet,
            ],
            typing.Type["SqliteCachingException"],
        ]
    ]

    category_id: CategoryID
    _category: Category

    cause_id: CauseID
    params: ParamMap
    msg: str

    _expected_params: ParamSet
    _cause: Cause
    _fmt: Format

    def __init__(
        self,
        *,
        category_id: CategoryID,
        cause_id: CauseID,
        params: ParamMap,
        stacklevel: int,
    ):
        self.category_id = category_id
        self.cause_id = cause_id
        self.params = params

        try:
            self._category = self._categories[category_id]
        except KeyError:
            raise SqliteCachingException(
                category_id=CategoryID(0),
                cause_id=CauseID(0),
                params={"category_id": category_id},
                stacklevel=1,
            )
        try:
            self._cause = self._category.causes[cause_id]
        except KeyError:
            raise SqliteCachingException(
                category_id=CategoryID(0),
                cause_id=CauseID(2),
                params={
                    "cause_id": cause_id,
                    "category_id": category_id,
                    "category_name": self._category.name,
                },
                stacklevel=1,
            )
        self._fmt = self._cause.fmt
        self._expected_params = self._cause.params
        provided_params = frozenset(self.params.keys())

        missing_params = self._expected_params - provided_params
        if missing_params:
            log.error("expected parameters not provided: [%s]", missing_params)
            raise SqliteCachingException(
                category_id=CategoryID(0),
                cause_id=CauseID(5),
                params={
                    "category_id": category_id,
                    "category_name": self._category.name,
                    "cause_id": cause_id,
                    "cause_name": self._cause.name,
                    "missing_params": missing_params,
                },
                stacklevel=1,
            )

        self.additional_params = {
            k: self.params[k] for k in (provided_params - self._expected_params)
        }
        if self.additional_params:
            log.warning(
                "unexpected additional parameters provided: [%s]",
                self.additional_params,
            )
            if self._raise_on_additional_params:
                raise SqliteCachingException(
                    category_id=CategoryID(0),
                    cause_id=CauseID(6),
                    params={
                        "category_id": category_id,
                        "category_name": self._category.name,
                        "cause_id": cause_id,
                        "cause_name": self._cause.name,
                        "additional_params": self.additional_params,
                    },
                    stacklevel=1,
                )

        self.msg = self._fmt.format(**self.params)

        log.error("Exception: [%s]", self.msg)
        log.debug(
            "raising [%s] with msg [%s]",
            type(self).__name__,
            self.msg,
            stack_info=True,
            stacklevel=4,
        )

        super().__init__(self.msg)

    @classmethod
    def raise_on_additional_params(
        cls,
        should_raise: typing.Optional[bool],
        /,
    ) -> bool:
        if should_raise is not None:
            log.warning(
                "setting [%s]._raise_on_additional_params to [%s]",
                cls.__name__,
                should_raise,
            )
            cls._raise_on_additional_params = should_raise
        return cls._raise_on_additional_params

    @classmethod
    def register_category(
        cls,
        *,
        category_name: NameStr,
        category_id: CategoryInt,
    ):
        return cls._register_category(
            cls,
            Name(category_name),
            CategoryID(category_id),
        )


def __register_cause(
    cls,
    cause_name: Name,
    cause_id_: CauseID,
    fmt: Format,
    params: ParamSet,
    /,
):
    log.info(
        "registering cause [%s] for category [%d (%s)] with id [%d]",
        cause_name,
        cls._category_id,
        cls._category_name,
        cause_id_,
    )
    cause_name = Name(cause_name)
    fmt = Format(fmt)

    category = cls._categories.get(cls._category_id, None)
    if not category:
        raise SqliteCachingException(
            category_id=CategoryID(0),
            cause_id=CauseID(4),
            params={
                "category_id": cls._category_id,
                "cause_id": cause_id_,
                "cause_name": cause_name,
            },
            stacklevel=1,
        )
    causes = category.causes
    existing_cause = causes.get(cause_id_, None)
    if existing_cause:
        raise SqliteCachingException(
            category_id=CategoryID(0),
            cause_id=CauseID(3),
            params={
                "cause_id": cause_id_,
                "existing_cause_name": existing_cause.name,
                "cause_name": cause_name,
            },
            stacklevel=1,
        )
    category_exception: typing.Any = category.exception

    class CauseException(category_exception):
        _cause_id: typing.ClassVar[int] = cause_id_
        _cause_name: typing.ClassVar[str] = cause_name

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
        params=params,
        exception=CauseException,
    )
    causes[cause_id_] = cause

    CauseException.__name__ = cause_name
    CauseException.__qualname__ = cause_name
    return CauseException


def __register_category(
    cls,
    category_name: Name,
    category_id_: CategoryID,
    /,
):
    log.info("registering category [%s] with id [%d]", category_name, category_id_)

    existing_category = cls._categories.get(category_id_, None)
    if existing_category:
        log.error(
            (
                "previously registered category with id [%d (%s)], cannot "
                "overwrite with [%s]"
            ),
            category_id_,
            existing_category.name,
            category_name,
        )
        raise SqliteCachingException(
            category_id=CategoryID(0),
            cause_id=CauseID(1),
            params={
                "category_id": category_id_,
                "existing_category_name": existing_category.name,
                "category_name": category_name,
            },
            stacklevel=1,
        )

    class CategoryException(SqliteCachingException):
        _category_id: typing.ClassVar[CategoryID] = category_id_
        _category_name: typing.ClassVar[str] = category_name
        _register_cause: typing.ClassVar[
            typing.Callable[
                [
                    typing.Any,
                    Name,
                    CauseID,
                    Format,
                    ParamSet,
                ],
                typing.Type[SqliteCachingException],
            ]
        ]

        def __init__(
            self,
            *,
            cause_id: CauseID,
            params: ParamMap,
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
            cause_name: NameStr,
            cause_id: CauseInt,
            fmt: FormatStr,
            params: ParamSet,
        ):
            return cls._register_cause(
                cls,
                Name(cause_name),
                CauseID(cause_id),
                Format(fmt),
                params,
            )

    CategoryException._register_cause = __register_cause

    category = Category(name=category_name, exception=CategoryException, causes={})
    cls._categories[category_id_] = category

    CategoryException.__name__ = category_name
    CategoryException.__qualname__ = category_name
    return CategoryException


SqliteCachingException._register_category = __register_category


SqliteCachingMetaException = SqliteCachingException.register_category(
    category_name=f"{__name__}.SqliteCachingMetaException",
    category_id=0,
)
SqliteCachingMissingCategoryException = SqliteCachingMetaException.register_cause(
    cause_name=f"{__name__}.SqliteCachingMissingCategoryException",
    cause_id=0,
    fmt="No category matching {category_id} was found",
    params=frozenset(["category_id"]),
)
SqliteCachingDuplicateCategoryException = SqliteCachingMetaException.register_cause(
    cause_name=f"{__name__}.SqliteCachingDuplicateCategoryException",
    cause_id=1,
    fmt=(
        "previously registered category with id [{category_id} "
        "({existing_category_name})], cannot overwrite with [{category_name}]"
    ),
    params=frozenset(["category_id", "existing_category_name", "category_name"]),
)
SqliteCachingMissingCauseException = SqliteCachingMetaException.register_cause(
    cause_name=f"{__name__}.SqliteCachingMissingCauseException",
    cause_id=2,
    fmt=(
        "No cause matching {cause_id} was found for category: [{category_id} "
        "({category_name})]"
    ),
    params=frozenset(["cause_id", "category_id", "category_name"]),
)
SqliteCachingDuplicateCauseException = SqliteCachingMetaException.register_cause(
    cause_name=f"{__name__}.SqliteCachingMissingCauseException",
    cause_id=3,
    fmt=(
        "previously registered cause with id [{cause_id} ({existing_cause_name})], "
        "cannot overwrite with [{cause_name}]"
    ),
    params=frozenset(["cause_id", "existing_cause_name", "cause_name"]),
)
SqliteCachingNoCategoryForCauseException = SqliteCachingMetaException.register_cause(
    cause_name=f"{__name__}.SqliteCachingNoCategoryForCauseException",
    cause_id=4,
    fmt=(
        "No matching category was found with category_id [{category_id}] when "
        "registering exception with cause_id [{cause_id} ({cause_name})]"
    ),
    params=frozenset(
        [
            "category_id",
            "cause_id",
            "cause_name",
        ],
    ),
)
SqliteCachingMissingParamsException = SqliteCachingMetaException.register_cause(
    cause_name=f"{__name__}.SqliteCachingMissingParamsException",
    cause_id=5,
    fmt=(
        "not all specified parameters were not provided when raising exception "
        " with category [{category_id} ({category_name})], cause [{cause_id} "
        "({cause_name})]: [{missing_params}]"
    ),
    params=frozenset(
        [
            "category_id",
            "category_name",
            "cause_id",
            "cause_name",
            "missing_params",
        ],
    ),
)
SqliteCachingAdditionalParamsException = SqliteCachingMetaException.register_cause(
    cause_name=f"{__name__}.SqliteCachingAdditionalParamsException",
    cause_id=6,
    fmt=(
        "Unexpected additional parameters were provided when raising exception "
        " with category [{category_id} ({category_name})], cause [{cause_id} "
        "({cause_name})]: [{additional_params}]"
    ),
    params=frozenset(
        [
            "category_id",
            "category_name",
            "cause_id",
            "cause_name",
            "additional_params",
        ],
    ),
)
