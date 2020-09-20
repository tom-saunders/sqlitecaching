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

T = typing.TypeVar("T", bound="SqliteCachingException")


class Cause(typing.NamedTuple):
    id: CauseID
    name: Name
    fmt: Format
    params: ParamSet


class Category(typing.NamedTuple):
    id: CategoryID
    name: Name
    causes: typing.Dict[CauseID, Cause]


try:
    _ = CATEGORY_REG
except NameError:
    CATEGORY_REG: typing.Dict[CategoryID, Category] = {}


class CauseProvider(typing.Generic[T]):
    subcls: typing.Type[T]

    category_id: CategoryID
    id: CauseID
    name: Name

    def __init__(
        self,
        *,
        except_cls: typing.Type[T],
        category_id: CategoryID,
        cause_id: CauseID,
        cause_name: Name,
    ):
        self.subcls = type(
            str(cause_name),
            (except_cls,),
            {},
        )

        self.category_id = category_id
        self.id = cause_id
        self.name = cause_name

    def __call__(
        self,
        params: ParamMap,
        /,
    ) -> T:
        return self.subcls(
            category_id=self.category_id,
            cause_id=self.id,
            params=params,
            stacklevel=2,
        )


class CategoryProvider(typing.Generic[T]):
    except_cls: typing.Type[T]

    id: CategoryID
    name: Name

    def __init__(
        self,
        *,
        except_cls: typing.Type[T],
        category_id: CategoryID,
        category_name: Name,
    ):
        self.except_cls = except_cls
        self.id = category_id
        self.name = category_name

    def register_cause(
        self,
        *,
        cause_name: NameStr,
        cause_id: CauseInt,
        fmt: FormatStr,
        params: ParamSet,
    ) -> typing.Callable[[ParamMap], T]:
        cause_id = CauseID(cause_id)
        cause_name = Name(cause_name)
        fmt = Format(fmt)
        log.info(
            "registering cause [%s] for category [%d (%s)] with id [%d]",
            cause_name,
            self.id,
            self.name,
            cause_id,
        )

        category = CATEGORY_REG.get(self.id, None)
        if not category:
            raise SqliteCachingException(
                category_id=CategoryID(0),
                cause_id=CauseID(4),
                params={
                    "category_id": self.id,
                    "cause_id": cause_id,
                    "cause_name": cause_name,
                },
                stacklevel=1,
            )
        causes = category.causes
        existing_cause = causes.get(cause_id, None)
        if existing_cause:
            raise SqliteCachingException(
                category_id=CategoryID(0),
                cause_id=CauseID(3),
                params={
                    "cause_id": cause_id,
                    "existing_cause_name": existing_cause.name,
                    "cause_name": cause_name,
                },
                stacklevel=1,
            )
        cause = Cause(
            id=cause_id,
            name=cause_name,
            fmt=fmt,
            params=params,
        )
        causes[cause_id] = cause

        return CauseProvider[T](
            except_cls=self.except_cls,
            category_id=self.id,
            cause_id=cause_id,
            cause_name=cause_name,
        )


class SqliteCachingException(Exception):
    _raise_on_additional_params: typing.ClassVar[bool] = False

    category: Category
    cause: Cause
    params: ParamMap
    msg: str

    def __init__(
        self,
        *,
        category_id: CategoryID,
        cause_id: CauseID,
        params: ParamMap,
        stacklevel: int,
    ):
        self.params = params

        try:
            self.category = CATEGORY_REG[category_id]
        except KeyError:
            raise SqliteCachingException(
                category_id=CategoryID(0),
                cause_id=CauseID(0),
                params={"category_id": category_id},
                stacklevel=1,
            )
        try:
            self.cause = self.category.causes[cause_id]
        except KeyError:
            raise SqliteCachingException(
                category_id=CategoryID(0),
                cause_id=CauseID(2),
                params={
                    "cause_id": cause_id,
                    "category_id": category_id,
                    "category_name": self.category.name,
                },
                stacklevel=1,
            )
        fmt = self.cause.fmt
        expected_params = self.cause.params
        provided_params = frozenset(self.params.keys())

        missing_params = expected_params - provided_params
        if missing_params:
            log.error("expected parameters not provided: [%s]", missing_params)
            raise SqliteCachingException(
                category_id=CategoryID(0),
                cause_id=CauseID(5),
                params={
                    "category_id": category_id,
                    "category_name": self.category.name,
                    "cause_id": cause_id,
                    "cause_name": self.cause.name,
                    "missing_params": missing_params,
                },
                stacklevel=1,
            )

        additional_params = {
            k: self.params[k] for k in (provided_params - expected_params)
        }
        if additional_params:
            log.warning(
                "unexpected additional parameters provided: [%s]",
                additional_params,
            )
            if self._raise_on_additional_params:
                raise SqliteCachingException(
                    category_id=CategoryID(0),
                    cause_id=CauseID(6),
                    params={
                        "category_id": category_id,
                        "category_name": self.category.name,
                        "cause_id": cause_id,
                        "cause_name": self.cause.name,
                        "additional_params": additional_params,
                    },
                    stacklevel=1,
                )

        self.msg = fmt.format(**self.params)

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
        cls: typing.Type[T],
        *,
        category_name: NameStr,
        category_id: CategoryInt,
    ) -> CategoryProvider[T]:
        category_name = Name(category_name)
        category_id = CategoryID(category_id)

        log.info("registering category [%s] with id [%d]", category_name, category_id)

        existing_category = CATEGORY_REG.get(category_id, None)
        if existing_category:
            raise SqliteCachingException(
                category_id=CategoryID(0),
                cause_id=CauseID(1),
                params={
                    "category_id": category_id,
                    "existing_category_name": existing_category.name,
                    "category_name": category_name,
                },
                stacklevel=1,
            )

        category = Category(id=category_id, name=category_name, causes={})
        CATEGORY_REG[category_id] = category

        return CategoryProvider[T](
            except_cls=cls,
            category_id=category_id,
            category_name=category_name,
        )


SqliteCachingMetaCategory = SqliteCachingException.register_category(
    category_name=f"{__name__}.SqliteCachingMetaCategory",
    category_id=0,
)
SqliteCachingMissingCategoryException = SqliteCachingMetaCategory.register_cause(
    cause_name=f"{__name__}.SqliteCachingMissingCategoryException",
    cause_id=0,
    fmt="No category matching {category_id} was found",
    params=frozenset(["category_id"]),
)
SqliteCachingDuplicateCategoryException = SqliteCachingMetaCategory.register_cause(
    cause_name=f"{__name__}.SqliteCachingDuplicateCategoryException",
    cause_id=1,
    fmt=(
        "previously registered category with id [{category_id} "
        "({existing_category_name})], cannot overwrite with [{category_name}]"
    ),
    params=frozenset(["category_id", "existing_category_name", "category_name"]),
)
SqliteCachingMissingCauseException = SqliteCachingMetaCategory.register_cause(
    cause_name=f"{__name__}.SqliteCachingMissingCauseException",
    cause_id=2,
    fmt=(
        "No cause matching {cause_id} was found for category: [{category_id} "
        "({category_name})]"
    ),
    params=frozenset(["cause_id", "category_id", "category_name"]),
)
SqliteCachingDuplicateCauseException = SqliteCachingMetaCategory.register_cause(
    cause_name=f"{__name__}.SqliteCachingMissingCauseException",
    cause_id=3,
    fmt=(
        "previously registered cause with id [{cause_id} ({existing_cause_name})], "
        "cannot overwrite with [{cause_name}]"
    ),
    params=frozenset(["cause_id", "existing_cause_name", "cause_name"]),
)
SqliteCachingNoCategoryForCauseException = SqliteCachingMetaCategory.register_cause(
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
SqliteCachingMissingParamsException = SqliteCachingMetaCategory.register_cause(
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
SqliteCachingAdditionalParamsException = SqliteCachingMetaCategory.register_cause(
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
