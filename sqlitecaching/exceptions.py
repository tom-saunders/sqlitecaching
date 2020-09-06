import logging
import typing
from collections import namedtuple

log = logging.getLogger(__name__)


class Cause(typing.NamedTuple):
    name: str
    fmt: str
    params: typing.Any
    exception: Exception


class Type(typing.NamedTuple):
    name: str
    exception: Exception


class SqliteCachingException(Exception):

    _types = {}

    def __init__(
        self, *, type_id: int, cause_id: int, params: typing.Dict, stacklevel: int
    ):
        self.type_id = type_id
        self.cause_id = cause_id
        if not params:
            params = {}
        self.params = params

        self._type = self._types.get(type_id, None)
        if not self._type:
            raise SqliteCachingException(
                type_id=0, cause_id=0, params={"type_id": type_id}, stacklevel=1
            )
        self._cause = self._type.exception._causes.get(cause_id, None)
        if not self._cause:
            raise SqliteCachingException(
                type_id=0,
                cause_id=2,
                params={
                    "cause_id": cause_id,
                    "type_id": type_id,
                    "type_name": self._type.name,
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
    def register_type(cls, *, type_name: str, type_id: int):
        log.info("registering type [%s] with id [%d]", type_name, type_id)
        existing_type = cls._types.get(type_id, None)
        if existing_type:
            log.error(
                (
                    "previously registered type with id [%d (%s)], cannot "
                    "overwrite with [%s]"
                ),
                type_id,
                existing_type.name,
                type_name,
            )
            raise SqliteCachingException(
                type_id=0,
                cause_id=1,
                params={
                    "type_id": type_id,
                    "existing_type_name": existing_type.name,
                    "type_name": type_name,
                },
                stacklevel=1,
            )

        class TypeException(SqliteCachingException):

            _type_id = type_id
            _type_name = type_name
            _causes = {}

            def __init__(self, *, cause_id: int, params: typing.Dict, stacklevel: int):
                super().__init__(
                    type_id=self._type_id,
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
                req_params: typing.List,
                opt_params: typing.Dict = None,
            ):
                log.info(
                    "registering cause [%s] for type [%d (%s)] with id [%d]",
                    cause_name,
                    cls._type_id,
                    cls._type_name,
                    cause_id,
                )
                if not req_params:
                    req_params = []
                if not opt_params:
                    opt_keys = []
                    opt_values = []
                else:
                    (opt_keys, opt_values) = zip(*opt_params.items())

                existing_cause = cls._causes.get(cause_id, None)
                if existing_cause:
                    raise SqliteCachingException(
                        type_id=0,
                        cause_id=3,
                        params={
                            "cause_id": cause_id,
                            "existing_cause_name": existing_cause.name,
                            "cause_name": cause_name,
                        },
                        stacklevel=1,
                    )
                param_name = f"{cause_name.replace('.', '_')}__params"
                params = req_params + opt_keys
                Params = namedtuple(param_name, params, defaults=opt_values)

                class CauseException(TypeException):
                    _cause_id = cause_id

                    def __init__(
                        self, *, params: typing.Dict = None, stacklevel: int = 2
                    ):
                        super().__init__(
                            cause_id=self._cause_id,
                            params=params,
                            stacklevel=(stacklevel + 1),
                        )

                cause = Cause(
                    name=cause_name, fmt=fmt, params=Params, exception=CauseException
                )
                cls._causes[cause_id] = cause

                CauseException.__name__ = cause_name
                CauseException.__qualname__ = cause_name
                return CauseException

        typ = Type(name=type_name, exception=TypeException)
        cls._types[type_id] = typ

        TypeException.__name__ = type_name
        TypeException.__qualname__ = type_name
        return TypeException


SqliteCachingMetaException = SqliteCachingException.register_type(
    type_name=f"{__name__}.SqliteCachingMetaException", type_id=0
)
SqliteCachingMissingTypeException = SqliteCachingMetaException.register_cause(
    cause_name=f"{__name__}.SqliteCachingMissingTypeException",
    cause_id=0,
    fmt="No type matching {type_id} was found",
    req_params=["type_id"],
)
SqliteCachingDuplicateTypeException = SqliteCachingMetaException.register_cause(
    cause_name=f"{__name__}.SqliteCachingDuplicateTypeException",
    cause_id=1,
    fmt=(
        "previously registered type with id [{type_id} ({existing_type_name})], "
        "cannot overwrite with [{type_name}]"
    ),
    req_params=["type_id", "existing_type_name", "type_name"],
)
SqliteCachingMissingCauseException = SqliteCachingMetaException.register_cause(
    cause_name=f"{__name__}.SqliteCachingMissingCauseException",
    cause_id=2,
    fmt="No cause matching {cause_id} was found for type: [{type_id} ({type_name})]",
    req_params=["cause_id", "type_id", "type_name"],
)
SqliteCachingDuplicateCauseException = SqliteCachingMetaException.register_cause(
    cause_name=f"{__name__}.SqliteCachingMissingCauseException",
    cause_id=3,
    fmt=(
        "previously registered cause with id [{cause_id} ({existing_cause_name})], "
        "cannot overwrite with [{cause_name}]"
    ),
    req_params=["cause_id", "existing_cause_name", "cause_name"],
)
