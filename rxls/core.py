# flake8: noqa
from struct import Struct
from typing import Any, Iterable, TypeVar, Callable, NoReturn, Type
from typing_extensions import ParamSpec, TypeVarTuple, Unpack as Un
from functools import cache
from collections import deque
from recordclass import as_dataclass as _as_dataclass
from typing_extensions import dataclass_transform

__all__ = [
    "cached",
    "as_dataclass",
    "exhaust",
    "u8_u",
    "u8_p",
    "u4_u",
    "u4_p",
    "u2_u",
    "u2_p",
    "u1_u",
    "u1_p",
    "f8_u",
    "f8_p",
    "f4_u",
    "f4_p",
    "struct_u",
    "struct_p",
    "end_decl",
    "Un",
]

P = ParamSpec("P")
T = TypeVar("T")
Ts = TypeVarTuple("Ts")


def cached(f: Callable[P, T]) -> Callable[P, T]:
    raise NotImplementedError()


def exhaust(it: Iterable[T], f: "Callable[[T], Any] | None" = None) -> None:
    deque(it if f is None else map(f, it), 0)


cached = cache  # type: ignore

_T = TypeVar("_T")


@dataclass_transform()
def as_dataclass(
    cls: "Type[_T] | None" = None,
    *,
    use_dict: bool = False,
    use_weakref: bool = False,
    hashable: bool = False,
    sequence: bool = False,
    mapping: bool = False,
    iterable: bool = False,
    readonly: bool = False,
    module: "str | None" = None,
    fast_new: bool = True,
    rename: bool = False,
    gc: bool = False
) -> Callable[[Type[_T]], Type[_T]]:
    if cls is not None:
        return _as_dataclass(
            use_dict=use_dict,
            use_weakref=use_weakref,
            hashable=hashable,
            sequence=sequence,
            mapping=mapping,
            iterable=iterable,
            readonly=readonly,
            module=module,
            fast_new=fast_new,
            rename=rename,
            gc=gc,
        )(cls)

    def wrapper(cls: Type[_T]) -> Type[_T]:
        return _as_dataclass(
            use_dict=use_dict,
            use_weakref=use_weakref,
            hashable=hashable,
            sequence=sequence,
            mapping=mapping,
            iterable=iterable,
            readonly=readonly,
            module=module,
            fast_new=fast_new,
            rename=rename,
            gc=gc,
        )(
            cls
        )  # type: ignore

    return wrapper


def struct_u(s: "str | Struct") -> Callable[[Callable[[bytes], tuple[Un[Ts]]]], Callable[[bytes], tuple[Un[Ts]]]]:
    def wrapper(f: Callable[[bytes], tuple[Un[Ts]]]) -> Callable[[bytes], tuple[Un[Ts]]]:
        return Struct(s).unpack if isinstance(s, str) else s.unpack  # type: ignore

    return wrapper


def struct_p(s: "str | Struct") -> Callable[[Callable[[Un[Ts]], bytes]], Callable[[Un[Ts]], bytes]]:
    def wrapper(f: Callable[[Un[Ts]], bytes]) -> Callable[[Un[Ts]], bytes]:
        return Struct(s).pack if isinstance(s, str) else s.pack  # type: ignore

    return wrapper


def end_decl() -> NoReturn:
    raise NotImplementedError()


@struct_u("<Q")
def u8_u(b: bytes) -> tuple[int]:
    end_decl()


@struct_u("<I")
def u4_u(b: bytes) -> tuple[int]:
    end_decl()


@struct_u("<H")
def u2_u(b: bytes) -> tuple[int]:
    end_decl()


@struct_u("<B")
def u1_u(b: bytes) -> tuple[int]:
    end_decl()


@struct_u("<f")
def f4_u(b: bytes) -> tuple[float]:
    end_decl()


@struct_u("<d")
def f8_u(b: bytes) -> tuple[float]:
    end_decl()


@struct_p("<Q")
def u8_p(i: int) -> bytes:
    end_decl()


@struct_p("<I")
def u4_p(i: int) -> bytes:
    end_decl()


@struct_p("<H")
def u2_p(i: int) -> bytes:
    end_decl()


@struct_p("<B")
def u1_p(i: int) -> bytes:
    end_decl()


@struct_p("<f")
def f4_p(i: float) -> bytes:
    end_decl()


@struct_p("<d")
def f8_p(i: float) -> bytes:
    end_decl()
