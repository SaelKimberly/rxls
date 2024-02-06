from __future__ import annotations

__all__ = [
    "NUMBA_AVAILABLE",
    "NUMBA_TBB_AVAILABLE",
    "POLARS_AVAILABLE",
    "PaArray",
    "PaValue",
    "Un",
    "as_dataclass",
    "cached",
    "end_decl",
    "struct_p",
    "struct_u",
    "u1_p",
    "u2_p",
    "u4_p",
    "u4_u",
]

import os
import warnings
from struct import Struct
from typing import TYPE_CHECKING

from recordclass import as_dataclass as _as_dataclass
from typing_extensions import (
    Unpack as Un,
)
from typing_extensions import dataclass_transform

if TYPE_CHECKING:
    from typing import Callable, NoReturn, TypeVar

    import pyarrow as pa
    from typing_extensions import (
        ParamSpec,
        TypeAlias,
        TypeVarTuple,
    )

    try:
        from functools import cache  # type: ignore
    except ImportError:
        from functools import lru_cache as cache

    PaArray: TypeAlias = "pa.Array | pa.ChunkedArray"
    PaValue: TypeAlias = "pa.Array | pa.ChunkedArray | pa.Scalar"

    PV = TypeVar("PV", pa.Scalar, pa.Array)

    T = TypeVar("T")
    P = ParamSpec("P")
    Ts = TypeVarTuple("Ts")

    def cached(f: Callable[P, T]) -> Callable[P, T]:
        raise NotImplementedError

    cached = cache  # type: ignore # noqa: F811
else:
    try:
        from functools import cache as cached
    except ImportError:
        from functools import lru_cache as cached

PERFORMANCE_WARNINGS: bool = os.environ.get("RXLS_PERFORMANCE_WARNINGS", "0") == "1"

try:
    import numba

    NUMBA_AVAILABLE = True

    try:
        from numba.np.ufunc import parallel

        warnings.filterwarnings("ignore", ".*TBB.*", numba.errors.NumbaWarning)

        parallel._check_tbb_version_compatible()

        NUMBA_TBB_AVAILABLE = True

    except ImportError:
        NUMBA_TBB_AVAILABLE = False

        if PERFORMANCE_WARNINGS:
            warnings.warn(
                "Numba GPU acceleration of RXLS is unavailable (slowdown x4)",
                UserWarning,
                stacklevel=2,
            )


except ImportError:
    NUMBA_AVAILABLE = False
    NUMBA_TBB_AVAILABLE = False

    if PERFORMANCE_WARNINGS:
        warnings.warn(
            "Numba CPU acceleration of RXLS is unavailable (slowdown x4)",
            UserWarning,
            stacklevel=2,
        )


try:
    import polars as pl  # noqa: F401

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False


@dataclass_transform()
def as_dataclass(
    cls: type[T] | None = None,
    *,
    use_dict: bool = False,
    use_weakref: bool = False,
    hashable: bool = False,
    sequence: bool = False,
    mapping: bool = False,
    iterable: bool = False,
    readonly: bool = False,
    fast_new: bool = True,
    rename: bool = False,
    gc: bool = False,
) -> Callable[[type[T]], type[T]]:
    if cls is not None:
        return _as_dataclass(
            use_dict=use_dict,
            use_weakref=use_weakref,
            hashable=hashable,
            sequence=sequence,
            mapping=mapping,
            iterable=iterable,
            readonly=readonly,
            fast_new=fast_new,
            rename=rename,
            gc=gc,
        )(cls)  # type: ignore

    def wrapper(cls: type[T]) -> type[T]:
        return _as_dataclass(
            use_dict=use_dict,
            use_weakref=use_weakref,
            hashable=hashable,
            sequence=sequence,
            mapping=mapping,
            iterable=iterable,
            readonly=readonly,
            fast_new=fast_new,
            rename=rename,
            gc=gc,
        )(cls)  # type: ignore

    return wrapper


def struct_u(
    s: str | Struct,
) -> Callable[[Callable[[bytes], tuple[Un[Ts]]]], Callable[[bytes], tuple[Un[Ts]]]]:
    def wrapper(
        f: Callable[[bytes], tuple[Un[Ts]]],
    ) -> Callable[[bytes], tuple[Un[Ts]]]:
        return Struct(s).unpack if isinstance(s, str) else s.unpack  # type: ignore

    return wrapper


def struct_p(
    s: str | Struct,
) -> Callable[[Callable[[Un[Ts]], bytes]], Callable[[Un[Ts]], bytes]]:
    def wrapper(f: Callable[[Un[Ts]], bytes]) -> Callable[[Un[Ts]], bytes]:
        return Struct(s).pack if isinstance(s, str) else s.pack  # type: ignore

    return wrapper


def end_decl() -> NoReturn:
    raise NotImplementedError


@struct_u("<I")
def u4_u(b: bytes) -> tuple[int]:
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
