from __future__ import annotations

__all__ = [
    "arrow_fn",
    "from_buffers",
    "indices_nonzero",
    "invert",
    "match",
    "or_",
    "strptime",
    "take",
    "unsafe_cast",
]

from typing import TYPE_CHECKING, Sequence

import pyarrow as pa
import pyarrow.compute as pc

if TYPE_CHECKING:
    from typing import Callable, TypeVar

    from typing_extensions import ParamSpec, TypeAlias

    P = ParamSpec("P")
    T = TypeVar("T")

    PaValue: TypeAlias = "pa.Array | pa.ChunkedArray | pa.Scalar"
    PaArray: TypeAlias = "pa.Array | pa.ChunkedArray"

    MonoArray = TypeVar("MonoArray", bound=pa.Array)


def arrow_fn(
    name: str,
    options: type[pc.FunctionOptions]
    | Callable[..., pc.FunctionOptions]
    | pc.FunctionOptions
    | None = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    _pc_fn_raw = pc.get_function(name)
    _pc_fn: Callable[..., T] = _pc_fn_raw.call  # type: ignore
    _arg_len = _pc_fn_raw.arity

    def wrapper(fn: Callable[P, T]) -> Callable[P, T]:
        nonlocal options

        if options:
            if callable(options):

                def py_fn(*args: P.args, **kwargs: P.kwargs) -> T:  # type: ignore
                    nonlocal _pc_fn, _arg_len, options
                    return _pc_fn(args[:_arg_len], options(*args[_arg_len:], **kwargs))  # type: ignore
            else:

                def py_fn(*args: P.args, **_: P.kwargs) -> T:
                    nonlocal _pc_fn, _arg_len, options
                    return _pc_fn(args[:_arg_len], options)  # type: ignore

        else:

            def py_fn(*args: P.args, **_: P.kwargs) -> T:
                nonlocal _pc_fn
                return _pc_fn(args)  # type: ignore

        py_fn.__annotations__ = fn.__annotations__
        py_fn.__doc__ = "### {}\n\n{}".format(*_pc_fn_raw._doc[:2])
        return py_fn

    return wrapper


@arrow_fn("cast", pc.CastOptions.unsafe)
def unsafe_cast(x: PaValue, target_type: pa.DataType) -> pa.Array:
    raise NotImplementedError


@arrow_fn("strptime", pc.StrptimeOptions)
def strptime(arr: PaArray, format: str, unit: str, errors_is_null: bool, /) -> pa.Array:  # noqa: FBT001
    raise NotImplementedError


@arrow_fn("indices_nonzero")
def indices_nonzero(arr: PaArray) -> pa.Array[int, pa.Scalar[int]]:
    raise NotImplementedError


@arrow_fn("match_substring_regex", pc.MatchSubstringOptions)
def match(
    arr: PaArray,
    pattern: str,
    /,
    *,
    ignore_case: bool = False,
) -> pa.BooleanArray:
    raise NotImplementedError


@arrow_fn("invert")
def invert(x: PaArray, /) -> pa.BooleanArray:
    raise NotImplementedError


@arrow_fn("or")
def or_(x: PaArray, y: PaValue, /) -> pa.BooleanArray:
    raise NotImplementedError


@arrow_fn("and")
def and_(x: PaArray, y: PaValue, /) -> pa.BooleanArray:
    raise NotImplementedError


@arrow_fn("array_take", pc.TakeOptions(boundscheck=False))
def take(
    arr: MonoArray,
    indices: PaArray | list[int],
    # *,
    # boundscheck: bool = True,
) -> MonoArray:
    raise NotImplementedError


def from_buffers(
    dtype: pa.DataType[T],
    length: int,
    buffers: Sequence[None | pa.Buffer],
    null_count: int = -1,
    offset: int = 0,
    children: list[pa.Array] | None = None,
) -> pa.Array[T, pa.Scalar[T]]:
    raise NotImplementedError


from_buffers = pa.Array.from_buffers  # type: ignore  # noqa:F811
