from __future__ import annotations

__all__ = [
    "_strptime",
    "_unsafe_array",
    "_unsafe_arrow",
    "_unsafe_bool_arrow_to_numpy",
    "_unsafe_data_to_array",
    "_unsafe_data_to_arrow",
    "_unsafe_strings_to_array",
    "dt_has_d",
    "dt_has_t",
    "f8_is_i8",
    "f8_to_ms_win",
    "f8_to_rk",
    "i8_to_rk",
    "mf_strptime",
    "ms_to_f8_win",
    "rk_to_f8",
]

from typing import TYPE_CHECKING

import numpy as np
import pyarrow as pa

from ..core import NUMBA_AVAILABLE, NUMBA_TBB_AVAILABLE, POLARS_AVAILABLE
from .arrow import from_buffers, strptime

if TYPE_CHECKING:
    from typing import Any, Iterable, Sequence, TypeVar

    from numpy.typing import NDArray

    NT = TypeVar("NT", bound=np.generic)
    T = TypeVar("T")
    C = TypeVar("C")


def rk_to_f8(arr: NDArray[np.uint32]) -> NDArray[np.float64]:
    """
    Convert XLSB RkNumber format to Float64

    Parameters
    ----------
    arr : NDArray[np.uint32]
        Unchanged array of RkNumbers (4-bytes)

    Returns
    -------
    NDArray[np.float64]
        Float64 array

    """
    i_flag = arr & np.uint32(0b10)
    c_flag = ((arr & np.uint32(0b01)) * np.uint32(99) + np.uint32(1)).astype(np.float64)

    arr = arr & np.uint32(0xFFFFFFFC)

    return (
        np.where(
            i_flag,
            (arr.astype(np.int32) >> np.int32(2)).astype(np.float64),
            ((arr.astype(np.uint64) << np.uint64(32)).view(np.float64)),
        )
        / c_flag
    )


def f8_to_ms_win(arr: NDArray[np.float64]) -> NDArray[np.int64]:
    """
    Represents input Float64 array as "`Milliseconds since 1970-01-01`" from "`Days with fractions since 1900-01-01`".

    NOTE: In Windows MS Excel standard, for backward compatibility, 1900 is a leap year (+1 day in February 1900).

    If some floats are between `[0, 1)`, they will be represented as "`Time since 1970-01-01`" from "`Fraction of day`"

    Parameters
    ----------
    arr : NDArray[np.float64]
        Input Float64 array (days from 1900, or day fractions)

    Returns
    -------
    NDArray[np.int64]
        Output Int64 array (milliseconds from 1970, UNIX timestamp)

    """
    # NOTE 1: 25_568 - actual days from 1900-01-01 to 1970-01-01
    # NOTE 2: 25_569 - actual days for backward compatibility reason (for dates greater or equal to 1900-03-01)
    # NOTE 3: 86_400_000 - milliseconds per day

    return (
        (
            arr
            - (
                (arr >= np.float64(1.0))
                * (np.float64(25_569.0) - (arr < np.float64(60.0)))
            )
        )
        * np.float64(86_400_000.0)
    ).astype(np.int64)


def ms_to_f8_win(arr: NDArray[np.int64]) -> NDArray[np.float64]:
    """
    Represents input Int64 array as "`Days with fractions since 1900-01-01`" from "`Milliseconds since 1970-01-01`"

    NOTE: In Windows MS Excel standard, for backward compatibility, 1900 is a leap year (+1 day in February 1900).

    If some timestamps are less than milliseconds per day, they will be represented as "`Fraction of day`"

    Parameters
    ----------
    arr : NDArray[np.int64]
        Input Int64 array (milliseconds from 1970, UNIX timestamp)

    Returns
    -------
    NDArray[np.float64]
        Output Float64 array (Days with fractions since 1900-01-01)

    """
    # NOTE: -2_203_891_200_000 == 1900-03-01 (milliseconds from 1970-01-01)
    # NOTE: 2_209_161_600_000 == 25569 days (milliseconds between 1900-01-01 and 1970-01-01 with fake 1900 leap year)
    # NOTE: 2_209_075_200_000 == 25568 days (milliseconds between 1900-01-01 and 1970-01-01 without fake 1900 leap year)
    # NOTE: 86_400_000 == milliseconds per day

    ms_per_dy = np.int64(86_400_000)

    return (
        arr
        + (
            (arr >= ms_per_dy)
            * (2_209_075_200_000 + (arr >= np.int64(-2_203_891_200_000)) * ms_per_dy)
        )
    ) / ms_per_dy


def f8_to_rk(arr: NDArray[np.float64]) -> NDArray[np.uint32]:
    return np.where(
        arr == np.round(arr, 2),
        np.where(
            np.trunc(arr) == np.round(arr, 2),
            ((arr + np.float64(0.001)).astype(np.int32) << np.int32(2)) | np.int32(2),
            (
                (arr * np.float64(100.0) + np.float64(0.001)).astype(np.int32)
                << np.int32(2)
            )
            | np.int32(3),
        ).view(np.uint32),
        (arr.view(np.uint64) >> np.uint64(32)).astype(np.uint32)
        & np.uint32(0xFFFFFFFC),
    )


def i8_to_rk(
    arr: NDArray[np.int64 | np.int32 | np.uint64 | np.uint32],
) -> NDArray[np.uint32]:
    return (arr.astype(np.int32) << np.int32(2) | np.int32(2)).view(np.uint32)


def f8_is_i8(arr: NDArray[np.float64], int_threshold: int) -> np.bool_:
    """
    Check, that if we round input floating array to `int_threshold`, it will be equals to this array but with truncated fractions.

    Parameters
    ----------
    arr : NDArray[np.float64]
        Input Float64 numpy array
    int_threshold : int
        Count of fraction digits, for which input will be rounded.

    Returns
    -------
    np.bool_
        Is rounded input potentially integer array, or not

    """
    return ~np.any(np.trunc(arr) != np.round(arr, int_threshold))


def dt_has_t(arr: NDArray[np.int64]) -> np.bool_:
    """
    Checks, that unix timestamp has time part

    Parameters
    ----------
    arr : NDArray[np.uint64]
        Input numpy array (unix timestamps, ms from 1970-01-01)

    Returns
    -------
    np.bool_
        Have any of this timestamps time part or not

    """
    return np.any(arr % np.uint64(86_400_000))


def dt_has_d(arr: NDArray[np.int64]) -> np.bool_:
    """
    Checks, that unix timestamp has date part

    Parameters
    ----------
    arr : NDArray[np.uint64]
        Input numpy array (unix timestamps, ms from 1970-01-01)

    Returns
    -------
    np.bool_
        Have any of this timestamps date part or not

    """
    return np.any(arr > np.uint64(86_400_000))


if NUMBA_AVAILABLE:
    import numba as nb

    f8_is_i8 = nb.njit(
        nb.bool_(nb.types.Array(nb.f8, 1, "C", True), nb.u4),
        fastmath={"fast"},
        cache=True,
    )(f8_is_i8)  # type: ignore

    dt_has_d = nb.njit(
        nb.bool_(nb.types.Array(nb.int64, 1, "C", True)),
        fastmath={"fast"},
        cache=True,
    )(dt_has_d)  # type: ignore
    dt_has_t = nb.njit(
        nb.bool_(nb.types.Array(nb.int64, 1, "C", True)),
        fastmath={"fast"},
        cache=True,
    )(dt_has_t)  # type: ignore

    if NUMBA_TBB_AVAILABLE:

        def __gu_rk_to_f8(arr: NDArray[np.uint32], out: NDArray[np.float64]) -> None:
            i_flag = arr & np.uint32(0b10)
            c_flag = ((arr & np.uint32(0b01)) * np.uint32(99) + np.uint32(1)).astype(
                np.float64,
            )

            arr = arr & np.uint32(0xFFFFFFFC)

            out[:] = (
                np.where(
                    i_flag,
                    (arr.astype(np.int32) >> np.int32(2)).astype(np.float64),
                    ((arr.astype(np.uint64) << np.uint64(32)).view(np.float64)),
                )
                / c_flag
            )

        def __gu_f8_to_rk(arr: NDArray[np.float64], out: NDArray[np.uint32]) -> None:
            out[:] = np.where(
                arr == np.round(arr, 2),
                np.where(
                    np.trunc(arr) == np.round(arr, 2),
                    (
                        (arr + np.float64(0.001)).astype(np.int64).astype(np.int32)
                        << np.int32(2)
                    )
                    | np.int32(2),
                    (
                        (arr * np.float64(100.0) + np.float64(0.001)).astype(np.int32)
                        << np.int32(2)
                    )
                    | np.int32(3),
                ).view(np.uint32),
                (arr.view(np.uint64) >> np.uint64(32)).astype(np.uint32)
                & np.uint32(0xFFFFFFFC),
            )

        def __gu_i8_to_rk(
            arr: NDArray[np.int64 | np.int32 | np.uint64 | np.uint32],
            out: NDArray[np.uint32],
        ) -> None:
            out[:] = (arr.astype(np.int32) << np.int32(2) | np.int32(2)).view(np.uint32)

        def __gu_f8_to_ms_win(arr: NDArray[np.float64], out: NDArray[np.int64]) -> None:
            out[:] = (
                (
                    arr
                    - (
                        (arr >= np.float64(1.0))
                        * (np.float64(25_569.0) - (arr < np.float64(60.0)))
                    )
                )
                * np.float64(86_400_000.0)
            ).astype(np.int64)

        def __gu_ms_to_f8_win(arr: NDArray[np.int64], out: NDArray[np.float64]) -> None:
            ms_per_dy = np.int64(86_400_000)

            out[:] = (
                arr
                + (
                    (arr >= ms_per_dy)
                    * (
                        2_209_075_200_000
                        + (arr >= np.int64(-2_203_891_200_000)) * ms_per_dy
                    )
                )
            ) / ms_per_dy

        rk_to_f8 = nb.guvectorize(  # type: ignore
            [
                nb.void(
                    nb.types.Array(nb.u4, 1, "C", True),
                    nb.types.Array(nb.f8, 1, "C", False),
                ),
            ],
            "(n) -> (n)",
            target="cpu",
            nopython=True,
            cache=True,
        )(__gu_rk_to_f8)

        f8_to_rk = nb.guvectorize(  # type: ignore
            [
                nb.void(
                    nb.types.Array(nb.f8, 1, "C", True),
                    nb.types.Array(nb.u4, 1, "C", False),
                ),
            ],
            "(n) -> (n)",
            target="cpu",
            nopython=True,
            cache=True,
        )(__gu_f8_to_rk)

        i8_to_rk = nb.guvectorize(  # type: ignore
            [
                nb.void(
                    nb.types.Array(nb.i8, 1, "C", True),
                    nb.types.Array(nb.u4, 1, "C", False),
                ),
                nb.void(
                    nb.types.Array(nb.i4, 1, "C", True),
                    nb.types.Array(nb.u4, 1, "C", False),
                ),
                nb.void(
                    nb.types.Array(nb.u8, 1, "C", True),
                    nb.types.Array(nb.u4, 1, "C", False),
                ),
                nb.void(
                    nb.types.Array(nb.u4, 1, "C", True),
                    nb.types.Array(nb.u4, 1, "C", False),
                ),
            ],
            "(n) -> (n)",
            target="cpu",
            nopython=True,
            cache=True,
        )(__gu_i8_to_rk)

        f8_to_ms_win = nb.guvectorize(  # type: ignore
            [
                nb.void(
                    nb.types.Array(nb.f8, 1, "C", True),
                    nb.types.Array(nb.i8, 1, "C", False),
                ),
            ],
            "(n) -> (n)",
            target="cpu",
            nopython=True,
            cache=True,
        )(__gu_f8_to_ms_win)

        ms_to_f8_win = nb.guvectorize(  # type: ignore
            [
                nb.void(
                    nb.types.Array(nb.i8, 1, "C", True),
                    nb.types.Array(nb.f8, 1, "C", False),
                ),
            ],
            "(n) -> (n)",
            target="cpu",
            nopython=True,
            cache=True,
        )(__gu_ms_to_f8_win)

    else:
        rk_to_f8 = nb.njit(  # type: ignore
            nb.types.Array(nb.f8, 1, "C", True)(nb.types.Array(nb.u4, 1, "C", True)),
            fastmath={"fast"},
            cache=True,
        )(rk_to_f8)
        f8_to_ms_win = nb.njit(  # type: ignore
            nb.types.Array(nb.i8, 1, "C", True)(nb.types.Array(nb.f8, 1, "C", True)),
            cache=True,
        )(f8_to_ms_win)
        ms_to_f8_win = nb.njit(  # type: ignore
            nb.types.Array(nb.f8, 1, "C", True)(nb.types.Array(nb.i8, 1, "C", True)),
            cache=True,
        )(ms_to_f8_win)
        f8_to_rk = nb.njit(  # type: ignore
            nb.types.Array(nb.u4, 1, "C", True)(nb.types.Array(nb.f8, 1, "C", True)),
            cache=True,
            fastmath={"fast"},
        )(f8_to_rk)
        i8_to_rk = nb.njit(  # type: ignore
            [
                nb.types.Array(nb.u4, 1, "C", True)(
                    nb.types.Array(nb.i8, 1, "C", True),
                ),
                nb.types.Array(nb.u4, 1, "C", True)(
                    nb.types.Array(nb.i4, 1, "C", True),
                ),
                nb.types.Array(nb.u4, 1, "C", True)(
                    nb.types.Array(nb.u8, 1, "C", True),
                ),
                nb.types.Array(nb.u4, 1, "C", True)(
                    nb.types.Array(nb.u4, 1, "C", True),
                ),
            ],
            fastmath={"fast"},
            cache=True,
        )(i8_to_rk)


def _unsafe_array(arr: pa.Array, dtype: type[NT]) -> NDArray[NT]:
    """
    Fast and unsafe constructs numpy array from last buffer of arrow array.

    NOTE: Unsafe means:

    - There is no type checking. If size of arrow dtype and output numpy dtype are different, behavior is undefined.
    - If input arrow array is null array with null dtype, there will be error, because last buffer of this arrow array is `None`
    - If input arrow array is of non-fixed-size dtype (utf8, binary), behavior is undefined.

    NOTE: Recommend to use this function only with fixed-size-dtype arrow arrays with no nulls in them. Also, input and output dtype sizes must be equals.


    Parameters
    ----------
    arr : pa.Array
        Input arrow array of fixed size
    dtype : np.dtype[T]
        Type of output numpy array

    Returns
    -------
    NDArray[T]
        Numpy array of expected dtype

    """
    return np.frombuffer(arr.buffers()[-1], dtype)  # type: ignore


def _unsafe_arrow(
    arr: NDArray[Any],
    dtype: pa.DataType[T],
) -> pa.Array[T, pa.Scalar[T]]:
    """
    Fast and unsafe constructs pyarrow array from numpy array.

    NOTE: Unsafe means:

    - There is no type checking. If size of numpy dtype and output arrow dtype are different, behavior is undefined.
    - There will be error, if output arrow is of non-fixed-size dtype.
    - Output arrow cannot contains nulls

    NOTE: Recommend to use this function only with fixed-size-dtype numpy arrays. Also, input and output dtype sizes must be equals.

    Parameters
    ----------
    arr : NDArray[Any]
        Input numpy array
    dtype : pa.DataType[T]
        Type of output arrow array

    Returns
    -------
    pa.Array[T, pa.Scalar[T]]
        Arrow array of expected dtype

    """
    return from_buffers(dtype, len(arr), (None, pa.py_buffer(arr.data)), 0)


def _unsafe_data_to_arrow(
    data: Iterable[bytes | bytearray | memoryview],
    dtype: pa.DataType[T],
) -> pa.Array[T, pa.Scalar[T]]:
    """
    Fast and unsafe constructs pyarrow array from iterable of bytes.

    NOTE: Unsafe means:

    - There is no checking for size of elements of input iterable.
    - There will be error, if output arrow is of non-fixed-size dtype.
    - Output arrow cannot contains nulls

    NOTE: Recommend to use this function only with input iterable of elements of known fixed size.

    Parameters
    ----------
    data : Iterable[bytes]
        Input iterable of bytes
    dtype : pa.DataType[T]
        Type of output arrow array

    Returns
    -------
    pa.Array[T, pa.Scalar[T]]
        Arrow array of expected dtype

    """
    return from_buffers(
        dtype,
        len(data),  # type: ignore
        (None, pa.py_buffer(b"".join(data))),  # type: ignore
        0,
    )


def _unsafe_data_to_array(
    data: Iterable[bytes | bytearray | memoryview],
    dtype: type[NT],
) -> NDArray[NT]:
    """
    Fast and unsafe constructs numpy array from iterable of bytes.

    NOTE: Unsafe means:

    - There is no checking for size of elements of input iterable.
    - If output array is of non-fixed-size dtype, behavior is undefined.

    NOTE: Recommend to use this function only with input iterable of elements of known fixed size.

    Parameters
    ----------
    data : Iterable[bytes]
        Input iterable of bytes
    dtype : np.dtype[NT]
        Type of output numpy array

    Returns
    -------
    NDArray[NT]
        Numpy array of expected dtype

    """
    return np.frombuffer(b"".join(data), dtype)


def _unsafe_bool_arrow_to_numpy(arr: pa.BooleanArray) -> NDArray[np.bool_]:
    """
    Special function for fast and unsafe converting arrow boolean array (bitmask) to numpy boolean array.

    NOTE: Unsafe means:

    - There is no type checking

    Parameters
    ----------
    arr : pa.BooleanArray
        Input arrow boolean array

    Returns
    -------
    NDArray[np.bool_]
        Output numpy boolean array

    """
    return np.unpackbits(np.frombuffer(arr.buffers()[-1], np.uint8))[-len(arr) :][  # type: ignore
        ::-1
    ].view(np.bool_)


def _unsafe_strings_to_array(
    data: Iterable[str] | Iterable[int],
    dtype: type[NT],
    length: int = -1,
) -> NDArray[NT]:
    """
    Special function for fast and unsafe converting of input iterable of strings to numpy array of expected dtype

    NOTE: Actually, this is just the same function as numpy.fromiter

    Parameters
    ----------
    data : Iterable[str]
        Iterable of strings to convert to array
    dtype : np.dtype[NT]
        Type of output numpy array
    length : int
        Length of input iterable

    Returns
    -------
    NDArray[NT]
        Numpy array of expected dtype

    """
    raise NotImplementedError


_unsafe_strings_to_array = np.fromiter  # type: ignore

if POLARS_AVAILABLE:
    import polars as pl

    DEFAULT_TIME_FORMATS: tuple[str, ...] = ("%T", "%R", "%r", "%I:%M %p", "%T%.f")

    def _strptime(arr: pa.Array, format: str, /) -> NDArray[np.int64]:
        """
        Use of Polars strptime functionality (see chrono crate for advanced info).

        Faster and more flexible string to datetime parser.

        Arguments:
        ---------
            arr: pa.Array
                Strings Arrow array
            format: str
                Single possible format of timestamp

        Returns:
        -------
            NDArray[np.int64] -- UNIX Timestamp (milliseconds since 1970-01-01)

        """
        ret = _unsafe_array(
            pl.from_arrow(arr)
            .str.strptime(pl.Datetime("ms"), format, strict=False)  # type: ignore
            .to_arrow(),
            np.int64,
        )
        return np.where(ret < 0, ret % np.int64(86_400_000), ret)

else:
    DEFAULT_TIME_FORMATS: tuple[str, ...] = (
        "%T",
        "%R",
    )

    def _strptime(arr: pa.Array, format: str, /) -> NDArray[np.int64]:
        """
        Default pyarrow strptime (cannot parse milliseconds and AM/PM)

        Arguments:
        ---------
            arr: pa.Array
                Strings Arrow array
            format: str
                Single possible format of timestamp

        Returns:
        -------
            NDArray[np.int64] -- UNIX Timestamp (milliseconds since 1970-01-01)

        """
        ret = _unsafe_array(strptime(arr, format, "ms", True), np.int64)
        return np.where(ret < 0, ret % np.int64(86_400_000), ret)


DEFAULT_DATE_FORMATS: tuple[str, ...] = (
    # * ISO-8601
    "%Y-%m-%d",
    # * Some European formats
    "%d/%m/%Y",
    "%d/%m/%y",
    "%d.%m.%Y",
    "%d.%m.%y",
    # * Additional formats
    "%d-%m-%Y",
    "%d-%m-%y",
)

DEFAULT_DATETIME_FORMATS = tuple(
    dtf
    for df in DEFAULT_DATE_FORMATS
    for dtf in [
        *[
            x
            for y in [(f"{df} {tf}", f"{df}T{tf}") for tf in DEFAULT_TIME_FORMATS]
            for x in y
        ],
        df,
    ]
)


def mf_strptime(
    arr: pa.Array,
    formats: Sequence[str] | None = None,
    *,
    strict: bool = True,
) -> NDArray[np.int64]:
    """
    Try to recognize temporal data in the input arrow array.

    Output format is `Milliseconds since 1970-01-01` (unix timestamp)

    By default, formats are ISO: date and time, date and time.

    Milliseconds can be parsed only if Polars is available. PyArrow cannot parse them.

    Parameters
    ----------
    arr : pa.Array
        Input arrow array (LargeStringArray or StringArray supported)
    formats : tuple[str, ...] | None, optional
        Format strings for temporal data in the arrow array.
    strict : bool, optional
        Raise an error, when some scalars failed conversion

    Returns
    -------
    NDArray[np.int64]
        Output numpy Int64 array (`Milliseconds since 1970-01-01`)

    Raises
    ------
    pa.ArrowInvalid
        If one or more values cannot be recognized as temporal, using given formats.

    """
    result = np.repeat(np.int64(0), len(arr))

    if not formats:
        formats = (
            *DEFAULT_DATETIME_FORMATS,
            *DEFAULT_DATE_FORMATS,
            *DEFAULT_TIME_FORMATS,
        )

    for format in formats:
        result = np.where(result == 0, _strptime(arr, format), result)

        if np.all(result > np.int64(0)):
            return result

    if strict and np.any(result == 0):
        raise pa.ArrowInvalid
    return result
