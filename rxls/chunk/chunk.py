from __future__ import annotations

__all__ = ["chunk"]

from typing import TYPE_CHECKING

import numpy as np
import pyarrow as pa

from ..constants import (
    REPR_PREPARED,
    REPR_RKNUMBER,
    REPR_SHARED,
    REPR_TEMPORAL,
    TYPE_NULL,
    TYPE_NUMERIC,
    TYPE_STRING,
)
from ..core import as_dataclass
from .arrow import take, unsafe_cast
from .casts import (
    _unsafe_array,
    _unsafe_arrow,
    _unsafe_data_to_array,
    _unsafe_data_to_arrow,
    _unsafe_strings_to_array,
    f8_is_i8,
    f8_to_ms_win,
    mf_strptime,
    rk_to_f8,
)

if TYPE_CHECKING:
    from typing import Sequence, TypeVar

    import numpy.typing as npt
    from typing_extensions import Self

    T = TypeVar("T")


EMPTY_CHUNK = 0

EMPTY_SHAREDS: pa.LargeStringArray = pa.nulls(0, pa.large_string())  # type: ignore

UTF8 = pa.large_string()
UI_4 = pa.uint32()
F__8 = pa.float64()
I__8 = pa.int64()
T_MS = pa.timestamp("ms")


def null_slice(x: int, index: slice) -> int:
    if not x:
        return 0

    start = index.start or 0
    if start < 0:
        start = x + start

    stop = index.stop or x
    if stop < 0:
        stop = x + stop

    step = index.step or 1

    if step < 0:
        stop, start = start, stop
        step = -step

    if stop <= start:
        return 0

    itvl = stop - start
    return itvl // step + bool(itvl % step)


@as_dataclass(mapping=True)
class chunk:  # noqa: D101, N801
    data: (
        pa.Array  # ? XL_UTF | XLSX/B SST
        | npt.NDArray[np.uint32]  # ? XLSB_RKNUMBER
        | npt.NDArray[np.int64]  # ? UNIX TIMESTAMP
        | npt.NDArray[np.float64]  # ? XLS[BX]_FLOAT64 | XLS[BX]_TEMPORAL | XLSB_BOOL
        | int  # ? NULL CHUNK (length of empty chunk)
    ) = EMPTY_CHUNK
    "Chunk data. An `int` when type of chunk is `NULL`"

    type: int = 0x00
    "Type of chunk"

    size: int = 0
    "Size of chunk"

    sref: pa.LargeStringArray = EMPTY_SHAREDS
    "Reference to existing LargeStringArray with shared strings"

    @staticmethod
    def nulls(length: int = 0) -> chunk:
        """Create null-chunk of given length"""
        return chunk(length, TYPE_NULL, length)

    @staticmethod
    def xlsx_numeric(
        data: Sequence[str],
        *,
        is_temporal: bool = False,
    ) -> chunk:
        """Try to create numpy.ndarray[np.float64] from given strings sequence (on error, fallback to just strings chunk)"""
        try:
            return chunk(
                _unsafe_strings_to_array(data, np.float64),  # type: ignore
                TYPE_NUMERIC
                | (is_temporal * REPR_TEMPORAL)
                | ((not is_temporal) * REPR_PREPARED),
                len(data),
            )
        except ValueError:
            return chunk.xlsx_strings(data)  # type: ignore

    @staticmethod
    def xlsx_strings(
        data: Sequence[str],
        *,
        is_shared: bool = False,
        shareds: pa.LargeStringArray | None = None,
    ) -> chunk:
        """Create strings-chunk (may be shared strings) from given strings sequence. When shared, `shareds` must reference valid strings arrow array."""
        data_len = len(data)
        if is_shared:
            if shareds is None:
                shareds = EMPTY_SHAREDS
            return chunk(
                _unsafe_arrow(
                    _unsafe_strings_to_array(data, np.uint32, data_len),
                    UI_4,
                ),
                TYPE_STRING | REPR_SHARED,
                data_len,
                shareds,
            )
        return chunk(pa.array(data, UTF8), TYPE_STRING | REPR_PREPARED, data_len)  # type: ignore

    @staticmethod
    def xlsb_strings(
        data: list[bytes],
        *,
        is_shared: bool = False,
        shareds: pa.LargeStringArray | None = None,
    ) -> chunk:
        """Create strings-chunk from given bytes sequence. When `is_shared`, bytes must be little-endian encoded int32 values and `shareds` must reference valid strings arrow array"""
        data_len = len(data)
        if is_shared:
            if shareds is None:
                shareds = EMPTY_SHAREDS
            return chunk(
                _unsafe_data_to_arrow(data, UI_4),
                TYPE_STRING | REPR_SHARED,
                data_len,
                shareds,
            )  # type: ignore
        return chunk(
            pa.array(data, UTF8),  # type: ignore
            TYPE_STRING | REPR_PREPARED,
            data_len,
        )

    @staticmethod
    def xlsb_numeric(
        data: list[bytes],
        *,
        is_boolean: bool = False,
        is_temporal: bool = False,
        is_rknumber: bool = False,
    ) -> chunk:
        r"""
        Create numeric/temporal chunk from given bytes sequence.
        When `is_boolean`, each value is single-byte (b'\x01' or b'\x00').
        When is_rknumber, then int32(little-endian), otherwise - float64(little-endian)
        """  # noqa: D205
        if is_rknumber:
            ret = _unsafe_data_to_array(data, np.uint32)
        elif is_boolean:
            ret = _unsafe_data_to_array(data, np.uint8).astype(np.float64)
        else:
            ret = _unsafe_data_to_array(data, np.float64)
        return chunk(
            ret,
            TYPE_NUMERIC
            | (is_temporal * REPR_TEMPORAL)
            | (is_rknumber * REPR_RKNUMBER)
            | ((not (is_temporal or is_rknumber)) * REPR_PREPARED),
            len(data),
        )  # type: ignore

    @property
    def is_empty(self) -> bool:
        """Chunk has length of zero."""
        return not self.data if not self.type else not len(self.data)  # type: ignore

    @property
    def is_null(self) -> bool:
        """Chunk has NULL-typed"""
        return not self.type

    @property
    def is_numeric(self) -> bool:
        """Chunk has numeric type"""
        return bool(self.type & TYPE_NUMERIC)

    @property
    def is_temporal(self) -> bool:
        """Chunk has numeric type and temporal represenation"""
        return bool((self.type & TYPE_NUMERIC) and (self.type & REPR_TEMPORAL))

    def prepare(self) -> Self:
        """Compile inner data to actual representation, and return in new chunk."""
        if not (self.type & REPR_PREPARED):
            if self.type & TYPE_STRING:
                self.data = take(self.sref, self.data)  # type: ignore
                self.type = TYPE_STRING | REPR_PREPARED
            else:
                if self.type & REPR_RKNUMBER:
                    self.data = rk_to_f8(self.data)  # type: ignore
                if self.type & REPR_TEMPORAL:
                    self.data = f8_to_ms_win(self.data)  # type: ignore
                self.type |= REPR_PREPARED
        return self

    def as_numeric(self) -> chunk:
        """Cast chunk to numeric"""
        if not self.type or self.type & TYPE_NUMERIC:
            return self
        return chunk(
            _unsafe_array(self.arrow(dest_dtype=F__8), np.float64),
            TYPE_NUMERIC | REPR_PREPARED,
        )

    def as_temporal(self, formats: tuple[str, ...] | None = None) -> chunk:
        """Cast chunk to temporal (`formats` argument used for strings inner data)"""
        if self.type & TYPE_NUMERIC:
            if self.type & REPR_TEMPORAL:
                return self
            if self.type & REPR_PREPARED:
                return chunk(self.data, TYPE_NUMERIC | REPR_TEMPORAL)
            return chunk(self.data, self.type | REPR_TEMPORAL)
        return chunk(
            mf_strptime(self.data, formats),  # type: ignore
            TYPE_NUMERIC | REPR_TEMPORAL | REPR_PREPARED,
        )  # type: ignore

    def take(self, index: int | pa.BooleanArray | slice) -> chunk:
        """
        Get sub-chunk from this chunk.

        - if `index` is `int`, returns chunk of zero or one element.
        - if `index` is `NDArray[uint32]`, return chunk by cell indices
        - if `index` is `slice`, return slice of chunk.

        Parameters
        ----------
        index : int | NDArray[np.uint32] | slice
            _description_

        Returns
        -------
        Self
            _description_

        """
        if self.type & TYPE_STRING:
            if isinstance(index, int):
                _data = self.data[index : index + 1]  # type: ignore
            elif isinstance(index, slice):
                _data = self.data[index]  # type: ignore
            else:
                # NOTE: index must be either slice or array of same length
                _data = self.data.filter(index)  # type: ignore
            return chunk(_data, self.type, len(_data), self.sref)
        if not self.type:
            if isinstance(index, int):
                _size = int(index < self.size)
            elif isinstance(index, slice):
                _size = null_slice(self.size, index)
            elif len(index) > self.size:
                raise ValueError(index)
            else:
                _size = index.true_count
            return chunk(_size, TYPE_NULL, _size)

        if isinstance(index, int):
            _data = self.data[index : index + 1]  # type: ignore
        elif isinstance(index, slice):
            _data = self.data[index]  # type: ignore
        else:
            _data = self.data[index.to_numpy(zero_copy_only=False)]  # type: ignore

        return chunk(_data, self.type, len(_data), self.sref)  # type: ignore

    def utf8(self) -> pa.LargeStringArray:
        """Create pyarrow.LargeStringArray from this chunk"""
        return unsafe_cast(self.arrow(), UTF8)  # type: ignore

    def arrow(
        self,
        *,
        dest_dtype: pa.DataType | None = None,
        strict: bool = True,
    ) -> pa.Array:
        """Converts internal data as is to pyarrow array"""
        if not self.type:
            return pa.nulls(self.data, dest_dtype)  # type: ignore

        data = self.data if self.type & REPR_PREPARED else self.prepare().data

        ret: pa.Array
        if self.type & TYPE_STRING:
            ret = data  # type: ignore
        elif self.type & REPR_TEMPORAL:
            ret = _unsafe_arrow(data, T_MS)  # type: ignore
        else:
            ret = _unsafe_arrow(data, F__8)  # type: ignore
        if dest_dtype is not None and dest_dtype.id != ret.type.id:  # type: ignore
            try:
                return unsafe_cast(ret, dest_dtype)
            except pa.ArrowInvalid:
                if strict:
                    raise
        return ret  # type: ignore

    @staticmethod
    def take_over(  # noqa: C901, PLR0912
        chunks: list[chunk],
        /,
        offset: int = 0,
        length: int = 0,
        index: pa.BooleanArray | None = None,
    ) -> list[chunk]:
        """
        Function, that filters chunk sequence, using given offset, length and index.

        Behaviour is:
            - When given `offset` > 0, chunks before offset will be skipped.
            - First chunk will be top-stripped by `offset` - `cumulative size of skipped chunks`
            - When given `length` > 0, result will have exact same length. Chunks after `length` will be skipped.
            - Last chunk will be bottom-stripped by `length` - `cumulative size of previous chunks`
            - If `length` is greater than cumulative size of chunks, null-chunk with required length will be added to the end of result.

        NOTE: Usually, `length` and `index` are not used at same time.

        Args:
        ----
            chunks (Sequence[chunk]): Sequence of chunks to take over
            offset (int, optional): Global offset of sequence. Defaults to 0.
            length (int, optional): Global length of sequence. Defaults to 0.
            index (pa.BooleanArray | None, optional): Filter chunks using bit mask. Defaults to None.

        Returns:
        -------
            Sequence[chunk]: Filtered sequence of chunks

        """
        result = []
        cum_len = 0

        if index is not None:
            length = index.true_count

            if not length:
                return result

            idx_off = 0

            for _chunk in chunks:
                if offset > 0:
                    if offset >= _chunk.size:
                        offset -= _chunk.size
                        continue
                    _chunk = _chunk.take(slice(offset, None))
                    offset = 0

                c_idx = index[idx_off : (idx_off := idx_off + _chunk.size)]

                if not c_idx.true_count:
                    continue

                _it = _chunk.take(slice(len(c_idx))).take(c_idx)

                if _it.size:
                    result.append(_it)
                    cum_len += _it.size

        else:
            for _chunk in chunks:
                if offset > 0:
                    if offset >= _chunk.size:
                        offset -= _chunk.size
                        continue
                    _chunk = _chunk.take(slice(offset, None))
                    offset = 0

                cum_len += _chunk.size

                if length and cum_len > length:
                    _chunk = _chunk.take(slice(_chunk.size + length - cum_len))

                if _chunk.size:
                    result.append(_chunk)

                if cum_len >= length:
                    break

        if length and cum_len < length:
            result.append(chunk.nulls(length - cum_len))

        return result

    @staticmethod
    def concatenate(  # noqa: D417, C901, PLR0912, PLR0915
        chunks: list[chunk],
        /,
        offset: int = 0,
        length: int = 0,
        index: pa.BooleanArray | None = None,
        *,
        conf_to_datetime: bool = True,
        conf_to_numerics: bool = True,
        datetime_formats: tuple[str, ...] | None = None,
        float_round_prec: int = 6,
    ) -> pa.Array:
        """
        Create pyarrow.Array from sequence of given chunks. Type of resulting array will be:

        - of chunks type (if all chunks has same type)
        - of timestamp['ms'] (if there is temporal and string chunks, `conf_to_datetime` set to True, and strings converted to timestamp successful)
        - of float64 (if there is string and numeric chunks, `conf_to_numeric` set to True, and strings converted to float64 successful)
        - of int64 (if resulting array is of type float64, and truncating decimals will not resulting in data loss)
        - of large_string (if some conversions failed)

        Arguments:
        ---------
            chunks {list[chunk]} -- Sequence of chunks

        Keyword Arguments:
        -----------------
            offset {int} -- Skip `offset` cells on top (default: {0})
            length {int} -- Take **exactly** `length` cells (append null-chunk to the end of sequence, if cell count less then `length`) (default: {0})
            index {pa.BooleanArray | None} -- Take **only** cells, that have corresponding bit set in `index` mask. Override `length` argument with `true_count` of `index` (default: {None})
            conf_to_datetime {bool} -- Try to convert float64 and strings to timestamps, when some chunks are temporal (default: {True})
            conf_to_numerics {bool} -- Try to convert strings to float64, when some chunks are float64 (also, this parameter enables two-step converting of strings to temporal: string -> float64 -> timestamp, when used along with `conf_to_datetime`) (default: {True})
            datetime_formats {tuple[str, ...] | None} -- Custom date/time formats (supports milliseconds and timezones only if Polars available). Ignored, when `conf_to_datetime` set to `False` (default: {None})
            float_round_prec {int} -- Precision of float64 values, which on rounding to it, give equivalent result as decimals truncating (default: {6})

        Returns:
        -------
            pa.Array -- pyarrow.Array

        """
        if offset or length or index is not None:
            chunks = chunk.take_over(chunks, offset, length, index)

        temp_idx = []
        nums_idx = []
        utf8_idx = []
        null_idx = set()

        for i, _chunk in enumerate(chunks):
            if not _chunk.type:
                null_idx.add(i)
            elif _chunk.type & TYPE_STRING:
                utf8_idx.append(i)
            elif _chunk.type & REPR_TEMPORAL:
                temp_idx.append(i)
            else:
                nums_idx.append(i)

        utf8_fallback = False

        if temp_idx:  # noqa: PLR1702
            if conf_to_datetime:
                if nums_idx:
                    for i in nums_idx:
                        chunks[i] = chunks[i].as_temporal()
                if utf8_idx:
                    for i in utf8_idx:
                        try:
                            chunks[i] = chunks[i].as_temporal(datetime_formats)
                        except pa.ArrowInvalid:  # noqa: PERF203
                            if conf_to_numerics:
                                try:
                                    chunks[i] = chunks[i].as_numeric().as_temporal()
                                except pa.ArrowInvalid:
                                    utf8_fallback = True
                            else:
                                utf8_fallback = True
            elif nums_idx or utf8_idx:
                utf8_fallback = True
        elif nums_idx and utf8_idx:
            if conf_to_numerics:
                for i in utf8_idx:
                    try:
                        chunks[i] = chunks[i].as_numeric()
                    except pa.ArrowInvalid:  # noqa: PERF203
                        utf8_fallback = True
                        break
            else:
                utf8_fallback = True

        if utf8_fallback:
            dtype = UTF8
        elif temp_idx:
            dtype = T_MS
        elif nums_idx:
            dtype = F__8

            is_integer = True

            # ? Checking integers:
            for i in range(len(chunks)):
                c = chunks[i]
                if not c.type:
                    continue

                if not (c.type & REPR_PREPARED):
                    chunks[i] = (c := c.prepare())

                if not bool(f8_is_i8(chunks[i].data, float_round_prec)):  # type: ignore
                    is_integer = False
                    break
            if is_integer:
                dtype = I__8
        else:
            dtype = UTF8

        return pa.chunked_array(
            [x.arrow(dest_dtype=dtype) for x in chunks],  # type: ignore
        ).combine_chunks()  # type: ignore
