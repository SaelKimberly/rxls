# ruff: noqa:D101, D102, D105, D107

from __future__ import annotations

__all__ = ["xl_series"]

from typing import TYPE_CHECKING

import pyarrow as pa

from .chunk import chunk
from .constants import (
    REPR_BOOLEAN,
    REPR_RKNUMBER,
    REPR_SHARED,
    REPR_TEMPORAL,
    TYPE_STRING,
)

if TYPE_CHECKING:
    from typing_extensions import Literal

    from .cell import cell

ONE = pa.scalar(1, pa.int8())

T_STR = pa.large_string()
T_U4 = pa.uint32()
T_F8 = pa.float64()


class xl_series:  # noqa: N801
    __slots__ = (
        "__binary",
        "__ch_off",
        "__chunk",
        "__chunks",
        "__e_row",
        "__s_row",
        "__shared",
        "__xdtr",
        "_conf_to_datetime",
        "_conf_to_numerics",
        "_datetime_formats",
        "_float_round_prec",
    )

    def __init__(
        self,
        shared: pa.LargeStringArray,
        binary: bool = False,  # noqa: FBT001, FBT002
        *,
        conflict_resolve: Literal["no", "temporal", "numeric", "all"] = "no",
        datetime_formats: tuple[str, ...] | None = None,
        float_precision: int = 6,
    ) -> None:
        self.__chunks: list[chunk] = []
        self.__chunk: list = []
        self.__xdtr: int = 0
        self.__s_row: int = -1
        self.__e_row: int = -1
        self.__shared: pa.LargeStringArray = shared
        self._float_round_prec = float_precision
        self._datetime_formats = datetime_formats
        self._conf_to_datetime = conflict_resolve in {"temporal", "all"}
        self._conf_to_numerics = conflict_resolve in {"numeric", "all"}

        self.__ch_off: int = 0

        self.__binary = binary

    def __len__(self) -> int:
        return self.__e_row - self.__s_row + 1

    def __pull_chunk(self) -> None:
        """Normalize chunk and add it to underlying list."""
        if self.__xdtr & TYPE_STRING:
            if self.__binary:
                c = chunk.xlsb_strings(
                    self.__chunk,
                    is_shared=bool(self.__xdtr & REPR_SHARED),
                    shareds=self.__shared,
                )
            else:
                c = chunk.xlsx_strings(
                    self.__chunk,
                    is_shared=bool(self.__xdtr & REPR_SHARED),
                    shareds=self.__shared,
                )
        elif self.__binary:
            c = chunk.xlsb_numeric(
                self.__chunk,
                is_boolean=bool(self.__xdtr & REPR_BOOLEAN),
                is_temporal=bool(self.__xdtr & REPR_TEMPORAL),
                is_rknumber=bool(self.__xdtr & REPR_RKNUMBER),
            )
        else:
            c = chunk.xlsx_numeric(
                self.__chunk,
                is_temporal=bool(self.__xdtr & REPR_TEMPORAL),
            )

        if self.__ch_off:
            self.__chunks.append(chunk.nulls(self.__ch_off))

        self.__chunks.append(c)

        self.__ch_off = 0
        self.__chunk.clear()

    def add(self, c: cell) -> int:
        # ! If cell row already here, skip add
        _row = c.row
        if _row <= self.__e_row:
            return self.__e_row

        # ! If we must append some null values, before new add:
        if _row > self.__e_row + 1:
            # ? If current chunk is not already added, add it.
            if self.__chunk:
                self.__pull_chunk()
            # ? Add nulls to chunks
            self.__ch_off = _row - self.__e_row - 1
            # ? Set current type to -1 (NULL)
            self.__xdtr = 0

        # ! If current cell has different type or repr, than pull current chunk, and start another
        if c.xdt != self.__xdtr:
            if self.__chunk:
                self.__pull_chunk()
            self.__xdtr = c.xdt

        # * Append cell value to the current chunk.
        self.__chunk.append(c.val)
        # * Set current row to cell row
        self.__e_row = _row
        # * If offset row is not set, set it to the current cell row
        if self.__s_row < 0:
            self.__s_row = _row

        # ? Returns current row
        return self.__e_row

    def to_utf8(
        self,
        offset: int = 0,
        length: int = 0,
        *,
        idx: pa.BooleanArray | None = None,
    ) -> pa.LargeStringArray:
        """
        Create pyarrow.Array of type LargeString, using (optional) offset and length.

        Original data still remains untouched in `self.__chunks`

        Useful for fetching a header from array
        """
        # NOTE: pyarrow.concat_arrays actually support Iterable[pyarrow.Array], so generators are also supported.
        if self.__chunk:
            self.__pull_chunk()
        if not self.__chunks:
            if length:
                return pa.nulls(length).cast("large_string")
            return pa.nulls(0).cast("large_string")
        return chunk.concatenate(
            self.__chunks,
            offset,
            length,
            idx,
            conf_to_datetime=self._conf_to_datetime,
            conf_to_numerics=self._conf_to_numerics,
            datetime_formats=self._datetime_formats,
            float_round_prec=self._float_round_prec,
        ).cast("large_string")

    def to_arrow(
        self,
        offset: int = 0,
        length: int = 0,
        *,
        idx: pa.BooleanArray | None = None,
    ) -> pa.Array:
        if self.__chunk:
            self.__pull_chunk()
        if not self.__chunks:
            if length:
                return pa.nulls(length)
            return pa.nulls(0)
        return chunk.concatenate(
            self.__chunks,
            offset,
            length,
            idx,
            conf_to_datetime=self._conf_to_datetime,
            conf_to_numerics=self._conf_to_numerics,
            datetime_formats=self._datetime_formats,
            float_round_prec=self._float_round_prec,
        )
