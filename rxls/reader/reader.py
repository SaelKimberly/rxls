import contextlib
import typing
import re
from datetime import time
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
import polars as pl
import pyarrow as pa

from ..cell import cell, xl_type
from .xlsb_reader import create_scanner as xlsb_scanner
from .xlsx_reader import create_scanner as xlsx_scanner

if typing.TYPE_CHECKING:
    from typing_extensions import TypeAlias
    from typing import IO, Literal, TypeVar, cast, overload

    T = TypeVar("T")
    PaArray: TypeAlias = "pa.Array[T, pa.Scalar[T]] | pa.ChunkedArray[T, pa.Scalar[T]]"  # type: ignore

__all__ = ["xl_scan", "xl_series"]


MSECS_PER_DAY = pl.lit(86_400_000)
WINDOWS_EPOCH = pl.lit(-25_569)


class xl_series:
    __slots__ = ("index", "head", "drop", "fmts", "_name", "cells", "types", "rowno", "inferring", "chunk", "frnd")

    def __init__(
        self,
        index: int,
        *,
        head_rows: int = 0,
        drop_rows: int = 0,
        date_fmts: "list[str] | None" = None,
        inferring: 'Literal["no", "basic", "strict", "extended"]' = "no",
        frounding: "int | None" = None,
    ) -> None:
        self.index: int = index
        self.head: int = head_rows
        self.drop: int = drop_rows
        self.fmts: "list[str]" = (
            date_fmts
            if date_fmts is not None
            else [
                "%d.%m.%Y %T%.f",
                "%d.%m.%Y %T",
                "%d.%m.%Y",
                "%d/%m/%Y %T%.f",
                "%d/%m/%Y %T",
                "%d/%m/%Y",
                "%F %T%.f",
                "%F %T",
                "%F",
            ]
        )
        self._name: str = ""
        self.frnd = frounding

        self.cells: "list[PaArray[str]]" = []
        self.types: "dict[xl_type, int]" = {}
        self.rowno: int = -1

        self.inferring: 'Literal["no", "basic", "strict", "extended"]' = inferring

        self.chunk: "list[str]" = []

    @property
    def name(self) -> "str | None":
        return self._name.strip(": ") or None

    @name.setter
    def name_setter(self, new_name: str) -> None:
        assert new_name and isinstance(new_name, str), f"Series name cannot be set to `{new_name}`!"
        self._name = new_name

    def add(self, c: cell) -> int:
        if c.row < self.head:
            self._name += f"::{self.head - c.row}|{c.val}"

        elif (n_row := c.row - self.head - self.drop) >= 0:
            if n_row - self.rowno > 1:
                if self.chunk:
                    self.cells.append(pa.array(self.chunk, pa.utf8()))
                    self.chunk.clear()
                self.cells.append(pa.nulls(n_row - self.rowno - 1, pa.utf8()))
            if c.val is not None:
                self.chunk.append(c.val)
            self.types[c.typ] = self.types.get(c.typ, 0) + 1

            self.rowno = n_row

        return max(0, self.rowno)

    def to_arrow(self, length: int, index: "pa.BooleanArray | None" = None) -> "PaArray":
        # sourcery skip: low-code-quality
        if self.chunk:
            self.cells.append(pa.array(self.chunk, pa.utf8()))
            self.chunk.clear()

        if index is not None:
            length = len(index)

        if not self.cells and length > 0:
            arr = pa.nulls(length, pa.utf8())
        else:
            arr: "pa.Array[str, pa.Scalar[str]]" = pa.concat_arrays(self.cells)  # type: ignore

        if length > 0:
            if len(arr) < length:
                arr = pa.concat_arrays([arr, pa.nulls(length - len(arr), pa.utf8())])
            elif len(arr) > length:
                arr = arr.slice(0, length)

        if index is not None:
            arr = arr.filter(index, null_selection_behavior="drop")

        ser = pl.Series(arr)

        if self.inferring == "no":
            return ser.to_arrow().cast("string")  # type: ignore

        strict = False
        most_used_type: xl_type
        if self.inferring in ("basic", "extended"):
            most_used_type = next(
                (x for x, cnt in self.types.items() if cnt == max(self.types.values())), xl_type.STRINGS
            )
        else:
            strict = True
            if len(self.types) > 1:
                if {xl_type.DATETIME, xl_type.DATETYPE}.issuperset(self.types):
                    most_used_type = xl_type.DATETIME
                elif {xl_type.FLOATIN, xl_type.INTEGER}.issuperset(self.types):
                    most_used_type = xl_type.FLOATIN
                elif {xl_type.FLOATIN, xl_type.DATETYPE}.issuperset(self.types):
                    most_used_type = xl_type.DATETYPE
                elif {xl_type.FLOATIN, xl_type.DATETIME, xl_type.DATETYPE}.issuperset(self.types):
                    most_used_type = xl_type.DATETIME
                else:
                    most_used_type = xl_type.STRINGS
            else:
                most_used_type = next(iter(self.types.keys()), xl_type.STRINGS)

        if most_used_type in (xl_type.STRINGS, xl_type.BOOLEAN):
            pass  # NOSONAR

        elif most_used_type in (xl_type.INTEGER, xl_type.FLOATIN):
            ret = ser.cast(pl.Float64, strict=False)
            if self.frnd is not None:
                ret = ret.round(self.frnd)

            if ((ret.drop_nulls() % 1.0) == 0.0).all():
                ret = ret.cast(pl.Int64)

            if not strict and (ret.null_count() == ser.null_count()):
                return ret.to_arrow()

        elif most_used_type == xl_type.DATETIME:
            ret = (
                ser.to_frame("v")
                .with_columns(f=pl.col("v").cast(pl.Float64, strict=False))
                .with_columns(
                    f=(((pl.col("f") < pl.lit(60.0)).cast(float) + pl.col("f") + WINDOWS_EPOCH) * MSECS_PER_DAY)
                )
                .with_columns(
                    s=pl.coalesce(
                        pl.col("f").cast(pl.Int64, strict=False).cast(pl.Datetime("ms")),
                        *[pl.col("v").str.strptime(pl.Datetime, fmt, strict=False) for fmt in self.fmts],
                    )
                )
                .get_column("s")
                .alias(ser.name)
            )

            if not strict or (ret.null_count() == ser.null_count()):
                if ret.drop_nulls().dt.time().__eq__(time.min).all():
                    return ret.dt.date().to_arrow()
                return ret.dt.round("1s").to_arrow()

        elif most_used_type == xl_type.DATETYPE:
            ret = (
                ser.to_frame("v")
                .with_columns(f=pl.col("v").cast(pl.Float64, strict=False))
                .with_columns(
                    f=(((pl.col("f") < pl.lit(60.0)).cast(float) + pl.col("f") + WINDOWS_EPOCH) * MSECS_PER_DAY)
                )
                .with_columns(
                    s=pl.coalesce(
                        pl.col("f").cast(pl.Int64, strict=False).cast(pl.Datetime("ms")),
                        *[pl.col("v").str.strptime(pl.Datetime, fmt, strict=False) for fmt in self.fmts],
                    )
                )
                .get_column("s")
                .alias(ser.name)
            )

            if not strict or (
                (ret.null_count() == ser.null_count()) and ret.drop_nulls().dt.time().__eq__(time.min).all()
            ):
                return ret.cast(pl.Date).to_arrow()

        elif most_used_type == xl_type.TIMETYPE:
            ret = (
                ser.to_frame("v")
                .with_columns(f=pl.col("v").cast(pl.Float64, strict=False) * MSECS_PER_DAY)
                .with_columns(
                    s=(
                        pl.datetime(1970, 1, 1) + pl.duration(milliseconds=pl.col("f").cast(pl.Int64, strict=False))
                    ).dt.time()
                )
                .get_column("s")
                .alias(ser.name)
            )

            if not strict or (ret.null_count() == ser.null_count()):
                return ret.to_arrow()

        elif most_used_type == xl_type.DURATION:
            ret = (
                ser.to_frame("v")
                .with_columns(f=pl.col("v").cast(pl.Float64, strict=False) * MSECS_PER_DAY)
                .with_columns(s=pl.duration(milliseconds=pl.col("f").cast(pl.Int64, strict=False)))
                .get_column("s")
                .alias(ser.name)
            )

            if not strict or (ret.null_count() == ser.null_count()):
                return ret.to_arrow()

        if self.inferring == "extended":
            with contextlib.suppress(Exception):
                if (xl_type.FLOATIN in self.types or xl_type.INTEGER in self.types) and ser.drop_nulls().str.contains(
                    r"^(?:(?:0(?:\.0+)?)|(?:-?(?:(?:[1-9]\d*)|0)\.\d*[1-9]\d*)|(?:-?[1-9]\d*))$"
                ).all():
                    ret = ser.cast(pl.Float64)
                    if self.frnd is not None:
                        ret = ret.round(self.frnd)
                    if ((ret.drop_nulls() % 1.0) == 0.0).all():
                        ret = ret.cast(pl.Int64)
                    return ret.to_arrow()

                if (
                    ser.drop_nulls()
                    .str.contains(
                        r"^(?:(?:\d{4}-\d{2}-\d{2})|(?:\d{2}[./]\d{2}[./]\d{4}))(?:.\d{2}:\d{2}:\d{2}(?:\.\d+)?)?$"
                    )
                    .all()
                ):
                    ret = (
                        ser.to_frame("v")
                        .with_columns(
                            s=pl.coalesce(
                                *[pl.col("v").str.strptime(pl.Datetime, fmt, strict=False) for fmt in self.fmts]
                            )
                        )
                        .get_column("s")
                    )
                    if ret.null_count() == ser.null_count():
                        return ret.to_arrow()

        return ser.to_arrow()

    if typing.TYPE_CHECKING:

        @overload
        def to_series(
            self, length: int, index: "pl.Series | pa.BooleanArray | None" = None, *, mode: 'Literal["pl"]' = "pl"
        ) -> "pl.Series":
            ...

        @overload
        def to_series(
            self, length: int, index: "pl.Series | pa.BooleanArray | None" = None, *, mode: 'Literal["pd"]'
        ) -> "pd.Series":
            ...

    def to_series(
        self, length: int, index: "pl.Series | pa.BooleanArray | None" = None, *, mode: 'Literal["pd", "pl"]' = "pl"
    ) -> "pl.Series | pd.Series":
        if index is not None and isinstance(index, pl.Series):
            if index.is_boolean():
                index = index.to_arrow()  # type: ignore
            else:
                index = index.is_not_null().to_arrow()  # type: ignore

        ret = cast(pl.Series, pl.from_arrow(self.to_arrow(length, index if index is not None else None)))  # type: ignore
        if mode == "pd":
            ret = ret.to_arrow().to_pandas(self_destruct=True)
            return ret.rename(self.name) if self.name else ret  # type: ignore
        return ret.alias(self.name) if self.name else ret


def create_header(cols: "dict[int, xl_series] | dict[str, xl_series]") -> "list[str]":
    result = [col.name or f"Unnamed: {i}" for i, col in enumerate(cols.values())]

    hdrs = []
    max_level = None
    last_hdrx = {}
    for name in result:
        try:
            parts = {int(no): nm for no, nm in (part.split("|", 1) for part in name.split("::"))}
        except Exception:
            parts = {max_level or 1: name}

        ptmax = max(parts)
        if max_level is None:
            max_level = ptmax

        elif ptmax < max_level:
            parts |= {i: nm for i, nm in last_hdrx.items() if i > ptmax}

        last_hdrx = parts

        hdrs.append(", ".join(last_hdrx[p] for p in reversed(sorted(last_hdrx))))
    result = hdrs

    return result


if typing.TYPE_CHECKING:

    @overload
    def xl_scan(
        xl_file: "str | Path | IO[bytes]",
        sheet: "int | str" = 0,
        *,
        mode: 'Literal["pd", "pl"]' = "pl",
        head: "int | list[str]" = 0,
        skip_rows: int = 0,
        drop_rows: int = 0,
        take_rows: int = -1,
        drop_cels: "str | None" = None,
        with_tqdm: bool = True,
        book_name: "str | None" = None,
        index_col: "str | None" = None,
        inferring: 'Literal["no", "basic", "strict", "extended"]' = "basic",
        frounding: "int | None" = None,
        keep_rows: bool = False,
    ) -> pl.DataFrame:
        ...

    @overload
    def xl_scan(
        xl_file: "str | Path | IO[bytes]",
        sheet: "int | str" = 0,
        *,
        mode: 'Literal["pd"]' = "pd",
        head: "int | list[str]" = 0,
        skip_rows: int = 0,
        drop_rows: int = 0,
        take_rows: int = -1,
        drop_cels: "str | None" = None,
        with_tqdm: bool = True,
        book_name: "str | None" = None,
        index_col: "str | None" = None,
        inferring: 'Literal["no", "basic", "strict", "extended"]' = "basic",
        frounding: "int | None" = None,
        keep_rows: bool = False,
    ) -> pd.DataFrame:
        ...


def xl_scan(
    xl_file: "str | Path | IO[bytes]",
    sheet: "int | str" = 0,
    *,
    mode: 'Literal["pd", "pl"]' = "pl",
    head: "int | list[str]" = 0,
    skip_rows: int = 0,
    drop_rows: int = 0,
    take_rows: int = -1,
    drop_cels: "str | None" = None,
    with_tqdm: bool = True,
    book_name: "str | None" = None,
    index_col: "str | None" = None,
    inferring: 'Literal["no", "basic", "strict", "extended"]' = "basic",
    frounding: "int | None" = None,
    keep_rows: bool = False,
) -> "pl.DataFrame | pd.DataFrame":  # sourcery skip: low-code-quality
    f_input: 'Literal["xlsx", "xlsb"]'
    if isinstance(xl_file, str):
        xl_file = Path(xl_file)
    if issubclass(type(xl_file), Path):
        suff = cast(Path, xl_file).suffix.lower()
        if suff == ".xlsx":
            f_input = "xlsx"
        elif suff == ".xlsb":
            f_input = "xlsb"
        else:
            raise ValueError(f"File has incorrect suffix: `{suff}`. Required `.xlsx` or `.xlsb`")

    else:
        with ZipFile(xl_file, "r") as _zf:
            if "xl/workbook.bin" in _zf.namelist():
                f_input = "xlsb"
            elif "xl/workbook.xml" in _zf.namelist():
                f_input = "xlsx"
            else:
                raise ValueError("Excel workbook has unsupported format. `XLSB` and `XLSX` formats supported only")

    scanner = xlsx_scanner if f_input == "xlsx" else xlsb_scanner

    cols: "dict[int, xl_series] | dict[str, xl_series]" = {}
    nrow: int = 0
    head_rows = head if isinstance(head, int) else 0

    for c in scanner(
        xl_file,
        sheet,
        take_rows=take_rows,
        skip_rows=skip_rows,
        drop_cels=drop_cels,
        with_tqdm=with_tqdm,
        book_name=book_name,
        keep_rows=keep_rows,
    ):
        nrow = cols.setdefault(
            c.col, xl_series(c.col, head_rows=head_rows, drop_rows=drop_rows, inferring=inferring, frounding=frounding)
        ).add(c)
        if 0 < take_rows < nrow:
            nrow = take_rows
            break

    cols = {col: cols[col] for col in sorted(cols)}

    if isinstance(head, list):
        names = [name for name, _ in zip(head, cols)]
    else:
        names = create_header(cols)
    used: "list[str]" = []

    for name in names:
        if name in used:
            n = name
            i = 1
            while (x := f"{n}. {i}") in used:
                i += 1

            used.append(x)
        else:
            used.append(name)

    names = list(used)
    cols = {name: cols[col] for name, col in zip(names, cols)}

    # print([(x.name, x.types) for x in cols.values()])

    if index_col is not None:
        index_col_name = next((name for name in names if re.match(index_col, name)), None)
        if index_col_name is None:
            raise KeyError(f"Column not found by regular expression: {index_col}")

        index_series: pl.Series = cols[index_col_name].to_series(nrow - skip_rows - head_rows - drop_rows + 2)  # type: ignore

        index_series = (
            index_series.to_frame("v").filter(pl.col("v").backward_fill().is_not_null()).get_column("v").is_not_null()
        )

        frames = {name: col.to_series(index_series.len(), index=index_series, mode=mode) for name, col in cols.items()}
    else:
        frames = {
            name: col.to_series(nrow - skip_rows - head_rows - drop_rows + 2, mode=mode) for name, col in cols.items()
        }

    return pl.DataFrame(frames) if mode == "pl" else pd.DataFrame(frames)  # type: ignore
