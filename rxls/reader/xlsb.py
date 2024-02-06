# ruff: noqa:C901, D102, D105, D107, ANN002, ANN204, PLR0915, TRY003, EM101, PLR2004
from __future__ import annotations

__all__ = ["XlsbWorkbook"]

import re
from functools import cached_property
from typing import TYPE_CHECKING
from zipfile import ZipFile

import pyarrow as pa

from ..biff import scan_biff
from ..biff_recs import BrtBundleSh
from ..cell import cell
from ..constants import (
    REPR_BOOLEAN,
    REPR_PREPARED,
    REPR_RKNUMBER,
    REPR_SHARED,
    REPR_TEMPORAL,
    TYPE_NUMERIC,
    TYPE_STRING,
)
from ..core import u2_p, u4_p, u4_u
from ..rels import Relationship

if TYPE_CHECKING:
    from pathlib import Path
    from typing import IO, Callable, Iterator, Sequence

    from typing_extensions import Self

re_dt = re.compile(r"(?<!\\)[dmhysDMHYS]")
re_xt = re.compile(r'(?:".*?")|(?:\[(?!(?:hh?|mm?|ss?)\])[^\]]*\])')

ERROR_STRINGS = frozenset(
    x.encode("utf-16")
    for x in (
        "#NULL!",
        "#DIV/0!",
        "#VALUE!",
        "#REF!",
        "#NAME?",
        "#NUM!",
        "#N/A",
        "#GETTING_DATA",
        "#ДЕЛ/0!",
        "#ЗНАЧ!",
        "#ССЫЛКА!",
        "#ИМЯ?",
        "#ЧИСЛО!",
        "#Н/Д",  # noqa: RUF001
    )
)

TEMPORAL_STYLES = frozenset(
    [0x0E, 0x0F, 0x10, 0x11, 0x12, 0x12, 0x13, 0x14, 0x15, 0x16, 0x2D, 0x2E, 0x2F],
)


class XlsbWorkbook:  # noqa: D101
    __slots__ = ("__dict__", "__file", "null_shared", "null_values", "zf")

    def __init__(
        self,
        file: str | Path | IO[bytes],
        *,
        null_values: Sequence[str] | Callable[[str], bool] | None = None,
    ) -> None:
        self.__file = file

        skip_predicate: Callable[[str], bool]
        if null_values is not None:
            if callable(null_values):
                skip_predicate: Callable[[str], bool] = (  # noqa: E731
                    lambda s: s in ERROR_STRINGS or null_values(s)  # type: ignore
                )
            else:
                skip_predicate: Callable[[str], bool] = frozenset(
                    list(ERROR_STRINGS) + list(null_values),
                ).__contains__
        else:
            skip_predicate: Callable[[str], bool] = ERROR_STRINGS.__contains__

        self.null_values = skip_predicate

        self.zf: ZipFile | None = None
        self.null_shared: set[bytes] = set()

    def __enter__(self) -> Self:
        self.zf = ZipFile(self.__file).__enter__()
        if "xl/workbook.bin" not in self.zf.namelist():
            raise AssertionError("Incorrect xlsb file")
        return self

    def __exit__(self, *_):
        if self.zf is not None:
            self.zf.__exit__(*_)

    @cached_property
    def sheets(self) -> dict[str, str]:
        if self.zf is None:
            msg = "Workbook is not open"
            raise RuntimeError(msg)

        s_sheet_idx: dict[str, str] = {}

        with self.zf.open("xl/_rels/workbook.bin.rels", "r") as io:
            wb_rels: list[Relationship] = Relationship.scan_xml(io)

        with self.zf.open("xl/workbook.bin") as io:
            for _sheet in scan_biff(io, {412}):
                _sheet = BrtBundleSh.loadr(_sheet)

                sheet_file = "xl/" + next(
                    x.Target for x in wb_rels if x.Id == _sheet.strRelID.value
                )

                s_sheet_idx[_sheet.strName.value] = sheet_file
        return s_sheet_idx

    @cached_property
    def shared(self) -> pa.LargeStringArray:
        """Scan xl/sharedStrings.bin in XSLB contents. Returns shared strings array, optimized for using with PyArrow."""
        if self.zf is None:
            msg = "Workbook is not open"
            raise RuntimeError(msg)

        with self.zf.open("xl/sharedStrings.bin") as io:
            return pa.array(
                (self.null_shared.add(u4_p(i)) or y)
                if self.null_values(
                    y := r.data[5 : 5 + u4_u(r.data[1:5])[0] * 2].decode("utf-16"),
                )
                else y
                for i, r in enumerate(scan_biff(io, {19}))
            ).cast("large_string")

    @cached_property
    def styles(self) -> frozenset[bytes]:
        """Scan xl/styles.bin in XLSB contents. Returns optimized temporal styles set for fastest parsing."""
        if self.zf is None:
            msg = "Workbook is not open"
            raise RuntimeError(msg)

        with self.zf.open("xl/styles.bin") as io:
            ret: set[bytes] = set()
            # ! [0]: Get full styles list from styles.bin (3-bytes little-endian XF ID and 2-bytes little-endian FMT ID)
            xfs: dict[bytes, bytes] = {
                u2_p(i) + b"\x00": fmt
                for i, fmt in enumerate(
                    r.data[2:4]
                    for r in scan_biff(io, {47})
                    if r.data[:2] != b"\xff\xff"
                )
            }
            io.seek(0)
            # ! [1]: Get full formats list from custom BrtFmt records (only 1-st format from every record)
            fms = {
                r.data[:2]: r.data[2:].decode("utf-16").split(";", 1)[0]
                for r in scan_biff(io, {44})
            }

            # ! [2]: Check, for each format, that it is temporal. Add XF IDs of BrtXFs using it to result.
            for f_id, fmt in fms.items():
                xf_ids = [x for x, y in xfs.items() if y == f_id]
                if fmt == "0":
                    ret.update(dict.fromkeys(xf_ids, 0x10))

                    continue
                if re_dt.search(re_xt.sub("", fmt)):
                    ret.update(xf_ids)

            # ! [3]: Add to result each BrtXF, where FmtId in date/time/duration builtin formats
            ret.update((x for x, y in xfs.items() if y[0] in TEMPORAL_STYLES))

            return frozenset(ret)  # type: ignore

    def iter_worksheet(
        self,
        sheet: int | str = 0,
        *,
        skip_rows: int = 0,
        take_rows: int = 0,
        take_rows_non_empty: bool = True,
        skip_cols: Sequence[int] | None = None,
        row_callback: Callable[[], None] | None = None,
    ) -> Iterator[cell]:
        if self.zf is None:
            msg = "Workbook is not open"
            raise RuntimeError(msg)

        _sheets = self.sheets

        if isinstance(sheet, int):
            if not (0 <= sheet < len(_sheets)):
                raise IndexError(sheet)
            sheet_path = next(
                y for i, (_, y) in enumerate(self.sheets.items()) if i == sheet
            )
        else:
            if sheet not in _sheets:
                raise KeyError(sheet)
            sheet_path = self.sheets[sheet]

        if not isinstance(skip_rows, int) or skip_rows < 0:
            raise ValueError("Argument `skip_rows` must be a positive integer")
        if not isinstance(take_rows, int) or take_rows < 0:
            raise ValueError("Argument `take_rows` must be a positive integer")

        take_rows += skip_rows
        row = -1
        is_empty_row = False

        with self.zf.open(sheet_path) as io:
            # ? Used record IDs:
            # * 0: BrtRowHdr
            # * 2: BrtCellRk        | TYPE_NUMERIC | REPR_RKNUMBER [| REPR_TEMPORAL]
            # * 4: BrtCellBool      | TYPE_NUMERIC | REPR_BOOLEAN
            # * 5: BrtCellReal      | TYPE_NUMERIC | [REPR_TEMPORAL | REPR_PREPARED]
            # * 6: BrtCellSt        | TYPE_STRING | REPR_PREPARED
            # * 7: BrtCellIsst      | TYPE_STRING | REPR_SHARED
            # * 8: BrtFmlaString    | TYPE_STRING | REPR_PREPARED
            # * 9: BrtFmlaNum       | TYPE_NUMERIC | [REPR_TEMPORAL | REPR_PREPARED]
            # * 10: BrtFmlaBool     | TYPE_NUMERIC | REPR_BOOLEAN
            # * 402: BrtEndSheetData
            for r in scan_biff(io, {0, 2, 4, 5, 6, 7, 8, 9, 10, 402}):
                if not r.rec_id:
                    if not is_empty_row or take_rows_non_empty:
                        row += 1
                    # ? If upcoming row number is greater than take_rows, stop iteration
                    if 0 < take_rows < row:
                        break

                    if skip_rows <= row and row_callback:
                        row_callback()

                    is_empty_row = True
                    continue

                # ? If upcoming row number is less than skip_rows, no data processing.
                if skip_rows > row:
                    continue

                if r.rec_id == 402:
                    row += 1
                    if row_callback:
                        row_callback()
                    break

                (col,) = u4_u(r.data[:4])

                if skip_cols and col in skip_cols:
                    continue

                adv_repr = REPR_TEMPORAL if r.data[4:7] in self.styles else 0

                if r.rec_id in {6, 8}:
                    # NOTE: The slowest case, due to utf-16 encoding.
                    cell_data = r.data[12 : u4_u(r.data[8:12])[0] * 2 + 12].decode(
                        "utf-16",
                    )
                    if self.null_values(cell_data):
                        continue

                    yield cell(
                        row - skip_rows,
                        col,
                        TYPE_STRING | REPR_PREPARED,
                        cell_data,  # type: ignore
                    )

                elif r.rec_id in {5, 9}:
                    yield cell(
                        row - skip_rows,
                        col,
                        TYPE_NUMERIC | (adv_repr or REPR_PREPARED),
                        r.data[8:16],
                    )

                elif r.rec_id in {4, 10}:
                    yield cell(
                        row - skip_rows,
                        col,
                        TYPE_NUMERIC | REPR_BOOLEAN,
                        r.data[8:9],
                    )

                elif r.rec_id == 7:
                    if (v := r.data[8:12]) not in self.null_shared:
                        yield cell(row - skip_rows, col, TYPE_STRING | REPR_SHARED, v)
                    else:
                        continue
                else:
                    yield cell(
                        row - skip_rows,
                        col,
                        TYPE_NUMERIC | REPR_RKNUMBER | adv_repr,
                        r.data[8:12],
                    )

                is_empty_row = False
