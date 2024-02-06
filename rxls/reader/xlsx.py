# ruff: noqa:C901, D102, D105, D107, ANN002, ANN204, PLR0915, TRY003, EM101, PLR2004
from __future__ import annotations

__all__ = ["XlsxWorkbook"]


import re
from functools import cached_property
from queue import SimpleQueue as Queue
from threading import Thread
from typing import TYPE_CHECKING
from zipfile import ZipFile

import pyarrow as pa
import pyarrow.lib
from pyexpat import ParserCreate

from ..cell import cell
from ..constants import (
    REPR_PREPARED,
    REPR_SHARED,
    REPR_TEMPORAL,
    TYPE_NUMERIC,
    TYPE_STRING,
)
from ..core import NUMBA_AVAILABLE
from ..rels import Relationship

if TYPE_CHECKING:
    from pathlib import Path
    from typing import IO, Callable, Iterator, Sequence, TypeVar

    from typing_extensions import Self

    T = TypeVar("T")

re_dt = re.compile(r"(?<!\\)[dmhysDMHYS]")
re_xt = re.compile(r'(?:".*?")|(?:\[(?!(?:hh?|mm?|ss?)\])[^\]]*\])')

re_date = re.compile(r"[ydYD]")
re_time = re.compile(r"[hsHS]")
re_span = re.compile(
    r"(?i)\[hh?\](:mm(:ss(\.0*)?)?)?|\[mm?\](:ss(\.0*)?)?|\[ss?\](\.0*)?",
)

ERROR_STRINGS = frozenset(
    (
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
    ),
)


def col_idx(col: bytes) -> int:
    a = -1
    i = 0
    for x in col[::-1]:
        if x > 64:
            a += (x - 64) * (26**i)
            i += 1
    return a


if NUMBA_AVAILABLE:
    import numba as nb

    col_idx = nb.njit(nb.types.int32(nb.types.Bytes(nb.u1, 1, "C", True)))(col_idx)  # type: ignore


TEMPORAL_STYLES = frozenset(
    [0x0E, 0x0F, 0x10, 0x11, 0x12, 0x12, 0x13, 0x14, 0x15, 0x16, 0x2D, 0x2E, 0x2F],
)

NUMERIC_STYLES = frozenset([0x01, 0x02, 0x03, 0x04, 0x25, 0x26, 0x27, 0x28, 0x30])


class XlsxWorkbook:  # noqa: D101
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
        self.null_shared: set[str] = set()

    def __enter__(self) -> Self:
        self.zf = ZipFile(self.__file).__enter__()
        if "xl/workbook.xml" not in self.zf.namelist():
            raise AssertionError("Invalid xlsx file")
        return self

    def __exit__(self, *_):
        if self.zf is not None:
            self.zf.__exit__(*_)

    @cached_property
    def sheets(self) -> dict[str, str]:
        if self.zf is None:
            msg = "Workbook is not open"
            raise RuntimeError(msg)

        rId_to_file = {}  # noqa: N806
        with self.zf.open("xl/_rels/workbook.xml.rels", "r") as io:
            wb_rels: list[Relationship] = Relationship.scan_xml(io)
            for rel in wb_rels:
                if rel.Type == "worksheet":
                    rId_to_file[rel.Id] = f"xl/{rel.Target}"

        rId_to_name = {}  # noqa: N806
        with self.zf.open("xl/workbook.xml") as io:

            def st_handler(tag: str, attr: dict[str, str]) -> None:
                nonlocal rId_to_name
                if tag == "sheet":
                    rId_to_name[attr["r:id"]] = attr["name"]

            parser = ParserCreate()
            parser.StartElementHandler = st_handler
            parser.ParseFile(io)
        return {rId_to_name[x]: rId_to_file[x] for x in rId_to_name}

    @cached_property
    def shared(self) -> pa.LargeStringArray:
        if self.zf is None:
            msg = "Workbook is not open"
            raise RuntimeError(msg)

        ret: pyarrow.lib.StringBuilder = pyarrow.lib.StringBuilder()

        with self.zf.open("xl/sharedStrings.xml") as io:
            must_be = False

            def cell_sh(tag: str, _: dict[str, str]) -> None:
                nonlocal must_be
                if tag == "si":
                    must_be = True

            def cell_th(txt: str) -> None:
                nonlocal must_be, ret
                if must_be and (txt := txt.strip()):
                    if self.null_values(txt):
                        return
                    ret.append(txt)
                    must_be = False

            def cell_eh(tag: str) -> None:
                nonlocal must_be
                if tag == "si" and must_be:
                    ret.append(None)  # type: ignore
                    self.null_shared.add(str(len(ret) - 1))
                    must_be = False

            parser = ParserCreate()
            parser.StartElementHandler = cell_sh
            parser.EndElementHandler = cell_eh
            parser.CharacterDataHandler = cell_th
            parser.buffer_text = True

            parser.ParseFile(io)

        return ret.finish().cast("large_string")

    @cached_property
    def styles(self) -> dict[str, int]:
        if self.zf is None:
            msg = "Workbook is not open"
            raise RuntimeError(msg)

        ret_temporal: set[str] = set()
        ret_numerics: set[str] = set()

        xfs_id = 0
        xfs: dict[str, str] = {}

        fms: dict[str, str] = {}

        with self.zf.open("xl/styles.xml") as io:

            def cell_sh(tag: str, attrs: dict[str, str]) -> None:
                nonlocal xfs, fms, xfs_id

                if tag == "numFmt":
                    fms[attrs["numFmtId"]] = attrs["formatCode"].split(";", 1)[0]

                elif tag == "xf" and "xfId" in attrs:
                    xfs[str(xfs_id)] = attrs["numFmtId"]
                    xfs_id += 1

            parser = ParserCreate()
            parser.StartElementHandler = cell_sh
            parser.buffer_text = True

            parser.ParseFile(io)

        for f_id, fmt in fms.items():
            xf_id = [x for x, y in xfs.items() if y == f_id]
            if fmt == "0" or ".00" in fmt:
                ret_numerics.update(xf_id)

            elif re_dt.search(re_xt.sub("", fmt)):
                ret_temporal.update(xf_id)

        ret_temporal.update(
            (
                x
                for x, y in xfs.items()
                if int(y) in TEMPORAL_STYLES and x not in ret_temporal
            ),
        )

        ret_numerics.update(
            (
                x
                for x, y in xfs.items()
                if int(y) in NUMERIC_STYLES and x not in ret_numerics
            ),
        )

        ret = dict.fromkeys(ret_temporal, TYPE_NUMERIC | REPR_TEMPORAL)
        ret.update(dict.fromkeys(ret_numerics, TYPE_NUMERIC))
        return ret

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

        def worker(
            io: IO[bytes],
            styles: dict[str, int],
            nullss: frozenset[str],
            nullpd: Callable[[str], bool],
            skip_rows: int,
            take_rows: int,
            take_non_empty: bool,  # noqa: FBT001
            skip_cols: Sequence[int] | None,
            o_q: Queue[cell | T],
            sentinel: T,
            callback: Callable[[], None] | None,
            /,
        ) -> None:
            tags: list[str] = []

            row = -1
            "Number of current row"

            is_empty_row: bool = True
            is_sharedstr: bool = False

            # cell fields
            c_row: int = 0
            c_typ: int = 0

            l_col: str = ""

            l_tag: str | None = None

            # ? First row started event:
            row += 1
            if callback is not None:
                callback()

            def cell_sh(tag: str, attrs: dict[str, str]) -> None:
                nonlocal row, tags, l_col, c_typ, c_row, styles, l_tag, skip_rows
                if not l_tag:
                    # ? If tags is empty, we are outside of sheet data.
                    if tag == "c" and row >= skip_rows:
                        # ? get cell coordinates
                        l_col = attrs["r"]

                        # ? check, if value is shared string index:
                        if attrs.get("t") == "s":
                            c_typ = TYPE_STRING | REPR_SHARED
                        # ? check, if value has temporal or numeric representation
                        elif typ := styles.get(attrs.get("s", "0")):
                            c_typ = typ
                        else:
                            c_typ = TYPE_STRING | REPR_PREPARED

                        c_row = row
                    else:
                        return
                # ? Add to tags only if it is already contains something.
                # ? Also, add if it is empty and current tag == 'c'
                l_tag = tag
                # tags.append(l_tag := tag)

            def cell_th(txt: str) -> None:
                nonlocal \
                    o_q, \
                    row, \
                    l_col, \
                    c_typ, \
                    l_tag, \
                    is_empty_row, \
                    is_sharedstr, \
                    nullss, \
                    nullpd, \
                    skip_rows, \
                    skip_cols

                if skip_rows > row:
                    return

                if l_tag == "v" and (txt := txt.strip()):
                    if is_sharedstr:
                        # ? Check, that shared string number is not in null-shared
                        if txt in nullss:
                            return
                    # ? Check, that null value predicate is false for this string
                    elif nullpd(txt):
                        return

                    c_col = col_idx(l_col.encode())
                    if not skip_cols or c_col not in skip_cols:
                        o_q.put(
                            cell(
                                c_row - skip_rows,
                                c_col,
                                c_typ,
                                txt.encode("utf-8"),
                            ),
                        )
                        is_empty_row = False

            def cell_eh(tag: str) -> None:
                nonlocal tags, row, is_empty_row, callback, c_typ, c_row, take_rows, l_tag

                # if tags:
                #     last_opened = tags.pop()
                #     assert (
                #         last_opened == tag
                #     ), f"Invalid closing tag: {tag}. Required {last_opened}."

                # ? Row closing tag
                if tag == "row":
                    # ? Row number for cells increases here only.
                    # * If row was empty and take_non_empty, do not increase row number
                    if not is_empty_row or take_non_empty:
                        row += 1

                    if callback is not None:
                        callback()

                    if is_empty_row:
                        take_rows += take_non_empty

                    # ? If entire row is empty, just increase max row count
                    if take_non_empty:
                        take_rows += is_empty_row
                    # ? Simple way to break in generator, that works in background thread
                    if 0 < take_rows < row:
                        raise GeneratorExit

                elif tag == "c":
                    c_typ = 0
                    l_tag = None

            parser = ParserCreate()
            parser.StartElementHandler = cell_sh
            parser.EndElementHandler = cell_eh
            parser.CharacterDataHandler = cell_th
            parser.buffer_text = True

            try:
                parser.ParseFile(io)
            except ValueError as e:
                if e.args[0] != "read from closed file.":
                    raise
            except AttributeError as e:
                if e.args[0] != "'NoneType' object has no attribute 'tell'":
                    raise
            except GeneratorExit:
                pass
            finally:
                o_q.put(sentinel)

        o_q: Queue[cell] = Queue()
        sentinel = object()

        with self.zf.open(sheet_path) as io:
            Thread(
                target=worker,
                args=(
                    io,
                    self.styles,
                    frozenset(self.null_shared),
                    self.null_values,
                    skip_rows,
                    take_rows,
                    take_rows_non_empty,
                    skip_cols,
                    o_q,
                    sentinel,
                    row_callback,
                ),
            ).start()

            yield from iter(o_q.get, sentinel)
