import re
import typing
from pathlib import Path
from typing import cast
from zipfile import ZipFile

from tqdm import tqdm

from ..cell import cell, xl_type
from ..core import end_decl, f8_u, struct_u, u4_u, u8_p
from ..record import record
from ..records import BrtBundleSh, BrtFmt, BrtXF
from ..rels import Relationship

if typing.TYPE_CHECKING:
    from typing import IO, Callable, Iterator


__all__ = ["create_scanner"]

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
        "#Н/Д",
    )
)


@struct_u("<II")
def cell_head(_: bytes) -> "tuple[int, int]":
    end_decl()


def scan_sstrs(io: "IO[bytes]") -> "list[str]":
    return [r.data[5 : 5 + u4_u(r.data[1:5])[0] * 2].decode("utf-16") for r in record.scan(io, "BrtSSTItem")]


def scan_csfrs(io: "IO[bytes]") -> "dict[int, xl_type]":
    # sourcery skip: low-code-quality
    ret = {}

    xfs = dict(enumerate(r.iFmt for r in record.scan(io, "BrtXF", cv=BrtXF) if r.ixfeParent != 0xFFFF))
    io.seek(0)
    fms = {r.ifmt: fmt for r in record.scan(io, "BrtFmt", cv=BrtFmt) if (fmt := r.check_datefmt())}

    for xf, fmtid in xfs.items():
        x_fmt: "xl_type | None" = (
            (
                xl_type.DATETIME
                if fmt == "dt"
                else xl_type.DATETYPE
                if fmt == "d"
                else xl_type.TIMETYPE
                if fmt == "t"
                else xl_type.DURATION
                if fmt == "td"
                else xl_type.FLOATIN
                if fmt == "f"
                else xl_type.INTEGER
            )
            if (fmt := fms.get(fmtid))
            else (
                xl_type.INTEGER
                if fmtid == 0x01
                else xl_type.FLOATIN
                if fmtid in (0x02, 0x03, 0x04, 0x25, 0x26, 0x27, 0x28, 0x30)
                else xl_type.DURATION
                if fmtid in (0x2D, 0x2E, 0x2F)
                else xl_type.DATETYPE
                if fmtid in (0x0E, 0x0F)
                else xl_type.TIMETYPE
                if fmtid in (0x12, 0x13, 0x14, 0x15)
                else xl_type.DATETIME
                if fmtid == 0x16
                else None
            )
        )
        if x_fmt is not None:
            ret[xf] = x_fmt

    return ret


def scan_cells(
    io: "IO[bytes]",
    shared_strs: "list[str]",
    classifiers: "dict[int, xl_type] | None" = None,
    *,
    skip_rows: int = 0,
    take_rows: int = -1,
    drop_cels: "str | re.Pattern | None" = None,
    progr_cbk: "Callable[[], None] | None" = None,
    keepempty: bool = False,
) -> "Iterator[cell]":  # sourcery skip: low-code-quality
    if classifiers is None:
        classifiers = {}
    row = 0
    is_empty_row = True
    col = 0

    if drop_cels and isinstance(drop_cels, str):
        drop_cels = re.compile(drop_cels)
    wait_drop = drop_cels is not None

    nrow = -1
    while next(record.scan(io, 0, 402, max_scan=1), None) is not None:
        nrow += 1
        if nrow < skip_rows:
            continue
        if 0 < take_rows < (row + 1):
            break

        is_empty_row = True
        for r in record.scan(io, 2, 4, 5, 6, 7, 8, 9, 10, break_on=[0, 402]):
            col, bxf = cell_head(r.data[:8])
            _t, _v = xl_type.ERRMPTY, None

            if wait_drop:
                c_val = None
                if r.rec_id in (6, 8):
                    (_l,) = u4_u(r.data[8:12])
                    if v := r.data[12 : 12 + _l * 2].decode("utf-16"):
                        c_val = v
                elif r.rec_id == 7:
                    c_val = shared_strs[u4_u(r.data[8:])[0]] or None

                if c_val in ERROR_STRINGS:
                    continue

                if c_val and drop_cels.match(c_val):  # type: ignore
                    yield cell(row, col, xl_type.STRINGS, c_val)
                    is_empty_row = False
                    wait_drop = False
                continue

            # ? BrtCellRk
            if r.rec_id == 2:
                (r_val,) = u4_u(r.data[8:])
                f1 = r_val & 0b11
                xv = r_val >> 2
                if f1 & 0b10:
                    if xv & 0x20000000:
                        xv = -(xv ^ 0x20000000)
                    if f1 & 0b01:
                        xv //= 100
                else:
                    (xv,) = f8_u(u8_p(xv << 34))
                    if f1 & 0b01:
                        xv /= 100
                    xv = round(xv, 6)
                if (cfr := classifiers.get(bxf, xl_type.INTEGER if f1 & 0b10 else xl_type.FLOATIN)) in (
                    xl_type.INTEGER,
                    xl_type.DATETYPE,
                ):
                    xv = int(xv)
                _t, _v = cfr, str(xv)
            # ? BrtCellBool & BrtFmlaBool
            elif r.rec_id in (4, 10):
                _t, _v = xl_type.BOOLEAN, f"{r.data[8]}"
            # ? BrtCellReal
            elif r.rec_id in (5, 9):
                v = round(f8_u(r.data[8:16])[0], 6)
                if (cfr := classifiers.get(bxf, xl_type.FLOATIN)) in (xl_type.INTEGER, xl_type.DATETYPE):
                    v = int(v)
                _t, _v = cfr, str(v)

            # ? BrtCellSt & BrtFmlaString
            elif r.rec_id in (6, 8):
                (_l,) = u4_u(r.data[8:12])
                if (
                    v := r.data[12 : 12 + _l * 2].decode(encoding="utf-16", errors="strict")
                ) and v not in ERROR_STRINGS:
                    _t, _v = xl_type.STRINGS, v
            # ? BrtCellIsst
            elif r.rec_id == 7:
                (idx,) = u4_u(r.data[8:])
                if (v := shared_strs[idx]) and v not in ERROR_STRINGS:
                    _t, _v = xl_type.STRINGS, v

            if _v is not None:
                yield cell(row, col, _t, _v)
                is_empty_row = False

        if keepempty or not is_empty_row:
            row += 1
            is_empty_row = True
            if progr_cbk is not None:
                progr_cbk()
    if row == 0 and progr_cbk is not None:
        progr_cbk()


def create_scanner(
    xl_file: "str | Path | IO[bytes]",
    sheet: "int | str",
    *,
    skip_rows: int = 0,
    take_rows: int = -1,
    drop_cels: "str | None" = None,
    with_tqdm: bool = True,
    book_name: "str | None" = None,
    keep_rows: bool = False,
) -> "Iterator[cell]":  # sourcery skip: low-code-quality
    if book_name is None:
        if isinstance(xl_file, str):
            book_name = Path(xl_file).name.rsplit(".", 1)[0]
        elif issubclass(type(xl_file), Path):
            book_name = cast(Path, xl_file).name.rsplit(".", 1)[0]
        else:
            book_name = "io\\stream\\xlsb"

    with ZipFile(xl_file, "r") as zf:
        files = zf.namelist()
        with zf.open("xl/styles.bin") as io:
            csfrs = scan_csfrs(io)
        if "xl/sharedStrings.bin" in files:
            with zf.open("xl/sharedStrings.bin") as io:
                sstrs = scan_sstrs(io)
        else:
            sstrs = []

        i_sheet_idx: "dict[int, str]" = {}
        s_sheet_idx: "dict[str, str]" = {}

        sheets: "list[str]" = []

        if "xl/_rels/workbook.bin.rels" in files:
            with zf.open("xl/_rels/workbook.bin.rels", "r") as io:
                wb_rels: "list[Relationship]" = Relationship.scan_xml(io)

            with zf.open("xl/workbook.bin") as io:
                for i, _sheet in enumerate(record.scan(io, "BrtBundleSh", cv=BrtBundleSh)):
                    sheet_file = f"xl/{next(x.Target for x in wb_rels if x.Id == _sheet.strRelID.value)}"

                    s_sheet_idx[_sheet.strName.value] = i_sheet_idx[i] = sheet_file

        sheets.extend(s_sheet_idx.keys())

        if isinstance(sheet, int):
            sheet_path = i_sheet_idx[sheet]
            sheet_name = next(name for name, path in s_sheet_idx.items() if path == sheet_path)
        else:
            sheet_path = s_sheet_idx[sheet]
            sheet_name = sheet

        with zf.open(sheet_path) as io:
            tq = None
            if with_tqdm:
                n = book_name
                if len(n) > 33:
                    n = f"{n[:15]}...{n[-min(len(n) - 15, 15):]}"
                if n != "io\\stream\\xlsb":
                    n += ".xlsb"
                tq = tqdm(desc=f"Excel reading: <{n}> [{sheet_name}]", unit=" rows")
                tq.__enter__()
                cbk = tq.update
            else:
                cbk = None
            try:
                yield from scan_cells(
                    io,
                    sstrs,
                    csfrs,
                    skip_rows=skip_rows,
                    take_rows=take_rows,
                    drop_cels=drop_cels,
                    keepempty=keep_rows,
                    progr_cbk=cbk,  # type: ignore
                )
            finally:
                if tq is not None:
                    tq.__exit__(None, None, None)
