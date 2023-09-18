import re
from pathlib import Path
from pyexpat import ParserCreate
from typing import IO, Callable, Iterator, Literal, TypeVar, cast
from queue import SimpleQueue as Queue
from threading import Thread
from collections import deque
from zipfile import ZipFile

from tqdm import tqdm

from ..records import BrtFmt, xstr
from ..cell import cell, xl_type


_T = TypeVar("_T")

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

__all__ = ["create_scanner"]


def col_idx(col: bytes) -> int:
    a = -1
    i = 0
    for x in col[::-1]:
        if x > 64:
            a += (x - 64) * (26**i)
            i += 1
    return a


try:
    import numba as nb

    col_idx = nb.njit(nb.types.int32(nb.types.Bytes(nb.u1, 1, "C", True)))(col_idx)  # type: ignore

except ImportError:
    pass


def scan_sstrs(io: IO[bytes]) -> list[str]:
    ret: list[str] = []

    must_be = False

    def cell_sh(tag: str, _: dict[str, str]):
        nonlocal must_be
        if tag == "si":
            must_be = True

    def cell_th(txt: str):
        nonlocal must_be, ret
        if must_be:
            ret.append(txt)
            must_be = False

    def cell_eh(tag: str):
        nonlocal must_be
        if tag == "si":
            if must_be:
                ret.append(None)  # type: ignore
                must_be = False

    parser = ParserCreate()
    parser.StartElementHandler = cell_sh
    parser.EndElementHandler = cell_eh
    parser.CharacterDataHandler = cell_th
    parser.buffer_text = True

    parser.ParseFile(io)
    return ret


def scan_csfrs(io: IO[bytes]) -> dict[int, xl_type]:
    ret = {}

    fms: dict[int, Literal["dt", "d", "t", "td", "i", "f"]] = {}
    xf = 0

    def cell_sh(tag: str, attrs: dict[str, str]):
        nonlocal xf, ret, fms
        if tag == "numFmt":
            xfmt = BrtFmt(int(attrs["numFmtId"]), xstr(attrs["formatCode"]))
            if fmt := xfmt.check_datefmt():
                fms[xfmt.ifmt] = fmt

        if tag == "xf" and "xfId" in attrs:
            fmtid = int(attrs["numFmtId"])
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
            xf += 1

    parser = ParserCreate()
    parser.StartElementHandler = cell_sh
    parser.buffer_text = True

    parser.ParseFile(io)
    return ret


def scan_cells(
    io: IO[bytes],
    shared_strs: list[str],
    classifiers: "dict[int, xl_type] | None" = None,
    *,
    skip_rows: int = 0,
    take_rows: int = -1,
    drop_cels: "str | re.Pattern | None" = None,
    progr_cbk: "Callable[[], None] | None" = None,
    keepempty: bool = False,
) -> Iterator[cell]:
    o_q: Queue[cell] = Queue()
    sentinel = object()

    def worker(
        io: IO[bytes],
        sstrs: list[str],
        csfrs: dict[int, xl_type],
        o_q: "Queue[cell | _T]",
        sentinel: _T,
        progr_cbk: "Callable[[], None] | None",
    ):
        nonlocal drop_cels, skip_rows, take_rows

        tags: deque[str] = deque()
        nrow = 0
        is_empty_row: bool = True
        is_sharedstr: bool = False
        rix = 0
        if wait_drop := drop_cels is not None:
            if isinstance(drop_cels, str):
                drop_cels = re.compile(drop_cels)
        # cell fields
        c_row = 0
        c_typ = xl_type.ERRMPTY
        c_val = None

        l_col: str = ""

        l_tag: "str | None" = None

        if progr_cbk is not None:
            progr_cbk()

        def cell_sh(tag: str, attrs: dict[str, str]):
            nonlocal tags, l_col, c_typ, c_row, nrow, is_sharedstr, csfrs, rix, skip_rows, l_tag
            if not tags:
                if tag == "c" and rix >= skip_rows:
                    # ? get column number (not zero-based, but ascending!)
                    l_col = attrs["r"]

                    # ? check, if value is shared string index:
                    is_sharedstr = attrs.get("t") == "s"
                    # ? check, if value has been date and/or time and/or delta decoded
                    s = int(attrs.get("s", "0"))
                    c_typ = csfrs.get(s, xl_type.STRINGS)
                    c_row = nrow
                else:
                    return
            tags.append(l_tag := tag)

        def cell_th(txt: str):
            nonlocal o_q, c_val, l_col, c_typ, l_tag, is_empty_row, is_sharedstr, sstrs, rix, skip_rows, wait_drop, drop_cels
            if l_tag == "v" and (txt := txt.strip()) and (rix >= skip_rows):
                if is_sharedstr:
                    try:
                        txt = sstrs[int(txt)]
                    except ValueError:
                        return
                    if not txt:
                        return

                if txt in ERROR_STRINGS:
                    return

                if wait_drop:
                    c_val = txt if is_sharedstr or c_typ is xl_type.STRINGS else None
                    if c_val and bool(drop_cels.match(c_val)):  # type: ignore
                        wait_drop = False
                        is_empty_row = False
                        o_q.put(cell(c_row, col_idx(l_col.encode()), c_typ, c_val))

                    return

                is_empty_row = False
                try:
                    c_val = f"{float(txt):.0f}" if c_typ in (xl_type.INTEGER, xl_type.DATETYPE) else txt
                except ValueError:
                    c_val = txt

                o_q.put(cell(c_row, col_idx(l_col.encode()), c_typ, c_val))

        def cell_eh(tag: str):
            nonlocal tags, nrow, is_empty_row, rix, progr_cbk, c_val, c_typ, c_row, take_rows
            if tags:
                assert (last_opened := tags.pop()) == tag, f"Invalid closing tag: {tag}. Required {last_opened}."
            # if tag == "c":
            #     c_val, c_typ, c_col, c_row = None, xl_type.STRINGS, 0, 0

            if tag == "row":
                # ?: </row> closing tag at the end of cell sequence
                # * If no data cells found between this and previous </row> tag,
                # * nrow stays zero
                if keepempty or not is_empty_row:
                    nrow += 1
                    if 0 < take_rows <= nrow:
                        raise GeneratorExit()
                    if progr_cbk is not None:
                        progr_cbk()
                rix += 1
                is_empty_row = True

        parser = ParserCreate()
        parser.StartElementHandler = cell_sh
        parser.EndElementHandler = cell_eh
        parser.CharacterDataHandler = cell_th
        parser.buffer_text = True

        try:
            parser.ParseFile(io)
        except ValueError as e:
            if e.args[0] != "read from closed file.":
                raise e
        except AttributeError as e:
            if e.args[0] != "'NoneType' object has no attribute 'tell'":
                raise e
        except GeneratorExit:
            pass
        finally:
            o_q.put(sentinel)

    Thread(target=worker, args=(io, shared_strs, classifiers, o_q, sentinel, progr_cbk)).start()

    yield from iter(o_q.get, sentinel)


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
) -> Iterator[cell]:
    if book_name is None:
        if isinstance(xl_file, str):
            book_name = Path(xl_file).name.rsplit(".", 1)[0]
        elif issubclass(type(xl_file), Path):
            book_name = cast(Path, xl_file).name.rsplit(".", 1)[0]
        else:
            book_name = "io\\stream\\xlsx"

    with ZipFile(xl_file, "r") as zf:
        files = zf.namelist()

        with zf.open("xl/styles.xml") as io:
            csfrs = scan_csfrs(io)
        if "xl/sharedStrings.xml" in files:
            with zf.open("xl/sharedStrings.xml") as io:
                sstrs = scan_sstrs(io)
        else:
            sstrs = []

        i_sheet_idx: dict[int, str] = {}
        s_sheet_idx: dict[str, str] = {}

        sheets: list[str] = []

        rId_to_file = {}
        with zf.open("xl/_rels/workbook.xml.rels") as io:

            def rs_handler(tag: str, attr: dict[str, str]):
                nonlocal rId_to_file
                if tag == "Relationship" and attr["Type"].endswith("/worksheet"):
                    rId_to_file[attr["Id"]] = "xl/" + attr["Target"]

            parser = ParserCreate()
            parser.StartElementHandler = rs_handler
            parser.ParseFile(io)

        rId_to_name = {}
        with zf.open("xl/workbook.xml") as io:

            def st_handler(tag: str, attr: dict[str, str]):
                nonlocal rId_to_name
                if tag == "sheet":
                    rId_to_name[attr["r:id"]] = attr["name"]

            parser = ParserCreate()
            parser.StartElementHandler = st_handler
            parser.ParseFile(io)

        i_sheet_idx = {i: _path for i, _path in enumerate(rId_to_file[x] for x in rId_to_name)}
        s_sheet_idx = {name: path for name, path in ((rId_to_name[x], rId_to_file[x]) for x in rId_to_name)}

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
                if n != "io\\stream\\xlsx":
                    n += ".xlsx"
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
                    take_rows=take_rows,
                    skip_rows=skip_rows,
                    drop_cels=drop_cels,
                    keepempty=keep_rows,
                    progr_cbk=cbk,
                )
            finally:
                if tq is not None:
                    tq.__exit__(None, None, None)
