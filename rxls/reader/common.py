from __future__ import annotations

__all__ = ["read"]

import re
from functools import reduce
from typing import TYPE_CHECKING
from zipfile import ZipFile

import pyarrow as pa

from ..chunk import arrow as pc
from ..series import xl_series
from .xlsb import XlsbWorkbook
from .xlsx import XlsxWorkbook

if TYPE_CHECKING:
    from pathlib import Path
    from typing import IO, Callable, Literal, Sequence


def __make_header(
    columns: dict[int, xl_series],
    first_row: int,
    length: int,
) -> list[str]:
    if length == 1:
        hdrs = [
            x if (x := columns[col_idx].to_utf8(first_row, 1)[0].as_py()) else "Unnamed"
            for col_idx in sorted(columns)
        ]
    else:
        raw_hdrs = [
            dict(enumerate(columns[col_idx].to_utf8(first_row, length).to_pylist()))
            for col_idx in sorted(columns)
        ]

        prev_hdr = {}

        hdrs = []

        for parts in raw_hdrs:
            if (
                top_index := next((i for i, x in parts.items() if x is not None), -1)
            ) >= 0:
                if prev_hdr:
                    parts.update({i: x for i, x in prev_hdr.items() if i < top_index})

                prev_hdr = parts

                hdrs.append(
                    ", ".join(h for x in sorted(parts) if (h := parts[x])) or "Unnamed",
                )
            else:
                hdrs.append("Unnamed")

    result_hdrs = []

    for hdr in hdrs:
        n = hdr
        i = 1
        while n in result_hdrs:
            n = f"{hdr}.{i}"
            i += 1

        result_hdrs.append(n)

    return hdrs


def __find_header(
    columns: dict[int, xl_series],
    pattern: str | int,
    min_row: int = 0,
    max_head_length: int = 30,
    *,
    ignore_case: bool = False,
) -> int:
    if isinstance(pattern, int):
        chunk = columns[pattern].to_utf8(min_row, max_head_length).is_null()
        if chunk.true_count < len(chunk):
            return chunk.to_pylist().index(False)
        return -1

    for col in columns.values():
        chunk = pc.match(
            col.to_utf8(min_row, max_head_length),
            pattern,
            ignore_case=ignore_case,
        )
        if not chunk.sum().as_py():
            continue

        return chunk.to_pylist().index(True)
    return -1


def read(  # noqa: C901, PLR0914, PLR0912, PLR0915
    # ? File name/path/binary stream
    file: str | Path | IO[bytes],
    # ? Sheet number/name
    sheet: int | str = 0,
    /,
    # ? Head indicator (or length (or values))
    header: int | bool | list[str] = 0,  # noqa: FBT001
    *,
    dtypes: Sequence[pa.DataType] | dict[str, pa.DataType] | pa.DataType | None = None,
    # ? Column manipulation kwargs
    skip_cols: list[int] | None = None,
    # ? Row manipulation kwargs
    skip_rows: int = 0,
    # ? If header is not integer greater than 0, this parameter will be ignored
    skip_rows_after_header: int = 0,
    # ? Take rows [after header[ and skip_rows_after_header]]
    take_rows: int = 0,
    take_rows_non_empty: bool = False,
    # ? For table head intellectual searching
    lookup_head: str | int | None = None,
    lookup_size: int = 30,
    # ? For rows intellectual filtering
    row_filters: str | list[str] | None = None,
    # ? Two or more filters may be concatenated using these binary operators:
    row_filters_strategy: Literal["or", "and"] | Sequence[Literal["or", "and"]] = "or",
    # ? For float to int casting when round by `float_precision` digits gives equal result to round(0)
    float_precision: int = 6,
    # ? For string temporal parsing (when in one column with temporal values)
    datetime_formats: tuple[str, ...] | str | None = None,
    # ? For float to temporal casting (when in one column with temporal values)
    conflict_resolve: Literal["no", "temporal", "numeric", "all"] = "no",
    # ? Detect, when utf-8 column actually contains float or int values
    utf8_type_infers: bool = True,
    # ? Additional null values for string dtype:
    null_values: Sequence[str] | Callable[[str], bool] | None = None,
    # ? Callback for row end event
    row_callback: Callable[[], None] | None = None,
) -> pa.Table:
    """Reader for {} file format"""
    # ! [0]: Testing input data, whether it XLSX or XLSB file:
    with ZipFile(file) as zf:
        content = zf.namelist()

        Reader: type[XlsxWorkbook]  # noqa: N806
        if "xl/workbook.xml" in content:
            Reader = XlsxWorkbook  # noqa: N806
            binary = False
        elif "xl/workbook.bin" in content:
            Reader = XlsbWorkbook  # type: ignore  # noqa: N806
            binary = True
        else:
            msg = "Invalid file content: workbook file not found!"
            raise AssertionError(msg)

    if isinstance(datetime_formats, str):
        datetime_formats = (datetime_formats,)

    _columns = {}

    with Reader(file, null_values=null_values) as _reader:
        for _cell in _reader.iter_worksheet(
            sheet,
            skip_rows=skip_rows,
            take_rows=take_rows,
            take_rows_non_empty=take_rows_non_empty,
            skip_cols=skip_cols,
            row_callback=row_callback,
        ):
            if _cell.col not in _columns:
                _columns[_cell.col] = xl_series(
                    _reader.shared,
                    binary,
                    conflict_resolve=conflict_resolve,
                    datetime_formats=datetime_formats,
                    float_precision=float_precision,
                )

            _columns[_cell.col].add(_cell)

    # max_col = max(_columns)
    # min_col = min(_columns)

    # columns = {i: _columns.get(i, []) for i in range(min_col, max_col + 1)}
    columns = _columns

    max_len = max(len(x) for x in columns.values())

    first_row = 0

    if lookup_head is not None:
        first_row = __find_header(columns, lookup_head, 0, lookup_size)
        if first_row == -1:
            msg = f"Cannot find header by given pattern: {lookup_head}"
            raise ValueError(msg)
        max_len -= first_row

    header_height = 0
    if header != 0:
        if isinstance(header, (int, bool)):
            header_height = int(header)
            header = __make_header(columns, first_row, header)
        else:  # noqa: PLR5501
            if len(header) != len(columns):
                msg = f"{len(columns)} headers must be passed. {len(header)} found"
                raise ValueError(
                    msg,
                )
    else:
        header = [f"Unnamed. {i}" for i, _ in enumerate(columns)]

    filters = []
    if row_filters:
        f_err = False
        if isinstance(row_filters, str):
            filters = [
                columns[x]
                for name, x in zip(header, sorted(columns))
                if re.match(row_filters, name)
            ]
        elif isinstance(row_filters, list):
            if all(isinstance(x, int) for x in row_filters):
                filters = [
                    columns[x]
                    for i, x in enumerate(sorted(columns))
                    if i in row_filters
                ]
            elif all(isinstance(x, str) for x in row_filters):
                filters = [
                    columns[x]
                    for name, x in zip(header, sorted(columns))
                    if any(bool(re.match(x, name)) for x in row_filters)
                ]
            else:
                f_err = True
        else:
            f_err = True

        if f_err:
            msg = f"Inappropriate filter: {row_filters!r}"
            raise ValueError(msg)

    g_offset = header_height + first_row + skip_rows_after_header
    g_length = max_len - header_height - skip_rows_after_header

    idx = None
    if filters:
        boolean_filters = [
            pc.invert(x.to_arrow(g_offset, g_length).is_null(nan_is_null=True))
            for x in filters
        ]
        if len(boolean_filters) == 1:
            idx = boolean_filters[0].cast("bool")

        elif row_filters_strategy == "or":
            idx = reduce(
                pc.or_,
                boolean_filters,
            ).cast("bool")
        elif row_filters_strategy == "and":
            idx = reduce(
                pc.and_,
                boolean_filters,
            ).cast("bool")
        elif isinstance(row_filters_strategy, (list, tuple)):
            if len(row_filters_strategy) != (len(boolean_filters) - 1):
                msg = f"Count of operators in `row_filters_strategy` argument must be {(len(boolean_filters) - 1)}, not {len(row_filters_strategy)}"
                raise ValueError(
                    msg,
                )
            for i, _unsupported in (
                (i, x)
                for i, x in enumerate(row_filters_strategy)
                if not isinstance(x, str) or x not in {"and", "or"}
            ):
                msg = f"Unsupported operator in position {i} of `row_filters_strategy` argument: {_unsupported} (supported operators: `and`, `or`)."
                raise ValueError(
                    msg,
                )
            i_boolean_filters = iter(boolean_filters)
            idx = next(i_boolean_filters)

            for op, boolean_filter in zip(row_filters_strategy, i_boolean_filters):
                if op == "and":
                    idx = pc.and_(idx, boolean_filter)
                else:
                    idx = pc.or_(idx, boolean_filter)
        else:
            msg = f"`row_filters_strategy` must be either `and`, `or` or list or tuple of these (got: {type(row_filters_strategy)!r})"
            raise TypeError(
                msg,
            )

        _ = idx.false_count

        if not idx.true_count:
            msg = "Maybe inappropriate filter column: result is empty"
            raise ValueError(msg)

    arrays = [columns[x].to_arrow(g_offset, g_length, idx=idx) for x in sorted(columns)]

    if dtypes is not None:  # noqa: PLR1702
        if isinstance(dtypes, pa.DataType):
            for i in range(len(arrays)):
                if arrays[i].type.id != dtypes.id:  # type: ignore
                    try:
                        arrays[i] = arrays[i].cast(dtypes)
                    except Exception:  # noqa: BLE001
                        msg = f"Column {header[i]} cannot be cast to dtype {dtypes!r}"
                        raise ValueError(
                            msg,
                        ) from None
        elif isinstance(dtypes, dict):
            for i, col in enumerate(header):
                if col in dtypes:
                    dtype = dtypes[col]
                    if arrays[i].type.id != dtype.id:
                        try:
                            arrays[i] = arrays[i].cast(dtype)
                        except Exception:  # noqa: BLE001
                            msg = f"Column {col} cannot be cast to dtype {dtype!r}"
                            raise ValueError(
                                msg,
                            ) from None
        else:
            if len(dtypes) != len(header):
                msg = f"Collection of dtypes has mismatched length with column count: {len(dtypes)} instead of {len(header)}"
                raise ValueError(
                    msg,
                )

            for i, dtype in enumerate(dtypes):
                if arrays[i].type.id != dtype.id:
                    try:
                        arrays[i] = arrays[i].cast(dtype)
                    except Exception:  # noqa: BLE001
                        msg = f"Column {i} ({header[i]!r}) cannot be cast to dtype {dtype!r}"
                        raise ValueError(
                            msg,
                        ) from None

    return pa.table(arrays, header)
