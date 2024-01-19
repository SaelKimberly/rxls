# RXLS

Shortcut from Read XLS\[X|B\]

Reading both XLSX and XLSB files, fast and memory-safe, with Python, into PyArrow.

## Description

This module provides one function: `read` for reading both .xlsx and .xlsb files.

### Simple usage showcase:

```python
import polars as pl
import pandas as pd

from rxls.reader import read

polars_df = pl.from_arrow(read(some_file, header=True))
pandas_df = read(some_file, header=True).to_pandas()
```

### Advanced usage examples.

> Some file has no header:

```python
from rxls.reader import read

table = read(some_file, header=0)  # produce `Unnamed: 0` ... `Unnamed: N` columns
table = read(some_file, header=['a', 'b', 'c'])  # produce table with columns `a`, `b`, `c`, and raise an exception, when column count != 3.
```

> Header is placed in unknown row (but we know, it is in top-30 and one cell of it contains word `timestamp`)

```python
from rxls.reader import read

table = read(some_file, header=True, lookup_head='^timestamp$')  # First row of header must contain cell with word `timestamp`
```

> Some rows in column `timestamp` contains unwanted cells (with subscripts, totals, or something else), but column `row_id` doesn't:

```python
from rxls.reader import read

table = read(some_file, header=True, row_filters='^row_id$')  # After `header`, will be only rows, that contains non-null value in column `row_id`
```

> Some rows in column `timestamp` contains unwanted cells, column `row_id` doesn't, but we will ensure, that table has also non-null column `client`:

```python
from rxls.reader import read

table = read(some_file, header=True, row_filters=['^row_id$', '^client$'])  # After `header`, will be only rows, that contains non-null value in columns `row_id` AND `client`
```

> Some rows in a sheet contains unwanted cells, but we know, that `row_id` column cells are not null, or `client` column cells are not null either:

```python
from rxls.reader import read

table = read(some_file, header=True, row_filters=['^row_id$', '^client$'], row_filters_strategy='or')  # After `header`, will be only rows, that contains non-null value in columns `row_id` OR `client`
```

> For tracking rows events:

```python
from rxls.reader import read
from tqdm import tqdm

with tqdm(desc='Reading some file', unit=' rows') as tq:
    table = read(some_file, header=True, row_filters=['^row_id$', '^client$'], row_filters_strategy='or', row_callback=tq.update)
```

## Some algorithm explanations:

RXLS uses only TWO datatypes for cells of excel files: NUMERIC and STRING. It is because all other datatypes, as I discovered, are just `styles`, applied to them (named `representation` in RXLS).

So, when MS Excel show us a date (or time, or timestamp, or duration interval), it's actually a floating-point number "under the hood". This number, when is between [0.0, 1.0), presents `part of day` (0.5 = 12 hours, 0.125 = 3 hours and vice versa), which useful for `time` and `duration` datatypes, and, when bigger or equals to `1`, presents `days since 1900 with part of day`. For `date` - decimals will be zero. All temporal cells (date/time, datetime, timedelta) will be converted to `timestamp[ms]` during `prepare` step. This behaviour may be overriden with `dtypes` argument - after all, pyarrow's `timestamp[ms]` datatype may be convert to `date64` or `time32['ms']` instantly and without any problems. `duration` datatype in Excel files, usually, not presented, and pyarrow cannot cast `timestamp['ms']` to `duration['ms']`. For such conversion, you can extract timestamp column and convert it as:

```python
import pyarrow as pa

column_name = 'duration_column'

table = read(some_file, header=True)

table.set_column(
    next(i for i, x in enumerate(table.column_names) if x == column_name),
    pa.field(column_name, pa.duration('ms')),
    table[column_name].combine_chunks().view(pa.duration('ms'))  # original dtype is pyarrow.timestamp['ms'], which is binary equivalent for pyarrow.duration['ms']
)
```

Strings most common representation is 4-byte little-endian integer (in `xlsb`) or some number (in `xlsx`). This number is an index of `shared` string in special part of Excel file (`xl/sharedStrings.[xml|bin]`). This special part will be scanned **on each `read` call**, before all other steps.

Keeping all this in mind, I've implemented class `chunk` for keeping original data `as-is`, until it should be accessed during final table creation. Chunks, actually, are column parts, and contains non-null values with same type and representation. Chunk may be of `NULL` type, but it contains just `count` of null values between not-null chunks.

Original data will be contained inside `chunk` as-is, intact until `prepare` step:

- `numpy.NDArray[numpy.float64]` - simple numeric values (with optional `temporal` flag)
- `numpy.NDArray[numpy.uint32]` - for `XLSB` RkNumber-formatted floating-poing or integer values.
- `pyarrow.LargeStringArray` - for `XLSX` and `XLSB` strings, that are not in `shared`, but presented as-is.
- `pyarrow.UInt64Array` - for `XLSX` and `XLSB` shared strings indices.
- `int` - for `null` chunks: count of null values

RXLS can skip entire chunks (or their parts) before `prepare` step, when they should be skipped (see `lookup_head` and `row_filters` arguments descriptions) - they will never be accessed afterwards.

RXLS performs two main steps for a sheet of Excel file: `reading` and `prepare`.

On `reading` step, no computations will be performed - it's just filling `xl_series` objects by `chunks`, keeping original data.

On `prepare` step, unwanted chunks will be dropped, and others will be `prepared` as:

- `numpy.NDArray[numpy.uint32]` -> `numpy.NDArray[numpy.float64]` - when numeric chunk has `rknubmer` flag.
- `numpy.NDArray[numpy.float64]` -> `numpy.NDArray[numpy.int64]` - when numeric chunk has `temporal` flag. *count of days with decimal part since 1900 (Windows)* -> *milliseconds since 1970-01-01 (UNIX timestamp)*
- `pyarrow.UInt64Array` -> `pyarrow.LargeStringArray` - shared strings indices to actual data from `shared`.
- `int` -> `pyarrow.NullArray` or `pyarrow.Array[T, pyarrow.Scalar[T]]` if column has chunks of another common datatype `T`.

These operations were totally optimized (and may performs much faster with `numba` along with (or without) `tbb` - see `Dependencies` section).

Also, about `lookup_head` and `header` with height > 1.

If header has more than 1 row, every column will fill it's topmost empty cells by use corresponding non-empty cell values from left neighbour. All other cells will be leave as is. After this, non-empty cells will be concatenated from top to down with `', '` separator. These operations performs for each column from left to right.

So, RXLS features are:

- Lazy and fast column-wise computations
- Smart header searching and row filtering
- Keeping datatypes and resolve conflicts.

## Notes:

- At the moment, RXLS just skip empty column (which has no header, nor any data).

## Parameters:

- Positional **only**:
  - **file**: path to the file, or BytesIO with file.
  - **sheet**: index or name of sheet for reading (default: `0`)
- Positional:
  - **header**:
    - **`bool`**: `True` when header is presented in the sheet and have one row height
    - **`int`**: `0` when no header is presented in the sheet, `N` for header of `N` rows height.
    - **`list[str]`**: when no header is presented, but we are know, what it should be. 
- Keyword-only
  - **dtypes**: Specify datatypes for columns.
    - **`Sequence[pyarrow.DataType]`**: when we know dtype for each non-empty column.
    - **`dict[str, pyarrow.DataType]`**: when we will override dtype for some columns.
    - **`pyarrow.DataType`**: when all columns must be of this dtype.
    - **`None`**: for original dtypes of columns.
  - **skip_cols**: Skip some columns by their `0`-based indices (A == 0). Performs on `reading` step.
  - **skip_rows**: Skip some rows on the top of the file. Performs on `reading` step.
  - **skip_rows_after_header**: Skip some rows after `header`. Performs on `prepare` step.
  - **take_rows**: Stop reading after this row (`0`-based). Performs on `reading` step.
  - **take_rows_non_empty**: Leave empty rows in resulting table. Performs on `reading` step.
  - **lookup_head**: Regular expression for smart-search of the first row of `header`, or `column` index, where first non-empty cell is the top-level cell of `header`.
  - **lookup_size**: Count of rows to perform lookup when searching for `header`. Note: RXLS will raise an exception, if `lookup_head` with this `lookup_size` is failed.
  - **row_filters**: Regular expression(s) for columns, which content determines empty and non-empty rows.
  - **row_filters_strategy**: Boolean operator(s) for situations with two or more columns in `row_filters`.
  - **float_precision**. All numeric values in MS Excel are `floating-point` under the hood, so, when rounding whole `float64` column to this precision gives equal result to just truncate decimals, this column will be converted to `int64`.
  - **datetime_formats**: One or more formats, which may appears in columns with `conflicts`.
  - **conflict_resolve**: When column contains two or more datatypes, this is a `conflict`. When conflict cannot be resolved, whole column will be convert to `utf-8`. Conflicts may be resolved as:
    - **`no`**: All parts in columns with `conflicts` will be convert to `utf8`.
    - **`temporal`**: Try to convert non-temporal parts of column with some temporal parts to temporal (`float64` -> `timestamp` and `utf8` -> `timestamp` (using default formats (`ISO 8601`), or as specified in `datetime_formats`)
    - **`numeric`**: Try to convert non-numeric parts of column with some numeric parts to numeric (`utf8` -> `float64`).
    - **`all`**: Use both strategies to resolve conflicts. When some parts of column is temporal, try to convert all other parts to temporal (also enable two-step string converting: `utf8` -> `float64` -> `timestamp`)
  - **utf8_type_infers**: `(WIP)` When resulting column is `utf-8` and all non-null cells of it passes regular expression of `numeric` values, convert it to `float64` (and, maybe, to `int64` after).
  - **null_values**: Advanced list of values, that should be skipped on `reading` step (or `callable` predicate for them).
  - **row_callback**: Any callable, which may be called without arguments on each row event. Useful for progress tracking.

## Dependencies

### Required:

- **pyarrow**>=`14.0.2`
- **numpy**>=`1.24.4`
- **recordclass**>=`0.21.1`
- **typing_extensions**>=`4.9.0`

### Optional:

- **numba**>=`0.58.1` (increase import time, but reading speed also increases x3/x4 and up)
- **tbb**>=`2021.11.0` (only for numba - additional performance gain)
- **polars**>=`0.20.4` (if needs to parse timestamps with milliseconds/microseconds/nanoseconds or AM/PM with timezone)
- **pandas**>=`2.0.3` (for pyarrow `to_pandas` functionality)
- **tqdm**>=`4.66.1` (fast progress tracking)

