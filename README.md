# RXLS

Shortcut from Read XLS\[X|B\]

Reading both XLSX and XLSB files, fast and memory-safe, with Python, into PyArrow.

## Description

This module provides one function: `read` for reading both .xlsx and .xlsb files.

```python
import polars as pl
import pandas as pd

from rxls.reader import read

polars_df = pl.from_arrow(read(some_file, header=True))
pandas_df = read(some_file, header=True).to_pandas()
```

## Parameters:

- Positional:
  - **file**: path to the file, or BytesIO with file.
  - **sheet**: index or name of sheet for reading (default: `0`)
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

