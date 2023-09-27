# rxls

Shortcut from Read XLS\[X|B\]

Reading both XLSX and XLSB files, fast and memory-safe, with Python, into Polars or Pandas.

# Description

This module provides one function: `xl_scan` for reading both .xlsx and .xlsb files.

# Parameters:

- Positional:
  - **xl_file**: path to the file, or BytesIO with file.
  - **sheet**: index or name of sheet for reading (default: `0`)
- Keyword-only
  - **mode**: `pd` or `pl` (default: `pl`, polars). What do we need: Pandas dataframe or Polars DataFrame
  - **head**: `int` or `list[str]` (default: `0`). Do you want to read `head` rows as multiline header, or want to override column names with your own list.
  - **skip_rows**: `int` (default: `0`). Skip some rows in top of the file
  - **drop_rows**: `int` (default: `0`). Drop some rows after header (or after skipped rows, if `head == 0` or `head` manually provided)
  - **take_rows**: `int` (default: `-1`, means infinity). Max amount of rows to read
  - **drop_cels**: `str` or `None`. Pattern for *the first cell of sheet* - cell, that matches this, will be first non-empty cell, and it's row also will be first. All cells before will be dropped, and all rows before will be skipped.
  - **with_tqdm**: bool (default: `True`). Show progress of reading (only current rows)
  - **book_name**: `str` or `None` (default: `None`). Override Excel workbook name in tqdm output.
  - **index_col**: `str` or `None` (default: `None`). Column, that defines, whether row must exist, or not. Allows for smart skpping rows, that not contains useful data.
  - **inferring**: `no`, `basic`, `strict` or `extended` (default: `basic`)
    - `no`. All columns will not be converted. Returns Utf-8 DataFrame.
    - `basic` (default). Convert data in column, based on most used cell type. Data can disappear from colums with mixed types.
    - `strict`. Convert data in column, based on most used cell type. If some data missed after converting, reverse changes and return Utf-8 column.
    - `extended`. Same as `basic`, but additionally tests remaining `utf-8` columns with regular expressions and convert them, on success, to the according type.
  - **frounding**: `int` or `None` (default: None). Round floating-point cells to the given precision.
  - **keep_rows**: `bool` (default: False). Keep empty rows in resulting dataframe. Ignored, when use along with `index_col`

# Dependencies:

`python>=3.8`

- pyarrow>=13.0.0
- polars>=0.19.3
- pandas>=2.0.3
- numpy>=1.24.4
- numba>=0.58.0
- recordclass>=0.20
- tqdm>=4.66.1
- typing-extensions>=4.8.0
