from enum import IntFlag
from .core import as_dataclass

__all__ = ["xl_type", "cell"]


class xl_type(IntFlag):
    ERRMPTY = 0b0000
    INTEGER = 0b0001
    FLOATIN = 0b0010
    BOOLEAN = 0b0100
    STRINGS = 0b1000

    DATETIME = 0b0001_0000
    DATETYPE = 0b0010_0000
    TIMETYPE = 0b0100_0000
    DURATION = 0b1000_0000


@as_dataclass(hashable=True)
class cell:
    row: int
    col: int
    typ: xl_type = xl_type.ERRMPTY
    val: "str | None" = None
