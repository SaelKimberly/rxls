import re
import typing

from ..core import as_dataclass, cached, u2_p, u2_u
from ..record import record, safe_read
from ..record_enum import BIFF_ENUM
from . import xstr

if typing.TYPE_CHECKING:
    from typing import Literal
    from ..record import RecordProto

__all__ = ["BrtFmt"]


re_dt = re.compile(r"(?<!\\)[dmhysDMHYS]")
re_xt = re.compile(r'(?:"[^"]*")|(?:\[(?!(?:hh?|mm?|ss?)\])[^\]]*\])')

re_date = re.compile(r"[ydYD]")
re_time = re.compile(r"[hsHS]")
re_span = re.compile(r"(?i)\[hh?\](:mm(:ss(\.0*)?)?)?|\[mm?\](:ss(\.0*)?)?|\[ss?\](\.0*)?")

BrtFmt_ID = BIFF_ENUM["BrtFmt"]


@as_dataclass
class BrtFmt:
    ifmt: int = 0
    stFmtCode: xstr = xstr("General")

    @staticmethod
    def loadr_exact(rc: record) -> "BrtFmt":
        (ifmt,) = u2_u(rc.data[:2])
        return BrtFmt(ifmt, xstr.from_binary(rc.data[2:]))

    @staticmethod
    def loadr(rc: record) -> "BrtFmt | None":
        with safe_read():
            return BrtFmt.loadr_exact(rc)

    @staticmethod
    @cached
    def __check_date_format(fmt: str) -> 'Literal["td", "dt", "d", "t", "i", "f", None]':
        f, *_ = fmt.split(";", 1)
        if f == "0":
            return "i"
        if ".00" in f:
            return "f"
        if re_dt.search(f := re_xt.sub("", f)):
            if re_span.search(f):
                return "td"
            if re_time.search(f):
                return "dt" if re_date.search(f) else "t"
            if re_date.search(f):
                return "d"

    def check_datefmt(self) -> 'Literal["td", "dt", "d", "t", "i", "f", None]':
        return BrtFmt.__check_date_format(self.stFmtCode.value)

    def dumpr(self) -> record:
        return record.from_data(BrtFmt_ID, u2_p(self.ifmt), self.stFmtCode)

    def __repr__(self) -> str:
        return f"BrtFmt: <{self.ifmt:02x}> `{self.stFmtCode}`"


if typing.TYPE_CHECKING:
    _: "RecordProto[BrtFmt]" = None
