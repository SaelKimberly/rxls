# flake8: noqa
from io import BytesIO

from ..record_enum import BIFF_ENUM
from ..record import RecordProto, record, safe_read
from ..core import Un, end_decl, as_dataclass, struct_p, struct_u

from . import xstr


@struct_u("<II")
def u44_u(_: bytes) -> tuple[int, int]:
    end_decl()


@struct_p("<II")
def u44_p(*_: Un[tuple[int, int]]) -> bytes:
    end_decl()


BrtBundleSh_ID = BIFF_ENUM["BrtBundleSh"]

__all__ = ["BrtBundleSh"]


@as_dataclass
class BrtBundleSh:
    hsState: int = 0
    iTabID: int = 1

    strRelID: xstr = xstr("rId1")
    strName: xstr = xstr("Sheet")

    @staticmethod
    def loadr_exact(rc: record) -> "BrtBundleSh":
        with BytesIO(rc.data[8:]) as io:
            return BrtBundleSh(*u44_u(rc.data[:8]), xstr.load_exact(io), xstr.load_exact(io))

    @staticmethod
    def loadr(rc: record) -> "BrtBundleSh | None":
        with safe_read():
            BrtBundleSh.loadr_exact(rc)

    def dumpr(self) -> record:
        return record.from_data(BrtBundleSh_ID, u44_p(self.hsState, self.iTabID), self.strRelID, self.strName)

    def __repr__(self) -> str:
        return f"BrtBundleSh: {self.strRelID!r} -> {self.strName!r}"


_: RecordProto[BrtBundleSh] = None
