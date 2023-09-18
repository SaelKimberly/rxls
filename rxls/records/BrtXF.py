from enum import IntEnum, IntFlag
from struct import Struct as st

from ..core import as_dataclass
from ..record import RecordProto, record, safe_read
from ..record_enum import BIFF_ENUM

u4_st_u = st("<I").unpack
u2_st_u = st("<H").unpack
u2_st_p = st("<H").pack

__all__ = ["HAlign", "VAlign", "ReadingOrder", "BrtXF"]

BUILTIN_FORMATS = {
    0x00: "General",
    0x01: "0",
    0x02: "0.00",
    0x03: "#,##0",
    0x04: "#,##0.00",
    0x09: "0%",
    0x0A: "0.00%",
    0x0B: "0.00E+00",
    0x0C: "# ?/?",
    0x0D: "# ??/??",
    # date formats:
    0x0E: "mm-dd-yy",
    0x0F: "d-mmm-yy",
    0x10: "d-mmm",
    0x11: "mmm-yy",
    # time formats:
    0x12: "h:mm AM/PM",
    0x13: "h:mm:ss AM/PM",
    0x14: "h:mm",
    0x15: "h:mm:ss",
    # datetime format
    0x16: "m/d/yy h:mm",
    0x25: "#,##0 ;(#,##0)",
    0x26: "#,##0 ;[Red](#,##0)",
    0x27: "#,##0.00;(#,##0.00)",
    0x28: "#,##0.00;[Red](#,##0.00)",
    # duration formats
    0x2D: "mm:ss",
    0x2E: "[h]:mm:ss",
    0x2F: "mmss.0",
    0x30: "##0.0E+0",
    0x31: "@",
}


class HAlign(IntEnum):
    GENERAL = 0
    LEFT = 1
    CENTER = 2
    RIGHT = 3
    FILL = 4
    JUSTIFY = 5
    CENTER_ACROSS_SELECTION = 6
    DISTRIBUTED = 7


class VAlign(IntEnum):
    TOP = 0
    CENTER = 1
    BOTTOM = 2
    JUSTIFY = 3
    DISTRIBUTED = 4


class ReadingOrder(IntEnum):
    CONTEXT_DEPENDENT = 0
    LEFT_TO_RIGHT = 1
    RIGHT_TO_LEFT = 2


class Protection(IntFlag):
    LOCKED = 0b01
    HIDDEN = 0b10


class CellXF_GrbitAtr(IntFlag):
    AUTO_CHANGE_CFMT = 0b0_0_0_0_0_1
    AUTO_CHANGE_FONT = 0b0_0_0_0_1_0
    AUTO_CHANGE_ALGN = 0b0_0_0_1_0_0
    AUTO_CHANGE_BRDR = 0b0_0_1_0_0_0
    AUTO_CHANGE_FILL = 0b0_1_0_0_0_0
    AUTO_CHANGE_LOCK = 0b1_0_0_0_0_0


class CellStyleXF_GrbitAtr(IntFlag):
    MUST_IGNORE_CFMT = 0b0_0_0_0_0_1
    MUST_IGNORE_FONT = 0b0_0_0_0_1_0
    MUST_IGNORE_ALGN = 0b0_0_0_1_0_0
    MUST_IGNORE_BRDR = 0b0_0_1_0_0_0
    MUST_IGNORE_FILL = 0b0_1_0_0_0_0
    MUST_IGNORE_LOCK = 0b1_0_0_0_0_0


@as_dataclass
class Alignment:
    trot: int = 0x0000
    indent: int = 0x0000

    alc: HAlign = HAlign.GENERAL
    alcv: VAlign = VAlign.BOTTOM

    fWrap: bool = False
    fJustLast: bool = False
    fShrinkToFit: bool = False
    fMergeCell: bool = False

    iReadingOrder: ReadingOrder = ReadingOrder.CONTEXT_DEPENDENT

    def __repr__(self) -> str:
        f_str = lambda n: f" {n}" if self[n] else ""  # type: ignore
        return (
            f"Alignment: {{rot: {self.trot} ind: {self.indent} h_a: {self.alc.name} v_a: {self.alcv.name} r_o: {self.iReadingOrder.name}"
            f' flags: [{f_str("fWrap")}{f_str("fJustLast")}{f_str("fShrinkToFit")}{f_str("fMergeCell")}]}}'
        )


BrtXF_st = st("<HHHHHBBHBB")
BrtXF_st_u = BrtXF_st.unpack
BrtXF_st_p = BrtXF_st.pack

BrtXF_ID = BIFF_ENUM["BrtXF"]


@as_dataclass
class BrtXF:
    ixfeParent: int = 0x0000
    iFmt: int = 0x0000
    iFont: int = 0x0000
    iFill: int = 0x0000
    ixBorder: int = 0x0000

    alignment: Alignment = Alignment()
    protected: Protection = Protection.LOCKED

    fSxButton: bool = False
    f123Prefix: bool = False

    xfGrbitAtr: "CellXF_GrbitAtr | CellStyleXF_GrbitAtr" = CellXF_GrbitAtr(0)

    @staticmethod
    def loadr_exact(rc: record) -> "BrtXF":
        (ixfeParent, iFmt, iFont, iFill, ixBorder, trot, indent, f1, xfGrbitAtr, _) = BrtXF_st_u(rc.data)

        h_align = HAlign(f1 & 0b111)
        v_align = VAlign(f1 >> 3 & 0b111)

        fWrap = bool(f1 & 0b0000_0100_0000)
        fJustLast = bool(f1 & 0b0000_1000_0000)
        fShrinkToFit = bool(f1 & 0b0001_0000_0000)
        fMergeCell = bool(f1 & 0b0010_0000_0000)

        iReadingOrder = ReadingOrder((f1 >> 10) & 0b11)

        return BrtXF(
            ixfeParent,
            iFmt,
            iFont,
            iFill,
            ixBorder,
            Alignment(trot, indent, h_align, v_align, fWrap, fJustLast, fShrinkToFit, fMergeCell, iReadingOrder),
            Protection((f1 >> 12) & 0b11),
            bool(f1 & 0b0100_0000_0000_0000),
            bool(f1 & 0b1000_0000_0000_0000),
            CellStyleXF_GrbitAtr(xfGrbitAtr & 0b111111)
            if ixfeParent == 0xFFFF
            else CellXF_GrbitAtr(xfGrbitAtr & 0b111111),
        )

    @staticmethod
    def loadr(rc: record) -> "BrtXF | None":
        with safe_read():
            return BrtXF.loadr_exact(rc)

    def dumpr(self) -> record:
        almt = self.alignment
        return record(
            BrtXF_ID,
            BrtXF_st_p(
                self.ixfeParent,
                self.iFmt,
                self.iFont,
                self.iFill,
                self.ixBorder,
                almt.trot,
                almt.indent,
                almt.alc.value
                | almt.alcv.value << 3
                | almt.fWrap << 6
                | almt.fJustLast << 7
                | almt.fShrinkToFit << 8
                | almt.fMergeCell << 9
                | almt.iReadingOrder.value << 10
                | self.protected.value << 12
                | self.fSxButton << 14
                | self.f123Prefix << 15,
                self.xfGrbitAtr.value,
                0,
            ),
        )

    def __repr__(self) -> str:
        f_str = lambda n: f" {n}" if self[n] else ""  # type: ignore

        return (
            f"XF {self.ixfeParent:04x}: [{self.iFmt:04x} {self.iFont:04x} {self.iFill:04x} {self.ixBorder:04x}]"
            f" {self.alignment!r}"
            f" {self.protected!r}"
            f' custom flags: {f_str("fSxButton")}{f_str("f123Prefix")}'
            f" xfGrBitAtr: {self.xfGrbitAtr!r}"
        )


_: RecordProto[BrtXF] = None
