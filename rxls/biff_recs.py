# ruff: noqa:D101, D102, D105, N801, N815, PLR2004
from __future__ import annotations

__all__ = ["xstr", "BrtBundleSh"]

from io import BytesIO
from typing import TYPE_CHECKING

from .core import as_dataclass, end_decl, struct_p, struct_u, u4_p, u4_u

if TYPE_CHECKING:
    from typing import IO

    from .biff import record
    from .core import Un


@struct_u("<II")
def u44_u(_: bytes) -> tuple[int, int]:
    end_decl()


@struct_p("<II")
def u44_p(*_: Un[tuple[int, int]]) -> bytes:
    end_decl()


@as_dataclass
class xstr:
    s: str | None = ""

    @property
    def value(self) -> str:
        return self.s or ""

    @staticmethod
    def load(io: IO[bytes]) -> xstr:
        (sz,) = u4_u(io.read(4))
        if sz == 0xFFFFFFFF:
            return xstr(None)
        return xstr(io.read(sz * 2).decode("utf-16"))

    def dump(self, io: IO[bytes]) -> int:
        if self.s is None:
            io.write(b"\xff\xff\xff\xff")
        elif self.s:
            io.write(u4_p(len(self.s)))
            io.write(self.s.encode("utf-16"))
        else:
            io.write(b"\x00\x00\x00\x00")

        return io.tell()

    @staticmethod
    def from_binary(b: bytes) -> xstr:
        (sz,) = u4_u(b[:4])
        if sz == 0xFFFFFFFF:
            return xstr(None)
        return xstr(b[4 : 4 + sz * 2].decode("utf-16"))

    @staticmethod
    def from_string(s: str) -> xstr:
        return xstr(s)

    def __repr__(self) -> str:
        return self.s or "<empty-string>"


@as_dataclass
class BrtBundleSh:
    hsState: int = 0
    iTabID: int = 1

    strRelID: xstr = xstr("rId1")
    strName: xstr = xstr("Sheet")

    @staticmethod
    def loadr(rc: record) -> BrtBundleSh:
        with BytesIO(rc.data[8:]) as io:
            return BrtBundleSh(*u44_u(rc.data[:8]), xstr.load(io), xstr.load(io))

    def __repr__(self) -> str:
        return f"BrtBundleSh: {self.strRelID!r} -> {self.strName!r}"
