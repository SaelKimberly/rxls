from typing import IO
from struct import Struct as st, error as st_err

from ..record import SampleProto
from ..core import as_dataclass

u4_st_u = st("<I").unpack
u4_st_p = st("<I").pack

__all__ = ["xstr"]


@as_dataclass
class xstr:
    s: "str | None" = ""

    @property
    def value(self) -> str:
        return self.s or ""

    @staticmethod
    def load_exact(io: IO[bytes]) -> "xstr":
        (sz,) = u4_st_u(io.read(4))
        if sz == 0xFFFFFFFF:
            return xstr(None)
        return xstr(io.read(sz * 2).decode("utf-16"))

    @staticmethod
    def load(io: IO[bytes]) -> "xstr | None":
        try:
            return xstr.load_exact(io)
        except (IndexError, UnicodeDecodeError, st_err):
            pass

    def dump(self, io: IO[bytes]) -> int:
        if self.s is None:
            io.write(b"\xff\xff\xff\xff")
        elif self.s:
            io.write(u4_st_p(len(self.s)))
            io.write(self.s.encode("utf-16"))
        else:
            io.write(b"\x00\x00\x00\x00")

        return io.tell()

    @staticmethod
    def from_binary(b: bytes) -> "xstr":
        (sz,) = u4_st_u(b[:4])
        if sz == 0xFFFFFFFF:
            return xstr(None)
        return xstr(b[4 : 4 + sz * 2].decode("utf-16"))

    @staticmethod
    def from_string(s: str) -> "xstr":
        return xstr(s)

    def __repr__(self) -> str:
        return self.s or "<empty-string>"


_: SampleProto[xstr] = None
