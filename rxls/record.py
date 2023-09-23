import os
import typing
from contextlib import contextmanager
from io import BytesIO
from struct import Struct as st
from struct import error as st_err

from .core import as_dataclass, u1_p, u2_p
from .record_enum import BIFF_ENUM, BIFF_ENUM_REVERSED

if typing.TYPE_CHECKING:
    from typing import (
        IO,
        Any,
        Callable,
        Iterable,
        Iterator,
        Protocol,
        Type,
        TypeVar,
        Union,
        overload,
    )

    class record_proto(Protocol):
        def __repr__(self) -> str:
            raise NotImplementedError()

        def dumpr(self) -> "record":
            raise NotImplementedError()

        @staticmethod
        def loadr(rc: "record") -> "record_proto | None":
            raise NotImplementedError()

        @staticmethod
        def loadr_exact(rc: "record") -> "record_proto":
            raise NotImplementedError()

    RecordProtoGeneric = TypeVar("RecordProtoGeneric", bound=record_proto)

    RecordProto = Union[RecordProtoGeneric, None]

    class sample_proto(Protocol):
        def __repr__(self) -> str:
            raise NotImplementedError()

        def dump(self, io: "IO[bytes]") -> int:
            raise NotImplementedError()

        @staticmethod
        def load(io: "IO[bytes]") -> "sample_proto | None":
            raise NotImplementedError()

    SampleProtoGeneric = TypeVar("SampleProtoGeneric", bound=sample_proto)

    SampleProto = Union[SampleProtoGeneric, None]

    __all__ = ["record", "RecordProto", "SampleProto", "dump_sz", "safe_read"]

else:
    __all__ = ["record", "dump_sz", "safe_read"]

u2_st_p = st("<H").pack
u1_st_p = st("<B").pack
u1_st_u = st("<B").unpack

b1_st_p = st("<B").pack
b1_st_u = st("<B").unpack
b2_st_p = st("<BB").pack
b3_st_p = st("<BBB").pack
b4_st_p = st("<BBBB").pack

ST_CACHE: "dict[str, tuple[int, Callable[[bytes], tuple[Any, ...]]]]" = {}


@contextmanager
def safe_read(io: "IO[bytes] | None" = None) -> "Iterator[None]":
    ix = io.tell() if io is not None else 0
    try:
        yield
    except (st_err, IndexError, UnicodeDecodeError):
        if io is not None:
            io.seek(ix)


def dump_sz(i: int) -> bytes:
    # 0.2 - 0.8 us
    # assert i < 0x10000000
    return (
        b1_st_p(i)
        if i < 0x80
        else b2_st_p(i & 0x7F | 0x80, i >> 7)
        if i < 0x4000
        else b3_st_p(i & 0x7F | 0x80, (i >> 7) & 0x7F | 0x80, i >> 14)
        if i < 0x200000
        else b4_st_p(i & 0x7F | 0x80, (i >> 7) & 0x7F | 0x80, (i >> 14) & 0x7F | 0x80, i >> 21)
    )


@as_dataclass
class record:
    rec_id: int = 0
    data: bytes = b""

    @property
    def is_empty(self) -> bool:
        return not self.data

    @property
    def rec_sz(self) -> int:
        return len(self.data)

    @property
    def xl_name(self) -> str:
        return BIFF_ENUM_REVERSED.get(self.rec_id, "Undefined")

    def __repr__(self) -> str:
        ix = self.rec_id
        if ix & 0b1000_0000:
            ix = (ix & 0b0111_1111) | ((ix & 0b0111_1111_0000_0000) >> 1)
        return f'{ix:04} ({self.rec_id:04x}) <{self.rec_sz:-^4}> `{self.xl_name}`{chr(10)}{chr(9)}[{self.data.hex(" ", 1) if self.data else ""}]'  # type: ignore

    def as_binary(self) -> bytes:
        sz = len(self.data)
        return b"".join(
            [
                u2_p(self.rec_id) if self.rec_id & 0x80 else u1_p(self.rec_id),
                b1_st_p(sz)
                if sz < 0x80
                else b2_st_p(sz & 0x7F | 0x80, sz >> 7)
                if sz < 0x4000
                else b3_st_p(sz & 0x7F | 0x80, (sz >> 7) & 0x7F | 0x80, sz >> 14)
                if sz < 0x200000
                else b4_st_p(sz & 0x7F | 0x80, (sz >> 7) & 0x7F | 0x80, (sz >> 14) & 0x7F | 0x80, sz >> 21),
                self.data,
            ]
        )

    def dump(self, io: "IO[bytes]") -> int:
        io.write(self.as_binary())

        return io.tell()

    @staticmethod
    def loadr(rc: "record") -> "record | None":
        return rc

    @staticmethod
    def loadr_exact(rc: "record") -> "record":
        return rc

    def dumpr(self) -> "record":
        return self  # type: ignore

    @staticmethod
    def from_data(id: "int | str", *data: "bytes | sample_proto") -> "record":
        # sourcery skip: avoid-builtin-shadow
        if isinstance(id, str):
            id = BIFF_ENUM[id]
        if not data:
            return record(id)
        with BytesIO() as io:
            for item in data:
                if isinstance(item, bytes):
                    io.write(item)
                else:
                    item.dump(io)
            return record(id, io.getvalue())

    @staticmethod
    def peek(io: "IO[bytes]") -> "record | None":
        # sourcery skip: avoid-builtin-shadow
        with safe_read(io):
            # read record id
            id = io.read(1)[0]
            if id & 0x80:
                id |= io.read(1)[0] << 8

            # read record size
            sz = io.read(1)[0]
            if sz & 0x80:
                sz = sz ^ 0x80 | (io.read(1)[0] << 7)
                if sz & 0x4000:
                    sz = sz ^ 0x4000 | (io.read(1)[0] << 14)
                    if sz & 0x200000:
                        sz = sz ^ 0x200000 | (io.read(1)[0] << 21)

            # read record data if exists and id in x_only set, skip data otherwise
            if sz:
                data = io.read(sz)
                return None if len(data) != sz else record(id, data)
            return record(id)

    if typing.TYPE_CHECKING:

        @overload
        @staticmethod
        def scan(
            io: "IO[bytes]",
            only: "int | str | None" = None,
            *more_only: "int | str",
            break_on: "int | str | Iterable[str] | Iterable[int] | None" = None,
            max_scan: int = -1,
        ) -> "Iterator[record]":
            ...

        @overload
        @staticmethod
        def scan(
            io: "IO[bytes]",
            only: "int | str | None" = None,
            *more_only: "int | str",
            break_on: "int | str | Iterable[str] | Iterable[int] | None" = None,
            max_scan: int = -1,
            cv: "Type[RecordProtoGeneric]",
        ) -> "Iterator[RecordProtoGeneric]":
            ...

    @staticmethod
    def scan(
        io: "IO[bytes]",
        only: "int | str | None" = None,
        *more_only: "int | str",
        break_on: "int | str | Iterable[str] | Iterable[int] | None" = None,
        max_scan: int = -1,
        cv: "Type[RecordProtoGeneric] | None" = None,
    ) -> "Iterator[RecordProtoGeneric]":  # sourcery skip: avoid-builtin-shadow, low-code-quality
        x_only: "set[int]" = (
            {only if isinstance(only, int) else BIFF_ENUM[only]}.union(
                x if isinstance(x, int) else BIFF_ENUM[x] for x in more_only
            )
            if only is not None
            else set()
        )

        if break_on is None:
            break_on = {}
        elif isinstance(break_on, str):
            break_on = {BIFF_ENUM[break_on]}
        elif isinstance(break_on, int):
            break_on = {break_on}
        else:
            break_on = {x if isinstance(x, int) else BIFF_ENUM[x] for x in break_on}

        pos: int = 0
        cnt: int = 0
        try:
            while max_scan < 0 or cnt < max_scan:
                pos = io.tell()
                # read record id
                id = io.read(1)[0]
                if id & 0x80:
                    id |= io.read(1)[0] << 8

                if id in break_on:
                    io.seek(pos)
                    break

                # read record size
                sz = io.read(1)[0]
                if sz & 0x80:
                    sz = sz ^ 0x80 | (io.read(1)[0] << 7)
                    if sz & 0x4000:
                        sz = sz ^ 0x4000 | (io.read(1)[0] << 14)
                        if sz & 0x200000:
                            sz = sz ^ 0x200000 | (io.read(1)[0] << 21)

                # read record data if exists and id in x_only set, skip data otherwise
                if not x_only or id in x_only:
                    if sz:
                        data = io.read(sz)
                        if len(data) != sz:
                            break

                        ret = record(id, data)
                    else:
                        ret = record(id)

                    if cv is not None:
                        yield cv.loadr_exact(ret)  # type: ignore
                    else:
                        yield ret  # type: ignore
                    cnt += 1
                elif sz:
                    io.seek(sz, os.SEEK_CUR)
        except (IndexError, st_err):  # , GeneratorExit, StopIteration
            io.seek(pos)


if typing.TYPE_CHECKING:
    _: RecordProto[record] = None
