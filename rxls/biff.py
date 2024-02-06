# ruff: noqa:D101, D102, D105, ANN201, N801, N813, PLR2004
"""BIFF12 PARSER"""
from __future__ import annotations

__all__ = ["dump_sz", "record", "scan_biff"]

import os
from io import BytesIO
from struct import Struct as st
from typing import TYPE_CHECKING

from .biff_enum import BIFF_ENUM, BIFF_ENUM_REVERSED
from .core import NUMBA_AVAILABLE, as_dataclass, u1_p, u2_p

if TYPE_CHECKING:
    from typing import IO, Iterator

u2_st_p = st("<H").pack
u1_st_p = st("<B").pack
u1_st_u = st("<B").unpack

b1_st_p = st("<B").pack
b1_st_u = st("<B").unpack
b2_st_p = st("<BB").pack
b3_st_p = st("<BBB").pack
b4_st_p = st("<BBBB").pack


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
        else b4_st_p(
            i & 0x7F | 0x80,
            (i >> 7) & 0x7F | 0x80,
            (i >> 14) & 0x7F | 0x80,
            i >> 21,
        )
    )


@as_dataclass(fast_new=True)
class record:
    rec_id: int
    data: bytes

    @property
    def is_empty(self) -> bool:
        return not self.data

    @property
    def rec_sz(self):
        return len(self.data)

    @property
    def rec_name(self):
        return BIFF_ENUM_REVERSED.get(self.rec_id, "Undefined")

    def __repr__(self) -> str:
        ix = self.rec_id
        if ix & 0b1000_0000:
            ix = (ix & 0b0111_1111) | ((ix & 0b0111_1111_0000_0000) >> 1)
        return f'{ix:04} ({self.rec_id:04x}) <{self.rec_sz:-^4}> `{self.rec_name}`{chr(10)}{chr(9)}[{self.data.hex(" ", 1) if self.data else ""}]'  # type: ignore

    @staticmethod
    def from_data(id: int | str, *data: bytes) -> record:
        if isinstance(id, str):
            id = BIFF_ENUM[id]
        if not data:
            return record(id, b"")
        with BytesIO() as io:
            for item in data:
                io.write(item)

            return record(id, io.getvalue())

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
                else b4_st_p(
                    sz & 0x7F | 0x80,
                    (sz >> 7) & 0x7F | 0x80,
                    (sz >> 14) & 0x7F | 0x80,
                    sz >> 21,
                ),
                self.data,
            ],
        )

    def dump(self, io: IO[bytes]) -> int:
        io.write(self.as_binary())

        return io.tell()


if NUMBA_AVAILABLE:  # noqa: PLR1702
    import numba as nb
    import numpy as np

    def scan_chunk(
        chunk: bytes,
        only: set[int] | None = None,
    ) -> Iterator[tuple[int, int, bytes]]:
        offset = 0
        preoff = 0
        ch_len = len(chunk)
        id = np.int16(-1)
        try:
            while True:
                id = np.int16(chunk[offset])
                offset += 1
                if id & 0x80:  # type: ignore
                    id |= np.int16(chunk[offset]) << 8  # type: ignore
                    offset += 1

                sz = chunk[offset]
                offset += 1
                if sz & 0x80:
                    sz = (sz & 0x7F) | (chunk[offset] << 7)
                    offset += 1
                    if sz & 0x4000:
                        sz = (sz & 0x3FFF) | (chunk[offset] << 14)
                        offset += 1
                        if sz & 0x200_000:
                            sz = (sz & 0x1FF_FFF) | (chunk[offset] << 21)
                            offset += 1
                if offset + sz > ch_len:
                    break

                if only is None:  # noqa: SIM114
                    yield (id, sz, chunk[offset : offset + sz])  # type: ignore
                elif id in only:
                    yield (id, sz, chunk[offset : offset + sz])  # type: ignore
                offset += sz
                preoff = offset
        except Exception:  # noqa: S110, BLE001
            pass
        yield (-1, preoff, b"")
        raise StopIteration

    scan_chunk = nb.njit(  # type: ignore
        [
            nb.types.Generator(
                gen_func=scan_chunk,
                arg_types=(nb.types.Bytes(nb.u1, 1, "C", True), nb.types.Omitted(None)),
                yield_type=nb.types.Tuple(
                    (nb.int16, nb.uint64, nb.types.Bytes(nb.u1, 1, "C", True)),
                ),
                has_finalizer=True,
                state_types=(
                    nb.types.Tuple(
                        (nb.int16, nb.uint64, nb.types.Bytes(nb.u1, 1, "C", True)),
                    ),
                    nb.types.Bytes(nb.u1, 1, "C", True),
                    nb.u8,
                    nb.typeof(None),
                ),
            )(nb.types.Bytes(nb.u1, 1, "C", True), nb.types.Omitted(None)),
            nb.types.Generator(
                gen_func=scan_chunk,
                arg_types=(
                    nb.types.Bytes(nb.u1, 1, "C", True),
                    nb.types.Optional(nb.types.Set(nb.i8, True)),
                ),
                yield_type=nb.types.Tuple(
                    (nb.int16, nb.uint64, nb.types.Bytes(nb.u1, 1, "C", True)),
                ),
                has_finalizer=True,
                state_types=(
                    nb.types.Tuple(
                        (nb.int16, nb.uint64, nb.types.Bytes(nb.u1, 1, "C", True)),
                    ),
                    nb.types.Bytes(nb.u1, 1, "C", True),
                    nb.u8,
                    nb.typeof(None),
                ),
            )(
                nb.types.Bytes(nb.u1, 1, "C", True),
                nb.types.Optional(nb.types.Set(nb.i8, True)),
            ),
        ],
        fastmath={"fast"},
        cache=True,
    )(scan_chunk)

    def scan_biff(io: IO[bytes], only: set[int] | None = None) -> Iterator[record]:
        chunk = b""
        if only is not None and not isinstance(only, set):
            only = set(only)

        while chunk := chunk + io.read(2**16):
            for _i, _o, _data in scan_chunk(chunk, only):
                if _i < 0:
                    chunk = chunk[_o:]
                else:
                    yield record(_i, _data)

else:

    def scan_biff(io: IO[bytes], only: set[int] | None = None) -> Iterator[record]:  # noqa: C901
        try:
            while True:
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
                if not only or id in only:
                    if sz:
                        data = io.read(sz)
                        if len(data) != sz:
                            break

                        yield record(id, data)
                    else:
                        yield record(id, b"")

                elif sz:
                    io.seek(sz, os.SEEK_CUR)
        except IndexError:  # , GeneratorExit, StopIteration
            pass
