# ruff: noqa:S101, PLR2004
from io import BytesIO

from ..rxls.biff import dump_sz, record, scan_biff


def test_from_data() -> None:
    r = record.from_data(0x381, b"some data", b"some_other_data")
    assert r.rec_id == 0x381
    assert r.data == b"some datasome_other_data"


def test_from_io() -> None:
    with BytesIO(b"\x81\x03\x09some data") as io:
        r = next(scan_biff(io))
    assert r.rec_id == 0x381
    assert r.data == b"some data"


def test_dump() -> None:
    r = record.from_data(0x381, b"some data", b"some_other_data")

    with BytesIO() as io:
        i = r.dump(io)
        b = io.getvalue()
    assert i == 27
    assert b == b"\x81\x03\x18some datasome_other_data"


def test_large_dump() -> None:
    r = record.from_data(0x00, b"some data", b"some_other_data" * 100)
    s = dump_sz(r.rec_sz)

    with BytesIO() as io:
        i = r.dump(io)
        b = io.getvalue()

    assert b == b"\x00%s%s" % (s, r.data)
    assert i == len(r.data) + len(s) + 1


def test_dump_and_read() -> None:
    r = record.from_data(0x381, b"some data", b"some_other_data")

    with BytesIO() as io:
        r.dump(io)
        io.seek(0)

        r_new = next(scan_biff(io))

    assert r == r_new
    assert r.data == r_new.data
    assert r.rec_id == r_new.rec_id


def test_large_dump_and_read() -> None:
    r = record.from_data(0x381, b"some data", b"some_other_data" * 100)

    with BytesIO() as io:
        r.dump(io)
        io.seek(0)

        r_new = next(scan_biff(io))

    assert r == r_new
    assert r.data == r_new.data
    assert r.rec_id == r_new.rec_id
