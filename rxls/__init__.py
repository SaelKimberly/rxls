import typing

from .core import as_dataclass, cached
from .record_enum import BIFF_ENUM, BIFF_ENUM_REVERSED
from .record import record

if typing.TYPE_CHECKING:
    from .record import RecordProto, SampleProto

from .rels import Relationship
from .reader.reader import xl_scan

if typing.TYPE_CHECKING:
    __all__ = [
        "as_dataclass",
        "cached",
        "BIFF_ENUM",
        "BIFF_ENUM_REVERSED",
        "RecordProto",
        "SampleProto",
        "record",
        "Relationship",
        "xl_scan",
    ]

else:
    __all__ = [
        "as_dataclass",
        "cached",
        "BIFF_ENUM",
        "BIFF_ENUM_REVERSED",
        "record",
        "Relationship",
        "xl_scan",
    ]
