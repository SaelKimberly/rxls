"""
Common constants for Excel parsing. TYPE and REPR describes how column chunk must be evaluated.

TYPE_NULL and REPR_PLAIN - Must NOT be used in any part of program. There is no reason for using it.

TYPE_STRING with REPR_PLAIN -> pyarrow.LargeStringArray
TYPE_STRING with REPR_SHAREDST -> pyarrow.LargeStringArray (taken from shared strings by index)
TYPE_STRING with REPR_COMPILED -> ready to use pyarrow.Array

TYPE_NUMERIC with REPR_TEMPORAL -> pyarrow.TimestampArray (from float64 to unix timestamp)
TYPE_NUMERIC with REPR_RKNUMBER -> pyarrow.TimestampArray (rknumber -> float64)
TYPE_NUMERIC with REPR_TEMPORAL and REPR_RKNUMBER -> pyarrow.TimestampArray (rknumber -> float64 -> unix timestamp)
TYPE_NUMERIC with REPR_COMPILED -> ready to use pyarrow.Array
"""


__all__ = [
    "REPR_PREPARED",
    "REPR_RKNUMBER",
    "REPR_SHARED",
    "REPR_TEMPORAL",
    "TYPE_NULL",
    "TYPE_NUMERIC",
    "TYPE_STRING",
]

TYPE_NULL = 0x00
TYPE_STRING = 0x10
TYPE_NUMERIC = 0x20

REPR_PLAIN = 0x00
REPR_SHARED = REPR_RKNUMBER = 0x01
REPR_BOOLEAN = 0x02
REPR_TEMPORAL = 0x04
REPR_PREPARED = 0x08
