from __future__ import annotations

__all__ = ["cell"]


from .core import as_dataclass


@as_dataclass(readonly=True)
class cell:  # noqa: N801
    """Excel cell representation. Very lightweight with recordclass"""

    row: int
    "0-based row index"

    col: int
    "0-based column index"

    xdt: int
    """
    ### DATA TYPE AND REPRESENTATION
    """

    val: bytes
    """
    ### CELL DATA
    """
