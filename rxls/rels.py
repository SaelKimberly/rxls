# ruff: noqa:D101, D102
from __future__ import annotations

__all__ = ["Relationship"]

from typing import IO

from pyexpat import ParserCreate

from .core import as_dataclass

RELS_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


@as_dataclass
class Relationship:
    Id: str
    Type: str
    Target: str

    @staticmethod
    def scan_xml(io: IO[bytes]) -> list[Relationship]:
        ret = []

        def rs_handler(tag: str, attr: dict[str, str]) -> None:
            nonlocal ret

            if tag == "Relationship":  # and attr["Type"].endswith("/worksheet"):
                ret.append(
                    Relationship(
                        attr["Id"],
                        attr["Type"].rsplit("/", 1)[-1],
                        attr["Target"],
                    ),
                )

        parser = ParserCreate()
        parser.StartElementHandler = rs_handler
        parser.ParseFile(io)

        return ret

    def dump_xml(self, io: IO[bytes]) -> None:
        io.write(
            f'<Relationship Id="{self.Id}" Type="{RELS_NS}/{self.Type}" Target="{self.Target}"/>'.encode(),
        )
