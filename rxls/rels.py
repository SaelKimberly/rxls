from typing import IO, cast

from lxml.etree import parse, _ElementTree as tree

from .core import as_dataclass

RELS_NS = "http://schemas.openxmlformats.org/package/2006/relationships"

__all__ = ["Relationship"]


@as_dataclass
class Relationship:
    Id: str
    Type: str
    Target: str

    @staticmethod
    def scan_xml(io: IO[bytes]) -> "list[Relationship]":
        return [
            Relationship(item.attrib["Id"], item.attrib["Type"].rsplit("/", 1)[-1], item.attrib["Target"])
            for item in cast(tree, parse(io, None)).iter(f"{{{RELS_NS}}}Relationship")
        ]

    def dump_xml(self, io: IO[bytes]):
        io.write(f'<Relationship Id="{self.Id}" Type="{RELS_NS}/{self.Type}" Target="{self.Target}"/>'.encode())
