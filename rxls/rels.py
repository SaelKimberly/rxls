import typing

from pyexpat import ParserCreate

from .core import as_dataclass

if typing.TYPE_CHECKING:
    from typing import IO

__all__ = ["Relationship"]

RELS_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


@as_dataclass
class Relationship:
    Id: str
    Type: str
    Target: str

    @staticmethod
    def scan_xml(io: IO[bytes]) -> "list[Relationship]":
        rels = []

        def rs_handler(tag: str, attr: dict[str, str]):
            nonlocal rels

            if tag == "Relationship":
                rels.append(Relationship(attr["Id"], attr["Type"].rsplit("/", 1)[-1], attr["Target"]))

        parser = ParserCreate()
        parser.StartElementHandler = rs_handler
        parser.ParseFile(io)

        return rels

    def dump_xml(self, io: IO[bytes]):
        io.write(f'<Relationship Id="{self.Id}" Type="{RELS_NS}/{self.Type}" Target="{self.Target}"/>'.encode())
