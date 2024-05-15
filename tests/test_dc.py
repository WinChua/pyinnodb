import dataclasses
import construct as cs

import logging

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class TDC():
    name: str = dataclasses.field(default="Hello,World")


def test_dc():
    t = TDC(name="OG")
    logger.info("name is %s", t.name)
    logger.info("Hello,World")


def test_cs():
    parser = cs.BitStruct(
        "hello" / cs.BitsInteger(10),
        "world" / cs.BitsInteger(6)
    )
    orig_parser = cs.Struct(
        "B" / cs.BitStruct(
            "Ok" / cs.BitsInteger(4),
            "Not" / cs.BitsInteger(4),
        ),
        "A" / cs.Int64ub,
    )
    result = parser.parse(b"\x01\x02")
    logger.info("parse result is %s", result)
    logger.info("sizeof orig_parser %d", orig_parser.sizeof())
    logger.info('o parse is %s', orig_parser.parse(
        b"\x01\x00\x00\x00\x00\x00\x00\x00\x01"))
