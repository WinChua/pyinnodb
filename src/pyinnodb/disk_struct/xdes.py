from .list import MListNode
from ..mconstruct import *
from .. import const

import typing as t


def swapbytesevery(every=2, unit=8):
    def swap(data):
        if len(data) % unit:
            raise ValueError(
                f"data length {len(data)} must be a multiple of unit({unit})")
        if unit % every:
            raise ValueError(
                f"unit({unit}) must be a multiple of every({every})")
        result = b""
        for seg in zip(*[data[i::unit] for i in range(unit)]):
            result += b"".join(
                list(map(bytes, zip(*[seg[i::every] for i in range(every)])))[::-1])
        return result
    return swap


def ByteSwapEvery(subcon, every=2, unit=8):
    size = subcon.sizeof()
    swap = swapbytesevery(every, unit)
    return cs.Transformed(subcon, swap, size, swap, size)


class MXdesEntry(CC):
    fseg_id: int = cfield(cs.Int64ub)
    xdes_list: MListNode = cfield(MListNode)
    state: int = cfield(cs.Int32ub)
    # page_state: t.List[int] = cfield(cs.Bitwise(
    #     (carray(64, cs.BitsInteger(2)))))
    page_state: t.List[int] = cfield(cs.Bitwise(
        ((ByteSwapEvery(carray(64, cs.BitsInteger(2)))))))
    # innodb use big-endaniss when writing data, and every 4 page's state is
    # in a write unit, which means that we should reverse every two bits in
    # one byte, here is an example:
    # page_0(00), page_1(01), page_2(10), page_3(11) ==> 11|10|01|00, when write by innodb
    # so, when parsing:
    # 11|10|01|00 => b"\x01\x01\x01\x00\x00\x01\x00\x00" =>
    # b"\x00\x00\x00\x01\x01\x00\x01\x01"

    def desc_page_usage(self, idx=None):
        used_idx = []
        for i, ps in enumerate(self.page_state):
            if not const.PageState.is_page_free(ps):
                used_idx.append(i+(0 if idx is None else 64 * idx))
        return used_idx
