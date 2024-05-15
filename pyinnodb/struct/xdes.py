from .list import MListNode
from ..mconstruct import *

import typing as t


class MXdesEntry(CC):
    fseg_id: int = cfield(cs.Int64ub)
    xdes_list: MListNode = cfield(MListNode)
    state: int = cfield(cs.Int32ub)
    page_state: t.List[int] = cfield(cs.Bitwise(carray(64, cs.BitsInteger(2))))
